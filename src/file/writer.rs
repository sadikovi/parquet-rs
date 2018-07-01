// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains file writer API.

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::rc::Rc;

use basic::PageType;
use byteorder::{LittleEndian, ByteOrder};
use column::page::{CompressedPage, PageWriter};
use column::writer::{ColumnWriter, get_column_writer};
use errors::{ParquetError, Result};
use file::{FOOTER_SIZE, PARQUET_MAGIC};
use file::metadata::*;
use file::properties::WriterPropertiesPtr;
use parquet_format as parquet;
use schema::types::{self, SchemaDescriptor, SchemaDescPtr, TypePtr};
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use util::io::{FileSink, Position, TOutputStream};

/// File writer interface.
pub trait FileWriter {
  /// Creates new row group from this file writer.
  fn create_row_group(&mut self) -> Box<RowGroupWriter>;

  /// Adds new row group to the file.
  /// Must be the one that was requested using `new_row_group()` method.
  fn append_row_group(&mut self, row_group_writer: Box<RowGroupWriter>) -> Result<()>;

  /// Closes and finalises file writer.
  ///
  /// All row groups must be appended before this method is called.
  /// No writes are allowed after this point.
  fn close(&mut self) -> Result<()>;
}

/// Row group writer interface.
pub trait RowGroupWriter {
  /// Returns number of columns in the schema.
  fn num_columns(&self) -> usize;

  /// Returns current column index.
  fn column_index(&self) -> usize;

  /// Returns true if there are more column writer available, false otherwise.
  fn has_next_column(&self) -> bool;

  /// Returns the next column writer.
  /// When no more columns are available, returns `None`.
  ///
  /// This method finalises previous column writer, so make sure that all writes are
  /// finished before calling this method.
  fn next_column(&mut self) -> Result<&mut ColumnWriter>;

  /// Closes this row group writer and updates row group metadata.
  fn close(&mut self) -> Result<()>;

  /// Returns row group metadata.
  /// Method is only available after `close()` is called.
  fn get_row_group_metadata(&self) -> RowGroupMetaDataPtr;
}

// ----------------------------------------------------------------------
// Serialized impl for file & row group writers

/// Serialized file writer.
///
/// The main entrypoint of writing a Parquet file.
pub struct SerializedFileWriter {
  file: File,
  schema: TypePtr,
  descr: SchemaDescPtr,
  props: WriterPropertiesPtr,
  ref_count: i32,
  total_num_rows: u64,
  row_groups: Vec<RowGroupMetaDataPtr>
}

impl SerializedFileWriter {
  /// Creates new file writer.
  pub fn new(mut file: File, schema: TypePtr, properties: WriterPropertiesPtr) -> Self {
    Self::start_file(&mut file).expect("Start Parquet file");
    Self {
      file: file,
      schema: schema.clone(),
      descr: Rc::new(SchemaDescriptor::new(schema)),
      props: properties,
      ref_count: 0,
      total_num_rows: 0,
      row_groups: Vec::new()
    }
  }

  /// Writes magic bytes at the beginning of the file.
  fn start_file(file: &mut File) -> Result<()> {
    file.write(&PARQUET_MAGIC)?;
    Ok(())
  }

  /// Assemble and writes metadata at the end of the file.
  fn write_metadata(&mut self) -> Result<()> {
    let file_metadata = parquet::FileMetaData {
      // Version of this file
      // TODO: check what this field means! looks like Spark writes "1" even for file
      // with data page v2 file.
      version: self.props.writer_version().as_num(),
      schema: types::to_thrift(self.schema.as_ref()),
      num_rows: self.total_num_rows as i64,
      row_groups: self.row_groups.as_slice().into_iter().map(|v| v.to_thrift()).collect(),
      key_value_metadata: None,
      created_by: Some(self.props.created_by().to_owned()),
      column_orders: None
    };

    // Write file metadata
    let start_pos = self.file.seek(SeekFrom::Current(0))?;
    {
      let transport = TOutputStream::new(&mut self.file);
      let mut protocol = TCompactOutputProtocol::new(transport);
      file_metadata.write_to_out_protocol(&mut protocol)?;
      protocol.flush()?;
    }
    let end_pos = self.file.seek(SeekFrom::Current(0))?;

    // Write footer
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    let metadata_len = (end_pos - start_pos) as i32;
    LittleEndian::write_i32(&mut footer_buffer, metadata_len);
    (&mut footer_buffer[4..]).write(&PARQUET_MAGIC)?;
    self.file.write(&footer_buffer)?;
    Ok(())
  }
}

impl FileWriter for SerializedFileWriter {
  fn create_row_group(&mut self) -> Box<RowGroupWriter> {
    let writer = SerializedRowGroupWriter::new(
      self.descr.clone(),
      self.props.clone(),
      &self.file
    );
    self.ref_count += 1;
    Box::new(writer)
  }

  fn append_row_group(
    &mut self,
    mut row_group_writer: Box<RowGroupWriter>
  ) -> Result<()> {
    self.ref_count -= 1;
    row_group_writer.close()?;
    self.row_groups.push(row_group_writer.get_row_group_metadata());
    Ok(())
  }

  fn close(&mut self) -> Result<()> {
    assert_eq!(self.ref_count, 0, "Row group count mismatch ({})", self.ref_count);
    self.write_metadata()
  }
}

/// Serialized row group writer.
///
/// Coordinates writing of a row group with column writers.
pub struct SerializedRowGroupWriter {
  descr: SchemaDescPtr,
  props: WriterPropertiesPtr,
  file: File,
  total_rows_written: Option<usize>,
  total_bytes_written: usize,
  column_index: usize,
  current_column_writer: Option<ColumnWriter>,
  row_group_metadata: Option<RowGroupMetaDataPtr>,
  column_chunks: Vec<ColumnChunkMetaDataPtr>
}

impl SerializedRowGroupWriter {
  pub fn new(
    schema_descr: SchemaDescPtr,
    properties: WriterPropertiesPtr,
    file: &File
  ) -> Self {
    let num_columns = schema_descr.num_columns();
    Self {
      descr: schema_descr,
      props: properties,
      file: file.try_clone().unwrap(),
      total_rows_written: None,
      total_bytes_written: 0,
      column_index: 0,
      current_column_writer: None,
      row_group_metadata: None,
      column_chunks: Vec::with_capacity(num_columns)
    }
  }

  /// Checks and finalises current column writer.
  fn finalise_column_writer(&mut self) -> Result<()> {
    if let Some(ref mut writer) = self.current_column_writer {
      let rows_written;
      let bytes_written;
      let metadata;

      match writer {
        ColumnWriter::BoolColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::Int32ColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::Int64ColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::Int96ColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::FloatColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::DoubleColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::ByteArrayColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        },
        ColumnWriter::FixedLenByteArrayColumnWriter(typed) => {
          typed.close()?;
          rows_written = typed.get_total_rows_written();
          bytes_written = typed.get_total_bytes_written();
          metadata = typed.get_column_metadata();
        }
      }

      // Update row group writer metrics
      self.total_bytes_written += bytes_written;
      self.column_chunks.push(metadata);
      if let Some(rows) = self.total_rows_written {
        if rows != rows_written {
          return Err(general_err!(
            "Incorrect number of rows, expected {} != {} rows",
            rows,
            rows_written
          ));
        }
      } else {
        self.total_rows_written = Some(rows_written);
      }
    }
    Ok(())
  }
}

impl RowGroupWriter for SerializedRowGroupWriter {
  #[inline]
  fn num_columns(&self) -> usize {
    self.descr.num_columns()
  }

  #[inline]
  fn column_index(&self) -> usize {
    self.column_index
  }

  #[inline]
  fn has_next_column(&self) -> bool {
    self.column_index < self.num_columns() && self.row_group_metadata.is_none()
  }

  #[inline]
  fn next_column(&mut self) -> Result<&mut ColumnWriter> {
    if self.row_group_metadata.is_some() {
      return Err(general_err!("Row group writer is closed"));
    }

    self.finalise_column_writer()?;
    if self.column_index < self.num_columns() {
      let sink = FileSink::new(&self.file);
      let page_writer = Box::new(SerializedPageWriter::new(sink));
      let column_writer = get_column_writer(
        self.descr.column(self.column_index),
        self.props.clone(),
        page_writer
      );

      self.column_index += 1;

      self.current_column_writer = Some(column_writer);
      Ok(self.current_column_writer.as_mut().unwrap())
    } else {
      Err(general_err!("No more column writers left"))
    }
  }

  #[inline]
  fn close(&mut self) -> Result<()> {
    if self.row_group_metadata.is_none() {
      self.finalise_column_writer()?;
      self.current_column_writer = None;

      let row_group_metadata =
        RowGroupMetaDataBuilder::new(self.descr.clone())
          .set_column_metadata(self.column_chunks.clone())
          .set_total_byte_size(self.total_bytes_written as i64)
          .set_num_rows(self.total_rows_written.unwrap_or(0) as i64)
          .build();

      self.row_group_metadata = Some(Rc::new(row_group_metadata));
    }

    Ok(())
  }

  #[inline]
  fn get_row_group_metadata(&self) -> RowGroupMetaDataPtr {
    assert!(
      self.row_group_metadata.is_some(),
      "Row group metadata is not available, row group writer is not closed"
    );

    self.row_group_metadata.clone().unwrap()
  }
}

/// Serialized page writer.
///
/// Writes and serializes data pages into output stream,
/// and provides proxy for comporession.
pub struct SerializedPageWriter<T: Write + Position> {
  sink: T,
  dictionary_page_offset: Option<u64>,
  data_page_offset: Option<u64>,
  total_uncompressed_size: u64,
  total_compressed_size: u64,
  num_values: u32
}

impl<T: Write + Position> SerializedPageWriter<T> {
  /// Creates new page writer.
  pub fn new(sink: T) -> Self {
    Self {
      sink: sink,
      dictionary_page_offset: None,
      data_page_offset: None,
      total_uncompressed_size: 0,
      total_compressed_size: 0,
      num_values: 0
    }
  }

  /// Serializes page header into Thrift.
  fn serialize_page_header(&mut self, header: parquet::PageHeader) -> Result<usize> {
    let start_pos = self.sink.pos();
    {
      let transport = TOutputStream::new(&mut self.sink);
      let mut protocol = TCompactOutputProtocol::new(transport);
      header.write_to_out_protocol(&mut protocol)?;
      protocol.flush()?;
    }
    Ok((self.sink.pos() - start_pos) as usize)
  }

  /// Serializes column chunk into Thrift.
  fn serialize_column_chunk(&mut self, chunk: parquet::ColumnChunk) -> Result<()> {
    let transport = TOutputStream::new(&mut self.sink);
    let mut protocol = TCompactOutputProtocol::new(transport);
    chunk.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;
    Ok(())
  }
}

impl<T: Write + Position> PageWriter for SerializedPageWriter<T> {
  fn write_page(&mut self, page: CompressedPage) -> Result<usize> {
    let uncompressed_size = page.uncompressed_size();
    let compressed_size = page.compressed_size();
    let num_values = page.num_values();
    let encoding = page.encoding();
    let page_type = page.page_type();

    let mut page_header = parquet::PageHeader {
      type_: page_type.into(),
      uncompressed_page_size: uncompressed_size as i32,
      compressed_page_size: compressed_size as i32,
      // TODO: Add support for crc checksum
      crc: None,
      data_page_header: None,
      index_page_header: None,
      dictionary_page_header: None,
      data_page_header_v2: None
    };

    match page {
      CompressedPage::DataPage { def_level_encoding, rep_level_encoding, .. } => {
        let data_page_header = parquet::DataPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          definition_level_encoding: def_level_encoding.into(),
          repetition_level_encoding: rep_level_encoding.into(),
          // TODO: Process statistics
          statistics: None
        };

        page_header.data_page_header = Some(data_page_header);
      },
      CompressedPage::DataPageV2 {
        num_nulls,
        num_rows,
        def_levels_byte_len,
        rep_levels_byte_len,
        is_compressed,
        ..
      } => {
        let data_page_header_v2 = parquet::DataPageHeaderV2 {
          num_values: num_values as i32,
          num_nulls: num_nulls as i32,
          num_rows: num_rows as i32,
          encoding: encoding.into(),
          definition_levels_byte_length: def_levels_byte_len as i32,
          repetition_levels_byte_length: rep_levels_byte_len as i32,
          is_compressed: Some(is_compressed),
          // TODO: Process statistics
          statistics: None
        };

        page_header.data_page_header_v2 = Some(data_page_header_v2);
      },
      CompressedPage::DictionaryPage { is_sorted, .. } => {
        let dictionary_page_header = parquet::DictionaryPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          is_sorted: Some(is_sorted)
        };
        page_header.dictionary_page_header = Some(dictionary_page_header);
      }
    }

    let start_pos = self.sink.pos();

    match page_type {
      PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => {
        if self.data_page_offset.is_none() {
          self.data_page_offset = Some(start_pos);
        }
        // Number of values is incremented for data pages only
        self.num_values += num_values;
      },
      PageType::DICTIONARY_PAGE => {
        assert!(self.dictionary_page_offset.is_none(), "Dictionary page is already set");
        self.dictionary_page_offset = Some(start_pos);
      }
      _ => {
        // Do nothing
      }
    }

    let header_size = self.serialize_page_header(page_header)?;
    self.sink.write_all(page.data())?;

    self.total_uncompressed_size += (uncompressed_size + header_size) as u64;
    self.total_compressed_size += (compressed_size + header_size) as u64;

    let bytes_written = (self.sink.pos() - start_pos) as usize;
    Ok(bytes_written)
  }

  fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> Result<()> {
    self.serialize_column_chunk(metadata.to_thrift())
  }

  fn close(&mut self) -> Result<()> {
    self.sink.flush()?;
    Ok(())
  }

  #[inline]
  fn dictionary_page_offset(&self) -> Option<u64> {
    self.dictionary_page_offset
  }

  #[inline]
  fn data_page_offset(&self) -> u64 {
    self.data_page_offset.unwrap_or(0)
  }

  #[inline]
  fn total_uncompressed_size(&self) -> u64 {
    self.total_uncompressed_size
  }

  #[inline]
  fn total_compressed_size(&self) -> u64 {
    self.total_compressed_size
  }

  #[inline]
  fn num_values(&self) -> u32 {
    self.num_values
  }
}
