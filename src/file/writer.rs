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

use std::io::Write;
use std::rc::Rc;

use basic::PageType;
use column::page::{CompressedPage, PageWriter};
use column::writer::{ColumnWriter, get_column_writer};
use errors::{ParquetError, Result};
use file::metadata::*;
use file::properties::WriterPropertiesPtr;
use parquet_format as parquet;
use schema::types::SchemaDescPtr;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use util::io::{Position, TOutputStream};

/// Serialized file writer.
///
/// The main entrypoint of writing a Parquet file.
pub struct SerializedFileWriter {
}

impl SerializedFileWriter {
}

/// Serialized row group writer.
///
/// Coordinates writing of a row group with column writers.
pub struct SerializedRowGroupWriter {
  descr: SchemaDescPtr,
  props: WriterPropertiesPtr,
  total_rows_written: Option<usize>,
  total_bytes_written: usize,
  column_index: usize,
  current_column_writer: Option<ColumnWriter>,
  row_group_metadata: Option<RowGroupMetaDataPtr>,
  column_chunks: Vec<ColumnChunkMetaDataPtr>
}

impl SerializedRowGroupWriter {
  pub fn new(schema_descr: SchemaDescPtr, properties: WriterPropertiesPtr) -> Self {
    let num_columns = schema_descr.num_columns();
    Self {
      descr: schema_descr,
      props: properties,
      total_rows_written: None,
      total_bytes_written: 0,
      column_index: 0,
      current_column_writer: None,
      row_group_metadata: None,
      column_chunks: Vec::with_capacity(num_columns)
    }
  }

  /// Returns number of columns in the schema.
  #[inline]
  pub fn num_columns(&self) -> usize {
    self.descr.num_columns()
  }

  /// Returns current column index.
  #[inline]
  pub fn column_index(&self) -> usize {
    self.column_index
  }

  /// Returns true if there are more column writer available, false otherwise.
  #[inline]
  pub fn has_next_column(&self) -> bool {
    self.column_index < self.num_columns() && self.row_group_metadata.is_none()
  }

  /// Returns the next column writer.
  /// When no more columns are available, returns `None`.
  ///
  /// This method finalises previous column writer, so make sure that all writes are
  /// finished before calling this method.
  #[inline]
  pub fn next_column(&mut self) -> Result<&mut ColumnWriter> {
    if self.row_group_metadata.is_some() {
      return Err(general_err!("Row group writer is closed"));
    }

    self.finalise_column_writer()?;
    if self.column_index < self.num_columns() {
      let page_writer = unimplemented!();
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

  /// Closes this row group writer and updates row group metadata.
  #[inline]
  pub fn close(&mut self) -> Result<()> {
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
  pub fn get_row_group_metadata(&self) -> RowGroupMetaDataPtr {
    assert!(
      self.row_group_metadata.is_some(),
      "Row group metadata is not available, row group writer is not closed"
    );

    self.row_group_metadata.clone().unwrap()
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

/// Serialized page writer.
///
/// Writes and serializes data pages into output stream,
/// and provides proxy for comporession.
pub struct SerializedPageWriter<T: Write + Position> {
  sink: T,
  dictionary_page_offset: Option<usize>,
  data_page_offset: Option<usize>,
  total_uncompressed_size: usize,
  total_compressed_size: usize,
  num_values: usize
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

    self.total_uncompressed_size += uncompressed_size + header_size;
    self.total_compressed_size += compressed_size + header_size;
    self.num_values += num_values as usize;

    let bytes_written = self.sink.pos() - start_pos;
    Ok(bytes_written)
  }

  fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> Result<()> {
    let column_metadata = parquet::ColumnMetaData {
      type_: metadata.column_type().into(),
      encodings: metadata.encodings().iter().map(|&v| v.into()).collect(),
      path_in_schema: Vec::from(metadata.column_path().as_slice()),
      codec: metadata.compression().into(),
      num_values: metadata.num_values(),
      total_uncompressed_size: metadata.uncompressed_size(),
      total_compressed_size: metadata.compressed_size(),
      key_value_metadata: None,
      data_page_offset: metadata.data_page_offset(),
      index_page_offset: None,
      dictionary_page_offset: metadata.dictionary_page_offset(),
      statistics: None,
      encoding_stats: None
    };

    let column_chunk = parquet::ColumnChunk {
      file_path: metadata.file_path().map(|v| v.clone()),
      file_offset: metadata.file_offset(),
      meta_data: Some(column_metadata),
      offset_index_offset: None,
      offset_index_length: None,
      column_index_offset: None,
      column_index_length: None
    };

    self.serialize_column_chunk(column_chunk)
  }

  #[inline]
  fn dictionary_page_offset(&self) -> Option<usize> {
    self.dictionary_page_offset
  }

  #[inline]
  fn data_page_offset(&self) -> usize {
    self.data_page_offset.unwrap_or(0)
  }

  #[inline]
  fn total_uncompressed_size(&self) -> usize {
    self.total_uncompressed_size
  }

  #[inline]
  fn total_compressed_size(&self) -> usize {
    self.total_compressed_size
  }

  #[inline]
  fn num_values(&self) -> usize {
    self.num_values
  }
}
