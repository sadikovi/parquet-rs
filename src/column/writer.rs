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

//! Contains column writer API.

use std::cmp;
use std::collections::VecDeque;
use std::mem;
use std::rc::Rc;

use basic::{Compression, Encoding, Type};
use column::page::{CompressedPage, Page, PageWriter};
use compression::{Codec, create_codec};
use data_type::*;
use encodings::encoding::{DictEncoder, Encoder, get_encoder};
use encodings::levels::LevelEncoder;
use errors::{ParquetError, Result};
use file::metadata::{ColumnChunkMetaDataBuilder, ColumnChunkMetaDataPtr};
use file::properties::{WriterPropertiesPtr, WriterVersion};
use schema::types::ColumnDescPtr;
use util::memory::{ByteBufferPtr, MemTracker};

/// Column writer for a Parquet type.
pub enum ColumnWriter {
  BoolColumnWriter(ColumnWriterImpl<BoolType>),
  Int32ColumnWriter(ColumnWriterImpl<Int32Type>),
  Int64ColumnWriter(ColumnWriterImpl<Int64Type>),
  Int96ColumnWriter(ColumnWriterImpl<Int96Type>),
  FloatColumnWriter(ColumnWriterImpl<FloatType>),
  DoubleColumnWriter(ColumnWriterImpl<DoubleType>),
  ByteArrayColumnWriter(ColumnWriterImpl<ByteArrayType>),
  FixedLenByteArrayColumnWriter(ColumnWriterImpl<FixedLenByteArrayType>)
}

/// Gets a specific column writer corresponding to column descriptor `col_descr`.
pub fn get_column_writer(
  descr: ColumnDescPtr,
  props: WriterPropertiesPtr,
  page_writer: Box<PageWriter>
) -> ColumnWriter {
  match descr.physical_type() {
    Type::BOOLEAN => ColumnWriter::BoolColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::INT32 => ColumnWriter::Int32ColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::INT64 => ColumnWriter::Int64ColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::INT96 => ColumnWriter::Int96ColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::FLOAT => ColumnWriter::FloatColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::DOUBLE => ColumnWriter::DoubleColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::BYTE_ARRAY => ColumnWriter::ByteArrayColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer)),
    Type::FIXED_LEN_BYTE_ARRAY => ColumnWriter::FixedLenByteArrayColumnWriter(
      ColumnWriterImpl::new(descr, props, page_writer))
  }
}

/// Gets a typed column writer for the specific type `T`, by "up-casting" `col_writer` of
/// non-generic type to a generic column writer type `ColumnWriterImpl`.
///
/// NOTE: the caller MUST guarantee that the actual enum value for `col_writer` matches
/// the type `T`. Otherwise, disastrous consequence could happen.
pub fn get_typed_column_writer<T: DataType>(
  col_writer: ColumnWriter
) -> ColumnWriterImpl<T> {
  match col_writer {
    ColumnWriter::BoolColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::Int32ColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::Int64ColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::Int96ColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::FloatColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::DoubleColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::ByteArrayColumnWriter(r) => unsafe { mem::transmute(r) },
    ColumnWriter::FixedLenByteArrayColumnWriter(r) => unsafe { mem::transmute(r) }
  }
}

/// Typed column writer for a primitive column.
pub struct ColumnWriterImpl<T: DataType> {
  // Column chunk metadata serves as indicator of whether or not column writer has been
  // closed. If it is set, then we have finalised the writes.
  column_chunk_metadata: Option<ColumnChunkMetaDataPtr>,
  descr: ColumnDescPtr,
  props: WriterPropertiesPtr,
  page_writer: Box<PageWriter>,
  has_dictionary: bool,
  dict_encoder: Option<DictEncoder<T>>,
  encoder: Box<Encoder<T>>,
  codec: Compression,
  compressor: Option<Box<Codec>>,
  num_buffered_values: usize,
  num_buffered_encoded_values: usize,
  rows_written: usize,
  total_bytes_written: u64,
  def_levels_sink: Vec<i16>,
  rep_levels_sink: Vec<i16>,
  data_pages: VecDeque<CompressedPage>
}

impl<T: DataType> ColumnWriterImpl<T> where T: 'static {
  pub fn new(
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    page_writer: Box<PageWriter>
  ) -> Self {
    let codec = props.compression(descr.path());
    let compressor = create_codec(codec).unwrap();

    // Optionally set dictionary encoder.
    let dict_encoder = if props.dictionary_enabled(descr.path()) {
      Some(DictEncoder::new(descr.clone(), Rc::new(MemTracker::new())))
    } else {
      None
    };

    // Whether or not this column writer has a dictionary encoding.
    let has_dictionary = dict_encoder.is_some();

    // Set either main encoder or fallback encoder.
    let fallback_encoder = get_encoder(
      descr.clone(),
      props.encoding(descr.path()),
      Rc::new(MemTracker::new())
    ).unwrap();

    Self {
      column_chunk_metadata: None,
      descr: descr,
      props: props,
      page_writer: page_writer,
      has_dictionary: has_dictionary,
      dict_encoder: dict_encoder,
      encoder: fallback_encoder,
      codec: codec,
      compressor: compressor,
      num_buffered_values: 0,
      num_buffered_encoded_values: 0,
      rows_written: 0,
      total_bytes_written: 0,
      def_levels_sink: vec![],
      rep_levels_sink: vec![],
      data_pages: VecDeque::new()
    }
  }

  /// Writes batch of values, definition levels and repetition levels.
  /// If it is non-nullable and non-repeated value, then definition and repetition levels
  /// can be omitted.
  pub fn write_batch(
    &mut self,
    values: &[T::T],
    def_levels: Option<&[i16]>,
    rep_levels: Option<&[i16]>
  ) -> Result<usize> {
    // We check for DataPage limits only after we have inserted the values. If a user
    // writes a large number of values, the DataPage size can be well above the limit.
    //
    // The purpose of this chunking is to bound this. Even if a user writes large number
    // of values, the chunking will ensure that we add data page at a reasonable pagesize
    // limit.

    // TODO: find out why we don't account for size of levels when we estimate page size.

    if self.column_chunk_metadata.is_some() {
      return Err(general_err!("Column writer has been closed"));
    }

    // Find out the minimal length to prevent index out of bound errors
    let mut min_len = values.len();
    if let Some(levels) = def_levels {
      min_len = cmp::min(min_len, levels.len());
    }
    if let Some(levels) = rep_levels {
      min_len = cmp::min(min_len, levels.len());
    }

    // Find out number of batches to process
    let write_batch_size = self.props.write_batch_size();
    let num_batches = min_len / write_batch_size;

    let mut values_offset = 0;
    let mut levels_offset = 0;

    for _ in 0..num_batches {
      values_offset += self.write_mini_batch(
        &values[values_offset..values_offset + write_batch_size],
        def_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size]),
        rep_levels.map(|lv| &lv[levels_offset..levels_offset + write_batch_size])
      )?;
      levels_offset += write_batch_size;
    }

    values_offset += self.write_mini_batch(
      &values[values_offset..],
      def_levels.map(|lv| &lv[levels_offset..]),
      rep_levels.map(|lv| &lv[levels_offset..])
    )?;

    // Return total number of values processed.
    Ok(values_offset)
  }

  /// Returns total number of bytes written by this column writer.
  pub fn get_total_bytes_written(&self) -> u64 {
    self.total_bytes_written
  }

  /// Returns reference counted column chunk metadata for this column.
  /// Metadata is only available after column writer has been closed.
  pub fn get_column_metadata(&self) -> ColumnChunkMetaDataPtr {
    assert!(
      self.column_chunk_metadata.is_some(),
      "Column chunk metadata is not available, column writer is not closed"
    );

    self.column_chunk_metadata.clone().unwrap()
  }

  /// Finalises writes and closes the column writer.
  /// Returns total number of bytes written by this column writer.
  pub fn close(&mut self) -> Result<()> {
    if self.column_chunk_metadata.is_none() {
      if self.dict_encoder.is_some() {
        self.write_dictionary_page()?;
      }
      self.flush_data_pages()?;
      self.write_column_metadata()?;
      self.dict_encoder = None;
    }

    Ok(())
  }

  /// Writes mini batch of values, definition and repetition levels.
  /// This allows fine-grained processing of values and maintaining a reasonable
  /// page size.
  fn write_mini_batch(
    &mut self,
    values: &[T::T],
    def_levels: Option<&[i16]>,
    rep_levels: Option<&[i16]>
  ) -> Result<usize> {
    let num_values;
    let mut values_to_write = 0;

    // Check if number of definition levels is the same as number of repetition levels.
    if def_levels.is_some() && rep_levels.is_some() {
      let def = def_levels.unwrap();
      let rep = rep_levels.unwrap();
      if def.len() != rep.len() {
        return Err(general_err!(
          "Inconsistent length of definition and repetition levels: {} != {}",
          def.len(),
          rep.len()
        ));
      }
    }

    // Process definition levels and determine how many values to write
    if self.descr.max_def_level() > 0 {
      if def_levels.is_none() {
        return Err(general_err!(
          "Definition levels are required, because max definition level = {}",
          self.descr.max_def_level()
        ));
      }

      let levels = def_levels.unwrap();
      num_values = levels.len();
      for &level in levels {
        if level == self.descr.max_def_level() {
          values_to_write += 1;
        }
      }

      self.write_definition_levels(levels);
    } else {
      values_to_write = values.len();
      num_values = values_to_write;
    }

    // Process repetition levels and determine how many rows we are about to process
    if self.descr.max_rep_level() > 0 {
      if rep_levels.is_none() {
        return Err(general_err!(
          "Repetition levels are required, because max repetition level = {}",
          self.descr.max_rep_level()
        ));
      }

      // A row could contain more than one value
      // Count the occasions where we start a new row
      let levels = rep_levels.unwrap();
      for &level in levels {
        if level == 0 {
          self.rows_written += 1;
        }
      }

      self.write_repetition_levels(levels);
    } else {
      // Each value is exactly one row
      // Equals to the original number of values, so we count nulls as well
      self.rows_written += values.len();
    }

    // Check that we have enough values to write
    if values.len() < values_to_write {
      return Err(general_err!(
        "Expected to write {} values, but have only {}",
        values_to_write,
        values.len()
      ));
    }

    // TODO: update page statistics

    self.write_values(&values[0..values_to_write])?;

    self.num_buffered_values += num_values;
    self.num_buffered_encoded_values += values_to_write;

    if self.should_add_data_page() {
      self.add_data_page()?;
    }

    if self.should_dict_fallback() {
      self.dict_fallback()?;
    }

    Ok(values_to_write)
  }

  #[inline]
  fn write_definition_levels(&mut self, def_levels: &[i16]) {
    self.def_levels_sink.extend_from_slice(def_levels);
  }

  #[inline]
  fn write_repetition_levels(&mut self, rep_levels: &[i16]) {
    self.rep_levels_sink.extend_from_slice(rep_levels);
  }

  #[inline]
  fn write_values(&mut self, values: &[T::T]) -> Result<()> {
    match self.dict_encoder {
      Some(ref mut encoder) => encoder.put(values),
      None => self.encoder.put(values)
    }
  }

  /// Returns true if we need to fall back to non-dictionary encoding.
  ///
  /// We can only fall back if dictionary encoder is set and we have exceeded dictionary
  /// size.
  #[inline]
  fn should_dict_fallback(&self) -> bool {
    match self.dict_encoder {
      Some(ref encoder) => {
        encoder.dict_encoded_size() >= self.props.dictionary_pagesize_limit() as u64
      },
      None => false
    }
  }

  /// Returns true if there is enough data for a data page, false otherwise.
  #[inline]
  fn should_add_data_page(&self) -> bool {
    self.encoder.estimated_data_encoded_size() >= self.props.data_pagesize_limit() as u64
  }

  /// Performs dictionary fallback.
  /// Prepares and writes dictionary and all data pages into page writer.
  fn dict_fallback(&mut self) -> Result<()> {
    // At this point we know that we need to fall back.
    self.write_dictionary_page()?;
    self.flush_data_pages()?;
    self.dict_encoder = None;
    Ok(())
  }

  /// Adds data page.
  /// Data page is either buffered in case of dictionary encoding or written directly.
  fn add_data_page(&mut self) -> Result<()> {
    // Extract encoded values
    let values = match self.dict_encoder {
      Some(ref mut encoder) => encoder.write_indices()?,
      None => self.encoder.flush_buffer()?
    };

    // Encode definition levels (always RLE)
    let def_levels = if self.descr.max_def_level() > 0 {
      self.encode_levels(
        Encoding::RLE,
        &self.def_levels_sink[..],
        self.descr.max_def_level()
      )?
    } else {
      vec![]
    };

    // Encode repetition levels (always RLE)
    let mut rep_levels = if self.descr.max_rep_level() > 0 {
      self.encode_levels(
        Encoding::RLE,
        &self.rep_levels_sink[..],
        self.descr.max_rep_level()
      )?
    } else {
      vec![]
    };

    // Encoded length of repetition levels
    let rep_levels_byte_len = rep_levels.len();
    // Encoded length of definition levels
    let def_levels_byte_len = def_levels.len();
    // Uncompressed length in bytes, compressed length depends on writer version
    let uncompressed_size = rep_levels_byte_len + def_levels_byte_len + values.len();
    // Select encoding based on current encoder and writer version (v1 or v2)
    let encoding = if self.dict_encoder.is_some() {
      self.props.data_page_dictionary_encoding()
    } else {
      self.encoder.encoding()
    };

    let compressed_page = if self.props.writer_version() == WriterVersion::PARQUET_1_0 {
      // Process as data page v1
      rep_levels.extend_from_slice(&def_levels[..]);
      rep_levels.extend_from_slice(values.data());
      let buffer = match self.compressor {
        Some(ref mut cmpr) => {
          // TODO: figure out capacity for the compressed buffer
          let mut compressed_buf = Vec::with_capacity(rep_levels.len() / 2);
          cmpr.compress(&rep_levels[..], &mut compressed_buf)?;
          compressed_buf
        }
        None => rep_levels
      };

      CompressedPage::DataPage {
        uncompressed_size: uncompressed_size,
        buf: ByteBufferPtr::new(buffer),
        num_values: self.num_buffered_values as u32,
        encoding: encoding,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        // TODO: process statistics
        statistics: None
      }
    } else {
      // Process as data page v2
      let mut compressed_values = Vec::with_capacity(values.len());
      match self.compressor {
        Some(ref mut cmpr) => cmpr.compress(values.data(), &mut compressed_values)?,
        None => compressed_values.extend_from_slice(values.data())
      }
      rep_levels.extend_from_slice(&def_levels[..]);
      rep_levels.extend_from_slice(&compressed_values[..]);
      let buffer = rep_levels;

      CompressedPage::DataPageV2 {
        uncompressed_size: uncompressed_size,
        buf: ByteBufferPtr::new(buffer),
        num_values: self.num_buffered_values as u32,
        encoding: encoding,
        num_nulls: (self.num_buffered_values - self.num_buffered_encoded_values) as u32,
        num_rows: self.rows_written as u32,
        def_levels_byte_len: def_levels_byte_len as u32,
        rep_levels_byte_len: rep_levels_byte_len as u32,
        is_compressed: self.compressor.is_some(),
        // TODO: process statistics
        statistics: None
      }
    };

    // Check if we need to buffer data page or flush it to the sink directly
    if self.dict_encoder.is_some() {
      self.data_pages.push_back(compressed_page);
    } else {
      self.write_data_page(compressed_page)?;
    }

    // Reset state
    self.rep_levels_sink.clear();
    self.def_levels_sink.clear();
    self.num_buffered_values = 0;
    self.num_buffered_encoded_values = 0;
    self.rows_written = 0;

    Ok(())
  }

  /// Finalises any outstanding data pages and flushes buffered data pages from
  /// dictionary encoding into underlying sink.
  #[inline]
  fn flush_data_pages(&mut self) -> Result<()> {
    // Write all outstanding data to a new page
    if self.num_buffered_values > 0 {
      self.add_data_page()?;
    }

    while let Some(page) = self.data_pages.pop_front() {
      self.write_data_page(page)?;
    }

    Ok(())
  }

  /// Assembles and writes column chunk metadata.
  fn write_column_metadata(&mut self) -> Result<()> {
    let total_compressed_size = self.page_writer.total_compressed_size() as i64;
    let total_uncompressed_size = self.page_writer.total_uncompressed_size() as i64;
    let num_values = self.page_writer.num_values() as i64;
    let dict_page_offset = self.page_writer.dictionary_page_offset().map(|v| v as i64);
    // If data page offset is not set, then no pages have been written
    let data_page_offset = self.page_writer.data_page_offset().unwrap_or(0) as i64;

    let file_offset;
    let mut encodings = Vec::new();

    if self.has_dictionary {
      assert!(dict_page_offset.is_some(), "Dictionary offset is not set");
      file_offset = dict_page_offset.unwrap() + total_compressed_size;
      // NOTE: This should be in sync with writing dictionary pages
      encodings.push(self.props.dictionary_page_encoding());
      encodings.push(self.props.data_page_dictionary_encoding());
      // Fallback to alternative encoding, add it to the list
      if self.dict_encoder.is_none() {
        encodings.push(self.encoder.encoding());
      }
    } else {
      file_offset = data_page_offset + total_compressed_size;
      encodings.push(self.encoder.encoding());
    }

    let metadata = ColumnChunkMetaDataBuilder::new(self.descr.clone())
      .set_compression(self.codec)
      .set_encodings(encodings)
      .set_file_offset(file_offset)
      .set_total_compressed_size(total_compressed_size)
      .set_total_uncompressed_size(total_uncompressed_size)
      .set_num_values(num_values)
      .set_data_page_offset(data_page_offset)
      .set_dictionary_page_offset(dict_page_offset)
      .build();

    self.page_writer.write_metadata(&metadata)?;
    self.column_chunk_metadata = Some(Rc::new(metadata));

    Ok(())
  }

  /// Encodes definition or repetition levels.
  #[inline]
  fn encode_levels(
    &self,
    encoding: Encoding,
    levels: &[i16],
    max_level: i16
  ) -> Result<Vec<u8>> {
    let size = LevelEncoder::max_buffer_size(encoding, max_level, levels.len());
    let mut encoder = LevelEncoder::new(encoding, max_level, vec![0; size]);
    encoder.put(&levels)?;
    encoder.consume()
  }

  /// Writes compressed data page into underlying sink.
  #[inline]
  fn write_data_page(&mut self, page: CompressedPage) -> Result<()> {
    self.total_bytes_written += self.page_writer.write_data_page(page)? as u64;
    Ok(())
  }

  /// Writes dictionary page into underlying sink.
  /// Dictionary encoder should not be used afterwards.
  #[inline]
  fn write_dictionary_page(&mut self) -> Result<()> {
    match self.dict_encoder {
      Some(ref encoder) => {
        let num_values = encoder.num_entries() as u32;
        let buf = encoder.write_dict()?;
        let page = Page::DictionaryPage {
          buf: buf,
          num_values: num_values,
          encoding: self.props.dictionary_page_encoding(),
          // TODO: is our dictionary data sorted?
          is_sorted: false
        };
        self.total_bytes_written +=
          self.page_writer.write_dictionary_page(page)? as u64;
        Ok(())
      },
      None => Err(general_err!("Dictionary encoder is not set"))
    }
  }
}
