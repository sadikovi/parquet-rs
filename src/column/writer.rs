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

use std::mem;
use std::rc::Rc;

use basic::{Encoding, Type};
use column::page::{CompressedPage, PageWriter};
use data_type::*;
use encodings::encoding::{DictEncoder, Encoder, get_encoder};
use encodings::levels::LevelEncoder;
use errors::{ParquetError, Result};
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
  descr: ColumnDescPtr,
  props: WriterPropertiesPtr,
  page_writer: Box<PageWriter>,
  dict_encoder: Option<DictEncoder<T>>,
  encoder: Box<Encoder<T>>,
  num_buffered_values: usize,
  num_buffered_encoded_values: usize,
  rows_written: usize,
  total_bytes_written: u64,
  def_levels_sink: Vec<i16>,
  rep_levels_sink: Vec<i16>,
  data_pages: Vec<CompressedPage>
}

impl<T: DataType> ColumnWriterImpl<T> where T: 'static {
  pub fn new(
    descr: ColumnDescPtr,
    props: WriterPropertiesPtr,
    page_writer: Box<PageWriter>
  ) -> Self {
    // Optionally set dictionary encoder.
    let dict_encoder = if props.dictionary_enabled(descr.path()) {
      Some(DictEncoder::new(descr.clone(), Rc::new(MemTracker::new())))
    } else {
      None
    };

    // Set either main encoder or fallback encoder.
    let fallback_encoder = get_encoder(
      descr.clone(),
      props.encoding(descr.path()),
      Rc::new(MemTracker::new())
    ).unwrap();

    Self {
      descr: descr,
      props: props,
      page_writer: page_writer,
      dict_encoder: dict_encoder,
      encoder: fallback_encoder,
      num_buffered_values: 0,
      num_buffered_encoded_values: 0,
      rows_written: 0,
      total_bytes_written: 0,
      def_levels_sink: vec![],
      rep_levels_sink: vec![],
      data_pages: vec![]
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

  /// Returns number of rows written so far.
  pub fn rows_written(&self) -> usize {
    self.rows_written
  }

  /// Returns total number of values that have been buffered so far.
  pub fn num_buffered_values(&self) -> usize {
    self.num_buffered_values
  }

  /// Returns number of non-null values that have been buffered so far.
  pub fn num_buffered_encoded_values(&self) -> usize {
    self.num_buffered_encoded_values
  }

  /// Finalises writes and closes the column writer.
  pub fn close(self) -> Result<()> {
    unimplemented!();
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
    unimplemented!();
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
    // Whether or not we use compressor for values
    let is_compressed = self.page_writer.has_compressor();

    let compressed_page = if self.props.writer_version() == WriterVersion::PARQUET_1_0 {
      // Process as data page v1
      rep_levels.extend_from_slice(&def_levels[..]);
      rep_levels.extend_from_slice(values.data());
      let buffer = if is_compressed {
        // TODO: figure out capacity for the compressed buffer
        let mut compressed_buf = Vec::with_capacity(rep_levels.len() / 2);
        self.page_writer.compress(&rep_levels[..], &mut compressed_buf)?;
        compressed_buf
      } else {
        rep_levels
      };

      CompressedPage::DataPage {
        uncompressed_size: uncompressed_size,
        buf: ByteBufferPtr::new(buffer),
        num_values: self.num_buffered_values as u32,
        encoding: encoding,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: None
      }
    } else {
      // Process as data page v2
      let mut compressed_values = Vec::with_capacity(values.len());
      if is_compressed {
        self.page_writer.compress(values.data(), &mut compressed_values)?;
      } else {
        compressed_values.extend_from_slice(values.data());
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
        is_compressed: is_compressed,
        statistics: None
      }
    };

    // Check if we need to buffer data page or flush it to the sink directly
    if self.dict_encoder.is_some() {
      self.data_pages.push(compressed_page);
    } else {
      self.total_bytes_written +=
        self.page_writer.write_data_page(compressed_page)? as u64;
    }

    // Reset state
    self.rep_levels_sink.clear();
    self.def_levels_sink.clear();
    self.num_buffered_values = 0;
    self.num_buffered_encoded_values = 0;
    self.rows_written = 0;

    Ok(())
  }

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
}
