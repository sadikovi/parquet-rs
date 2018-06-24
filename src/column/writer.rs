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
use column::page::PageWriter;
use data_type::*;
use encodings::encoding::{DictEncoder, Encoder, get_encoder};
use errors::{ParquetError, Result};
use schema::types::ColumnDescPtr;
use util::memory::MemTracker;

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
  col_descr: ColumnDescPtr,
  col_page_writer: Box<PageWriter>
) -> ColumnWriter {
  match col_descr.physical_type() {
    Type::BOOLEAN => ColumnWriter::BoolColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::INT32 => ColumnWriter::Int32ColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::INT64 => ColumnWriter::Int64ColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::INT96 => ColumnWriter::Int96ColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::FLOAT => ColumnWriter::FloatColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::DOUBLE => ColumnWriter::DoubleColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::BYTE_ARRAY => ColumnWriter::ByteArrayColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer)),
    Type::FIXED_LEN_BYTE_ARRAY => ColumnWriter::FixedLenByteArrayColumnWriter(
      ColumnWriterImpl::new(col_descr, col_page_writer))
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
  page_writer: Box<PageWriter>,
  dict_encoder: Option<DictEncoder<T>>,
  encoder: Option<Box<Encoder<T>>>,
  rows_written: usize
}

impl<T: DataType> ColumnWriterImpl<T> where T: 'static {
  pub fn new(column_descr: ColumnDescPtr, page_writer: Box<PageWriter>) -> Self {
    Self {
      descr: column_descr,
      page_writer: page_writer,
      dict_encoder: None,
      encoder: None,
      rows_written: 0
    }
  }

  /// Sets encoding for column writer.
  /// If main encoding is dictionary encoding, then fallback is required, otherwise it is
  /// optional and ignored.
  pub fn set_encoding(&mut self, encoding: Encoding, fallback: Option<Encoding>) {
    assert!(self.encoder.is_none(), "Can set encoder(s) only once");
    let mut current = encoding;
    // Optionally set dictionary encoder.
    if current == Encoding::RLE_DICTIONARY || current == Encoding::PLAIN_DICTIONARY {
      assert!(fallback.is_some(), "Dictionary encoding requires a fallback encoding");
      self.dict_encoder = Some(DictEncoder::new(
        self.descr.clone(),
        Rc::new(MemTracker::new())
      ));
      current = fallback.unwrap();
    }

    // Set either main encoder or fallback encoder.
    self.encoder = Some(get_encoder(
      self.descr.clone(),
      current,
      Rc::new(MemTracker::new())
    ).unwrap());
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
      for &level in levels {
        if level == self.descr.max_def_level() {
          values_to_write += 1;
        }
      }

      self.write_definition_levels(levels)?;
    } else {
      values_to_write = values.len();
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

      self.write_repetition_levels(levels)?;
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

    Ok(values_to_write)
  }

  /// Returns number of rows written so far.
  pub fn rows_written(&self) -> usize {
    self.rows_written
  }

  /// Finalises writes and closes the column writer.
  pub fn close(self) -> Result<()> {
    unimplemented!();
  }

  fn write_definition_levels(&mut self, def_levels: &[i16]) -> Result<()> {
    unimplemented!();
  }

  fn write_repetition_levels(&mut self, rep_levels: &[i16]) -> Result<()> {
    unimplemented!();
  }

  fn write_values(&mut self, values: &[T::T]) -> Result<()> {
    unimplemented!();
  }
}
