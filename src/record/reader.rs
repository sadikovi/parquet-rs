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

use basic::{Type as PhysicalType};
use column::reader::{ColumnReader, ColumnReaderImpl, get_typed_column_reader};
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::{ColumnDescPtr, SchemaDescriptor, Type};

pub trait ReadSupport<T> {
  fn get_root_converter(&mut self, schema: &Type) -> Converter;

  fn get_current_record(&self) -> T;
}

pub trait Converter {
  fn start(&mut self);

  fn end(&mut self);

  fn get_child_converter(&self, ordinal: usize) -> Converter;

  fn add_boolean(&mut self, value: bool);

  fn add_int32(&mut self, value: i32);

  fn add_int64(&mut self, value: i64);

  fn add_int96(&mut self, value: Int96);

  fn add_float(&mut self, value: f32);

  fn add_double(&mut self, value: f64);

  fn add_byte_array(&mut self, value: ByteArray);

  fn add_fixed_len_byte_array(&mut self, value: ByteArray);
}

// == Column batch interface ==

pub enum ColumnBatch<'a> {
  BoolColumnBatch(TypedColumnBatch<'a, BoolType>),
  Int32ColumnBatch(TypedColumnBatch<'a, Int32Type>),
  Int64ColumnBatch(TypedColumnBatch<'a, Int64Type>),
  Int96ColumnBatch(TypedColumnBatch<'a, Int96Type>),
  FloatColumnBatch(TypedColumnBatch<'a, FloatType>),
  DoubleColumnBatch(TypedColumnBatch<'a, DoubleType>),
  ByteArrayColumnBatch(TypedColumnBatch<'a, ByteArrayType>),
  FixedLenByteArrayColumnBatch(TypedColumnBatch<'a, FixedLenByteArrayType>),
}

impl<'a> ColumnBatch<'a> {
  pub fn new(descr: ColumnDescPtr, reader: ColumnReader<'a>, batch_size: usize) -> Self {
    match descr.physical_type() {
      PhysicalType::BOOLEAN => ColumnBatch::BoolColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::INT32 => ColumnBatch::Int32ColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::INT64 => ColumnBatch::Int64ColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::INT96 => ColumnBatch::Int96ColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::FLOAT => ColumnBatch::FloatColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::DOUBLE => ColumnBatch::DoubleColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::BYTE_ARRAY => ColumnBatch::ByteArrayColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
      PhysicalType::FIXED_LEN_BYTE_ARRAY => ColumnBatch::FixedLenByteArrayColumnBatch(
        TypedColumnBatch::new(descr, batch_size, reader)),
    }
  }

  pub fn consume(&mut self) -> Result<()> {
    match *self {
      ColumnBatch::BoolColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::Int32ColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::Int64ColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::Int96ColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::FloatColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::DoubleColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::ByteArrayColumnBatch(ref mut batch) => batch.consume(),
      ColumnBatch::FixedLenByteArrayColumnBatch(ref mut batch) => batch.consume(),
    }
  }

  pub fn current_def_level(&self) -> i16 {
    match *self {
      ColumnBatch::BoolColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::Int32ColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::Int64ColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::Int96ColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::FloatColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::DoubleColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::ByteArrayColumnBatch(ref batch) => batch.current_def_level(),
      ColumnBatch::FixedLenByteArrayColumnBatch(ref batch) => batch.current_def_level(),
    }
  }

  pub fn current_rep_level(&self) -> i16 {
    match *self {
      ColumnBatch::BoolColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::Int32ColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::Int64ColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::Int96ColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::FloatColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::DoubleColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::ByteArrayColumnBatch(ref batch) => batch.current_rep_level(),
      ColumnBatch::FixedLenByteArrayColumnBatch(ref batch) => batch.current_rep_level(),
    }
  }

  pub fn update_value(&self, converter: &mut Converter) {
    match *self {
      ColumnBatch::BoolColumnBatch(ref batch) => {
        converter.add_boolean(*batch.current_value());
      },
      ColumnBatch::Int32ColumnBatch(ref batch) => {
        converter.add_int32(*batch.current_value());
      },
      ColumnBatch::Int64ColumnBatch(ref batch) => {
        converter.add_int64(*batch.current_value());
      },
      ColumnBatch::Int96ColumnBatch(ref batch) => {
        converter.add_int96(batch.current_value().clone());
      },
      ColumnBatch::FloatColumnBatch(ref batch) => {
        converter.add_float(*batch.current_value());
      },
      ColumnBatch::DoubleColumnBatch(ref batch) => {
        converter.add_double(*batch.current_value());
      },
      ColumnBatch::ByteArrayColumnBatch(ref batch) => {
        converter.add_byte_array(batch.current_value().clone());
      },
      ColumnBatch::FixedLenByteArrayColumnBatch(ref batch) => {
        converter.add_fixed_len_byte_array(batch.current_value().clone());
      },
    }
  }
}

pub struct TypedColumnBatch<'a, T: DataType> {
  reader: ColumnReaderImpl<'a, T>,
  values: Vec<T::T>,
  def_levels: Vec<i16>,
  rep_levels: Vec<i16>,
  curr_index: usize,
  values_left: usize,
  can_buffer: bool
}

impl<'a, T: DataType> TypedColumnBatch<'a, T> where T: 'static {
  fn new(descr: ColumnDescPtr, batch_size: usize, column_reader: ColumnReader<'a>) -> Self {
    assert!(batch_size > 0, "Expected positive batch size, found: {}", batch_size);
    let def_levels = if descr.max_def_level() > 0 { vec![0; batch_size] } else { vec![0; 0] };
    let rep_levels = if descr.max_rep_level() > 0 { vec![0; batch_size] } else { vec![0; 0] };

    Self {
      reader: get_typed_column_reader(column_reader),
      values: vec![T::T::default(); batch_size],
      def_levels: def_levels,
      rep_levels: rep_levels,
      curr_index: 0,
      values_left: 0,
      can_buffer: true
    }
  }

  // consume next value and/or buffer more values
  fn consume(&mut self) -> Result<()> {
    self.curr_index += 1;
    if self.curr_index >= self.values_left {
      if !self.can_buffer {
        return Err(general_err!("Cannot buffer more values, column reader is empty"));
      }
      let (values, _levels) = self.reader.read_batch(self.values.len(),
        Some(&mut self.def_levels), Some(&mut self.rep_levels), &mut self.values)?;
      self.can_buffer = values == self.values.len();
      self.curr_index = 0;
      self.values_left = values;
    }
    Ok(())
  }

  fn current_def_level(&self) -> i16 {
    if self.def_levels.len() == 0 { 0 } else { self.def_levels[self.curr_index] }
  }

  fn current_rep_level(&self) -> i16 {
    if self.rep_levels.len() == 0 { 0 } else { self.rep_levels[self.curr_index] }
  }

  fn current_value(&self) -> &T::T {
    &self.values[self.curr_index]
  }
}

// == Record reader ==

pub struct RecordReader<'a> {
  num_rows: i64,
  proj: SchemaDescriptor,
  batches: Vec<ColumnBatch<'a>>
}

impl<'a> RecordReader<'a> {
  pub fn new(num_rows: i64, proj: SchemaDescriptor, batches: Vec<ColumnBatch<'a>>) -> Self {
    Self { num_rows: num_rows, proj: proj, batches: batches }
  }

  pub fn next<R: ReadSupport<R>>(&mut self) -> Option<R> {
    if self.num_rows <= 0 {
      return None;
    }
    self.num_rows -= 1;
    None
  }
}
