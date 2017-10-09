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

use std::collections::HashMap;

use basic::{Type as PhysicalType};
use column::reader::{ColumnReader, ColumnReaderImpl, get_typed_column_reader};
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::{ColumnPath, SchemaDescriptor, Type};

pub trait ReadSupport<T> {
  fn new() -> T;

  fn initialize(&mut self);

  fn finalize(&mut self);

  fn initialize_group(&mut self, ordinal: usize, tpe: &Type);

  fn finalize_group(&mut self, ordinal: usize, tpe: &Type);

  fn add_boolean(&mut self, ordinal: usize, tpe: &Type, value: bool);

  fn add_int32(&mut self, ordinal: usize, tpe: &Type, value: i32);

  fn add_int64(&mut self, ordinal: usize, tpe: &Type, value: i64);

  fn add_int96(&mut self, ordinal: usize, tpe: &Type, value: Int96);

  fn add_float(&mut self, ordinal: usize, tpe: &Type, value: f32);

  fn add_double(&mut self, ordinal: usize, tpe: &Type, value: f64);

  fn add_byte_array(&mut self, ordinal: usize, tpe: &Type, value: ByteArray);

  fn add_fixed_len_byte_array(&mut self, ordinal: usize, tpe: &Type, value: ByteArray);
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
  pub fn new(tpe: PhysicalType, reader: ColumnReader<'a>, batch_size: usize) -> Self {
    match tpe {
      PhysicalType::BOOLEAN => ColumnBatch::BoolColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::INT32 => ColumnBatch::Int32ColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::INT64 => ColumnBatch::Int64ColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::INT96 => ColumnBatch::Int96ColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::FLOAT => ColumnBatch::FloatColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::DOUBLE => ColumnBatch::DoubleColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::BYTE_ARRAY => ColumnBatch::ByteArrayColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
      PhysicalType::FIXED_LEN_BYTE_ARRAY => ColumnBatch::FixedLenByteArrayColumnBatch(
        TypedColumnBatch::new(batch_size, reader)),
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
  fn new(batch_size: usize, column_reader: ColumnReader<'a>) -> Self {
    Self {
      reader: get_typed_column_reader(column_reader),
      values: vec![T::T::default(); batch_size],
      def_levels: vec![0; batch_size],
      rep_levels: vec![0; batch_size],
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
    self.def_levels[self.curr_index]
  }

  fn current_rep_level(&self) -> i16 {
    self.rep_levels[self.curr_index]
  }

  // TODO: change this method to be specific to data type
  fn current_value(&self) -> &T::T {
    &self.values[self.curr_index]
  }
}

// == Record reader ==

pub struct RecordReader<'a> {
  num_rows: i64,
  projection: SchemaDescriptor,
  batches: HashMap<ColumnPath, ColumnBatch<'a>>
}

impl<'a> RecordReader<'a> {
  pub fn new(
      num_rows: i64,
      projection: SchemaDescriptor,
      batches: HashMap<ColumnPath, ColumnBatch<'a>>) -> Self {
    Self {
      num_rows: num_rows,
      projection: projection,
      batches: batches
    }
  }

  pub fn next<R: ReadSupport<R>>(&mut self) -> Option<R> {
    if self.num_rows > 0 {
      let mut record = R::new();
      let root = self.projection.root_schema();
      record.initialize();
      self.traverse(0, root, &mut record);
      record.finalize();
      Some(record)
    } else {
      None
    }
  }

  fn traverse<R: ReadSupport<R>>(&self, ordinal: usize, tpe: &Type, record: &mut R) {
    match tpe {
      group @ &Type::GroupType { .. } => {
        record.initialize_group(ordinal, group);
        let mut index = 0;
        for field in group.get_fields() {
          self.traverse(index, field, record);
          index += 1;
        }
        record.finalize_group(ordinal, group);
      },
      primitive @ &Type::PrimitiveType { .. } => {
        match primitive.get_physical_type() {
          PhysicalType::BOOLEAN => {
            // let batch = self.get_column_batch::<BoolType>();
            // record.add_boolean(ordinal, primitive, batch.next_value());
          },
          PhysicalType::INT32 => {

          },
          PhysicalType::INT64 => {

          },
          PhysicalType::INT96 => {

          },
          PhysicalType::FLOAT => {

          },
          PhysicalType::DOUBLE => {

          },
          PhysicalType::BYTE_ARRAY => {

          },
          PhysicalType::FIXED_LEN_BYTE_ARRAY => {

          },
        }
      },
    }
  }
}
