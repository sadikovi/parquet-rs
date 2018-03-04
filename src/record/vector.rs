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

use record::api::PrimitiveConverter;
use basic::{Type as PhysicalType};
use column::reader::{ColumnReader, ColumnReaderImpl, get_typed_column_reader};
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::ColumnDescPtr;

/// High level API wrapper on column reader.
/// Provides per-element access for each primitive column.
pub enum ColumnVector<'a> {
  BoolColumnVector(TypedColumnVector<'a, BoolType>),
  Int32ColumnVector(TypedColumnVector<'a, Int32Type>),
  Int64ColumnVector(TypedColumnVector<'a, Int64Type>),
  Int96ColumnVector(TypedColumnVector<'a, Int96Type>),
  FloatColumnVector(TypedColumnVector<'a, FloatType>),
  DoubleColumnVector(TypedColumnVector<'a, DoubleType>),
  ByteArrayColumnVector(TypedColumnVector<'a, ByteArrayType>),
  FixedLenByteArrayColumnVector(TypedColumnVector<'a, FixedLenByteArrayType>)
}

impl<'a> ColumnVector<'a> {
  /// Creates new column vector for column reader
  pub fn new(descr: ColumnDescPtr, reader: ColumnReader<'a>, batch_size: usize) -> Self {
    match descr.physical_type() {
      PhysicalType::BOOLEAN => {
        ColumnVector::BoolColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::INT32 => {
        ColumnVector::Int32ColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::INT64 => {
        ColumnVector::Int64ColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::INT96 => {
        ColumnVector::Int96ColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::FLOAT => {
        ColumnVector::FloatColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::DOUBLE => {
        ColumnVector::DoubleColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::BYTE_ARRAY => {
        ColumnVector::ByteArrayColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      },
      PhysicalType::FIXED_LEN_BYTE_ARRAY => {
        ColumnVector::FixedLenByteArrayColumnVector(
          TypedColumnVector::new(descr, batch_size, reader))
      }
    }
  }

  /// Invokes underlying typed column vector to buffer current value.
  /// Should be called once - either before `is_null` or `update_value`.
  pub fn consume(&mut self) -> Result<bool> {
    match *self {
      ColumnVector::BoolColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::Int32ColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::Int64ColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::Int96ColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::FloatColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::DoubleColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::ByteArrayColumnVector(ref mut typed) => typed.consume(),
      ColumnVector::FixedLenByteArrayColumnVector(ref mut typed) => typed.consume()
    }
  }

  /// Returns current definition level for a leaf vector
  pub fn current_def_level(&self) -> i16 {
    match *self {
      ColumnVector::BoolColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::Int32ColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::Int64ColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::Int96ColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::FloatColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::DoubleColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::ByteArrayColumnVector(ref typed) => typed.current_def_level(),
      ColumnVector::FixedLenByteArrayColumnVector(ref typed) => typed.current_def_level()
    }
  }

  /// Returns max definition level for a leaf vector
  pub fn max_def_level(&self) -> i16 {
    match *self {
      ColumnVector::BoolColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::Int32ColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::Int64ColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::Int96ColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::FloatColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::DoubleColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::ByteArrayColumnVector(ref typed) => typed.max_def_level(),
      ColumnVector::FixedLenByteArrayColumnVector(ref typed) => typed.max_def_level()
    }
  }

  /// Returns true, if current value is null.
  /// Based on the fact that for non-null value current definition level
  /// equals to max definition level.
  pub fn is_null(&self) -> bool {
    self.current_def_level() < self.max_def_level()
  }

  /// Updates non-null value for primitive converter.
  pub fn update_value(&self, converter: &mut PrimitiveConverter) {
    assert!(self.is_null(), "Value is null");
    match *self {
      ColumnVector::BoolColumnVector(ref typed) => {
        converter.add_boolean(*typed.current_value());
      },
      ColumnVector::Int32ColumnVector(ref typed) => {
        converter.add_int32(*typed.current_value());
      },
      ColumnVector::Int64ColumnVector(ref typed) => {
        converter.add_int64(*typed.current_value());
      },
      ColumnVector::Int96ColumnVector(ref typed) => {
        converter.add_int96(typed.current_value().clone());
      },
      ColumnVector::FloatColumnVector(ref typed) => {
        converter.add_float(*typed.current_value());
      },
      ColumnVector::DoubleColumnVector(ref typed) => {
        converter.add_double(*typed.current_value());
      },
      ColumnVector::ByteArrayColumnVector(ref typed) => {
        converter.add_byte_array(typed.current_value().clone());
      },
      ColumnVector::FixedLenByteArrayColumnVector(ref typed) => {
        converter.add_fixed_len_byte_array(typed.current_value().clone());
      }
    }
  }

  /// Test method to print values being traversed.
  pub fn print_test_value(&self) {
    if self.is_null() {
      println!("  is_null: {}, value: null", self.is_null());
    } else {
      match *self {
        ColumnVector::BoolColumnVector(ref typed) => {
          println!("  is_null: {}, value: {}", self.is_null(), typed.current_value());
        },
        ColumnVector::Int32ColumnVector(ref typed) => {
          println!("  is_null: {}, value: {}", self.is_null(), typed.current_value());
        },
        ColumnVector::Int64ColumnVector(ref typed) => {
          println!("  is_null: {}, value: {}", self.is_null(), typed.current_value());
        },
        ColumnVector::Int96ColumnVector(ref typed) => {
          println!("  is_null: {}, value: {:?}", self.is_null(), typed.current_value());
        },
        ColumnVector::FloatColumnVector(ref typed) => {
          println!("  is_null: {}, value: {}", self.is_null(), typed.current_value());
        },
        ColumnVector::DoubleColumnVector(ref typed) => {
          println!("  is_null: {}, value: {}", self.is_null(), typed.current_value());
        },
        ColumnVector::ByteArrayColumnVector(ref typed) => {
          println!("  is_null: {}, value: {:?}", self.is_null(), typed.current_value());
        },
        ColumnVector::FixedLenByteArrayColumnVector(ref typed) => {
          println!("  is_null: {}, value: {:?}", self.is_null(), typed.current_value());
        }
      }
    }
  }
}

/// Internal column vector as a wrapper for column reader (primitive leaf column).
/// Provides per-element access.
pub struct TypedColumnVector<'a, T: DataType> {
  reader: ColumnReaderImpl<'a, T>,
  batch_size: usize,
  // type properties
  max_def_level: i16,
  max_rep_level: i16,
  // values and levels
  values: Vec<T::T>,
  def_levels: Option<Vec<i16>>,
  rep_levels: Option<Vec<i16>>,
  // current index for the triplet (value, def, rep)
  curr_triplet_index: usize,
  // how many triplets are left before we need to buffer
  triplets_left: usize
}

impl<'a, T: DataType> TypedColumnVector<'a, T> where T: 'static {
  /// Creates new typed column vector based on provided column reader.
  /// Use batch size to specify the amount of values to buffer from column reader.
  fn new(descr: ColumnDescPtr, batch_size: usize, column_reader: ColumnReader<'a>) -> Self {
    assert!(batch_size > 0, "Expected positive batch size, found: {}", batch_size);

    let max_def_level = descr.max_def_level();
    let max_rep_level = descr.max_rep_level();

    let def_levels = if max_def_level == 0 { None } else { Some(vec![0; batch_size]) };
    let rep_levels = if max_rep_level == 0 { None } else { Some(vec![0; batch_size]) };

    Self {
      reader: get_typed_column_reader(column_reader),
      batch_size: batch_size,
      max_def_level: max_def_level,
      max_rep_level: max_rep_level,
      values: vec![T::T::default(); batch_size],
      def_levels: def_levels,
      rep_levels: rep_levels,
      curr_triplet_index: 0,
      triplets_left: 0
    }
  }

  fn max_def_level(&self) -> i16 {
    self.max_def_level
  }

  fn max_rep_level(&self) -> i16 {
    self.max_rep_level
  }

  fn current_value(&self) -> &T::T {
    // We might want to remove this check because column vector would check that anyway
    assert!(
      self.current_def_level() == self.max_def_level(),
      "Cannot extract value, max definition level: {}, current level: {}",
      self.max_def_level(), self.current_def_level()
    );
    &self.values[self.curr_triplet_index]
  }

  fn current_def_level(&self) -> i16 {
    match self.def_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_def_level
    }
  }

  fn current_rep_level(&self) -> i16 {
    match self.rep_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_rep_level
    }
  }

  /// Consumes and advances to the next triplet.
  /// Returns true, if there are more records to read, false there are no records left.
  fn consume(&mut self) -> Result<bool> {
    self.curr_triplet_index += 1;

    if self.curr_triplet_index >= self.triplets_left {
      let (values_read, levels_read) = {
        // Get slice of definition levels, if available
        let def_levels = match self.def_levels {
          Some(ref mut vec) => Some(&mut vec[..]),
          None => None
        };

        // Get slice of repetition levels, if available
        let rep_levels = match self.rep_levels {
          Some(ref mut vec) => Some(&mut vec[..]),
          None => None
        };

        // buffer triplets
        self.reader.read_batch(
          self.batch_size,
          def_levels,
          rep_levels,
          &mut self.values
        )?
      };

      // No more values or levels to read
      if values_read == 0 && levels_read == 0 {
        return Ok(false);
      }

      println!(" values_read: {}, levels_read: {}", values_read, levels_read);
      println!(" values: {:?}, def_levels: {:?}, rep_levels: {:?}", self.values, self.def_levels, self.rep_levels);

      // We never read values more than levels
      if levels_read == 0 || values_read == levels_read {
        // no definition levels read, column is required
        // or definition levels match values, so it does not require spacing
        self.curr_triplet_index = 0;
        self.triplets_left = values_read;
      } else if values_read < levels_read {
        // add spacing for triplets
        // if values_read == 0, then spacing will not be triggered
        let mut idx = values_read;
        for i in 0..levels_read {
          if self.def_levels.as_ref().unwrap()[levels_read - i - 1] == self.max_def_level {
            idx -= 1; // This is done to avoid usize becoming a negative value
            self.values.swap(levels_read - i - 1, idx);
          }
        }
        self.curr_triplet_index = 0;
        self.triplets_left = levels_read;
      } else {
        return Err(general_err!(
          "Spacing of values/levels is wrong, values_read: {}, levels_read: {}",
          values_read, levels_read
        ));
      }
    }

    Ok(true)
  }
}
