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

use record::api::Row;
use basic::{Type as PhysicalType, LogicalType};
use column::reader::{ColumnReader, ColumnReaderImpl, get_typed_column_reader};
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::ColumnDescPtr;

/// High level API wrapper on column reader.
/// Provides per-element access for each primitive column.
pub enum TripletIter {
  BoolTripletIter(TypedTripletIter<BoolType>),
  Int32TripletIter(TypedTripletIter<Int32Type>),
  Int64TripletIter(TypedTripletIter<Int64Type>),
  Int96TripletIter(TypedTripletIter<Int96Type>),
  FloatTripletIter(TypedTripletIter<FloatType>),
  DoubleTripletIter(TypedTripletIter<DoubleType>),
  ByteArrayTripletIter(TypedTripletIter<ByteArrayType>),
  FixedLenByteArrayTripletIter(TypedTripletIter<FixedLenByteArrayType>)
}

impl TripletIter {
  /// Creates new triplet for column reader
  pub fn new(descr: ColumnDescPtr, reader: ColumnReader, batch_size: usize) -> Self {
    match descr.physical_type() {
      PhysicalType::BOOLEAN => {
        TripletIter::BoolTripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::INT32 => {
        TripletIter::Int32TripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::INT64 => {
        TripletIter::Int64TripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::INT96 => {
        TripletIter::Int96TripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::FLOAT => {
        TripletIter::FloatTripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::DOUBLE => {
        TripletIter::DoubleTripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::BYTE_ARRAY => {
        TripletIter::ByteArrayTripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      },
      PhysicalType::FIXED_LEN_BYTE_ARRAY => {
        TripletIter::FixedLenByteArrayTripletIter(
          TypedTripletIter::new(descr, batch_size, reader))
      }
    }
  }

  /// Invokes underlying typed triplet iterator to buffer current value.
  /// Should be called once - either before `is_null` or `update_value`.
  pub fn read_next(&mut self) -> Result<bool> {
    match *self {
      TripletIter::BoolTripletIter(ref mut typed) => typed.read_next(),
      TripletIter::Int32TripletIter(ref mut typed) => typed.read_next(),
      TripletIter::Int64TripletIter(ref mut typed) => typed.read_next(),
      TripletIter::Int96TripletIter(ref mut typed) => typed.read_next(),
      TripletIter::FloatTripletIter(ref mut typed) => typed.read_next(),
      TripletIter::DoubleTripletIter(ref mut typed) => typed.read_next(),
      TripletIter::ByteArrayTripletIter(ref mut typed) => typed.read_next(),
      TripletIter::FixedLenByteArrayTripletIter(ref mut typed) => typed.read_next()
    }
  }

  /// Provides check on values/levels left without invoking the underlying typed triplet
  /// iterator.
  /// Returns true if more values/levels exist, false otherwise.
  /// It is always in sync with `read_next` method.
  pub fn has_next(&self) -> bool {
    match *self {
      TripletIter::BoolTripletIter(ref typed) => typed.has_next(),
      TripletIter::Int32TripletIter(ref typed) => typed.has_next(),
      TripletIter::Int64TripletIter(ref typed) => typed.has_next(),
      TripletIter::Int96TripletIter(ref typed) => typed.has_next(),
      TripletIter::FloatTripletIter(ref typed) => typed.has_next(),
      TripletIter::DoubleTripletIter(ref typed) => typed.has_next(),
      TripletIter::ByteArrayTripletIter(ref typed) => typed.has_next(),
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => typed.has_next()
    }
  }

  /// Returns current definition level for a leaf triplet iterator
  pub fn current_def_level(&self) -> i16 {
    match *self {
      TripletIter::BoolTripletIter(ref typed) => typed.current_def_level(),
      TripletIter::Int32TripletIter(ref typed) => typed.current_def_level(),
      TripletIter::Int64TripletIter(ref typed) => typed.current_def_level(),
      TripletIter::Int96TripletIter(ref typed) => typed.current_def_level(),
      TripletIter::FloatTripletIter(ref typed) => typed.current_def_level(),
      TripletIter::DoubleTripletIter(ref typed) => typed.current_def_level(),
      TripletIter::ByteArrayTripletIter(ref typed) => typed.current_def_level(),
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => typed.current_def_level()
    }
  }

  /// Returns max definition level for a leaf triplet iterator
  pub fn max_def_level(&self) -> i16 {
    match *self {
      TripletIter::BoolTripletIter(ref typed) => typed.max_def_level(),
      TripletIter::Int32TripletIter(ref typed) => typed.max_def_level(),
      TripletIter::Int64TripletIter(ref typed) => typed.max_def_level(),
      TripletIter::Int96TripletIter(ref typed) => typed.max_def_level(),
      TripletIter::FloatTripletIter(ref typed) => typed.max_def_level(),
      TripletIter::DoubleTripletIter(ref typed) => typed.max_def_level(),
      TripletIter::ByteArrayTripletIter(ref typed) => typed.max_def_level(),
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => typed.max_def_level()
    }
  }

  /// Returns current repetition level for a leaf triplet iterator
  pub fn current_rep_level(&self) -> i16 {
    match *self {
      TripletIter::BoolTripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::Int32TripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::Int64TripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::Int96TripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::FloatTripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::DoubleTripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::ByteArrayTripletIter(ref typed) => typed.current_rep_level(),
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => typed.current_rep_level()
    }
  }

  /// Returns max repetition level for a leaf triplet iterator
  pub fn max_rep_level(&self) -> i16 {
    match *self {
      TripletIter::BoolTripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::Int32TripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::Int64TripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::Int96TripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::FloatTripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::DoubleTripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::ByteArrayTripletIter(ref typed) => typed.max_rep_level(),
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => typed.max_rep_level()
    }
  }

  /// Returns true, if current value is null.
  /// Based on the fact that for non-null value current definition level
  /// equals to max definition level.
  pub fn is_null(&self) -> bool {
    self.current_def_level() < self.max_def_level()
  }

  /// Updates non-null value for current row.
  pub fn current_value(&self) -> Row {
    assert!(!self.is_null(), "Value is null");
    match *self {
      TripletIter::BoolTripletIter(ref typed) => {
        Row::convert_bool(
          typed.physical_type(), typed.logical_type(), *typed.current_value())
      },
      TripletIter::Int32TripletIter(ref typed) => {
        Row::convert_int32(
          typed.physical_type(), typed.logical_type(), *typed.current_value())
      },
      TripletIter::Int64TripletIter(ref typed) => {
        Row::convert_int64(
          typed.physical_type(), typed.logical_type(), *typed.current_value())
      },
      TripletIter::Int96TripletIter(ref typed) => {
        Row::convert_int96(
          typed.physical_type(), typed.logical_type(), typed.current_value().clone())
      },
      TripletIter::FloatTripletIter(ref typed) => {
        Row::convert_float(
          typed.physical_type(), typed.logical_type(), *typed.current_value())
      },
      TripletIter::DoubleTripletIter(ref typed) => {
        Row::convert_double(
          typed.physical_type(), typed.logical_type(), *typed.current_value())
      },
      TripletIter::ByteArrayTripletIter(ref typed) => {
        Row::convert_byte_array(
          typed.physical_type(), typed.logical_type(), typed.current_value().clone())
      },
      TripletIter::FixedLenByteArrayTripletIter(ref typed) => {
        Row::convert_byte_array(
          typed.physical_type(), typed.logical_type(), typed.current_value().clone())
      }
    }
  }
}

/// Internal typed triplet iterator as a wrapper for column reader
/// (primitive leaf column), provides per-element access.
pub struct TypedTripletIter<T: DataType> {
  reader: ColumnReaderImpl<T>,
  physical_type: PhysicalType,
  logical_type: LogicalType,
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
  triplets_left: usize,
  // helper flag to quickly check if we have more values/levels to read
  has_next: bool
}

impl<T: DataType> TypedTripletIter<T> where T: 'static {
  /// Creates new typed triplet iterator based on provided column reader.
  /// Use batch size to specify the amount of values to buffer from column reader.
  fn new(
    descr: ColumnDescPtr,
    batch_size: usize,
    column_reader: ColumnReader
  ) -> Self {
    assert!(batch_size > 0, "Expected positive batch size, found: {}", batch_size);

    let max_def_level = descr.max_def_level();
    let max_rep_level = descr.max_rep_level();

    let def_levels = if max_def_level == 0 { None } else { Some(vec![0; batch_size]) };
    let rep_levels = if max_rep_level == 0 { None } else { Some(vec![0; batch_size]) };

    Self {
      reader: get_typed_column_reader(column_reader),
      physical_type: descr.physical_type(),
      logical_type: descr.logical_type(),
      batch_size: batch_size,
      max_def_level: max_def_level,
      max_rep_level: max_rep_level,
      values: vec![T::T::default(); batch_size],
      def_levels: def_levels,
      rep_levels: rep_levels,
      curr_triplet_index: 0,
      triplets_left: 0,
      has_next: false
    }
  }

  /// Returns physical type for the current typed triplet iterator.
  pub fn physical_type(&self) -> PhysicalType {
    self.physical_type
  }

  /// Returns logical type for the current typed triplet iterator.
  pub fn logical_type(&self) -> LogicalType {
    self.logical_type
  }

  /// Returns maximum definition level for the triplet iterator (leaf column).
  fn max_def_level(&self) -> i16 {
    self.max_def_level
  }

  /// Returns maximum repetition level for the triplet iterator (leaf column).
  fn max_rep_level(&self) -> i16 {
    self.max_rep_level
  }

  /// Returns current value.
  /// Method does not advance the iterator, therefore can be called multiple times.
  fn current_value(&self) -> &T::T {
    assert!(
      self.current_def_level() == self.max_def_level(),
      "Cannot extract value, max definition level: {}, current level: {}",
      self.max_def_level(), self.current_def_level()
    );
    &self.values[self.curr_triplet_index]
  }

  /// Returns current definition level.
  /// If field is required, then maximum definition level is returned.
  fn current_def_level(&self) -> i16 {
    match self.def_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_def_level
    }
  }

  /// Returns current repetition level.
  /// If field is required, then maximum repetition level is returned.
  fn current_rep_level(&self) -> i16 {
    match self.rep_levels {
      Some(ref vec) => vec[self.curr_triplet_index],
      None => self.max_rep_level
    }
  }

  /// Advances to the next triplet.
  /// Returns true, if there are more records to read, false there are no records left.
  fn read_next(&mut self) -> Result<bool> {
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

        // Buffer triplets
        self.reader.read_batch(
          self.batch_size,
          def_levels,
          rep_levels,
          &mut self.values
        )?
      };

      // No more values or levels to read
      if values_read == 0 && levels_read == 0 {
        self.has_next = false;
        return Ok(false);
      }

      // We never read values more than levels
      if levels_read == 0 || values_read == levels_read {
        // There are no definition levels to read, column is required
        // or definition levels match values, so it does not require spacing
        self.curr_triplet_index = 0;
        self.triplets_left = values_read;
      } else if values_read < levels_read {
        // Add spacing for triplets.
        // The idea is setting values for positions in def_levels when current definition
        // level equals to maximum definition level. Values and levels are guaranteed to
        // line up, because of the column reader method.

        // Note: if values_read == 0, then spacing will not be triggered
        let mut idx = values_read;
        let def_levels = self.def_levels.as_ref().unwrap();
        for i in 0..levels_read {
          if def_levels[levels_read - i - 1] == self.max_def_level {
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

    self.has_next = true;
    Ok(true)
  }

  /// Quick check if iterator has more values/levels to read.
  /// It is updated as a result of `read_next` method, so they are synchronized.
  fn has_next(&self) -> bool {
    self.has_next
  }
}
