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

use std::fmt;
use basic::{Type as PhysicalType, LogicalType};
use data_type::{ByteArray, Int96};

#[derive(Clone, Debug)]
pub enum Row {
  // Primitive types
  Null,
  Bool(bool),
  Byte(i8),
  Short(i16),
  Int(i32),
  Long(i64),
  Float(f32),
  Double(f64),
  Str(String),
  Bytes(ByteArray),
  Timestamp(u64), // timestamp with milliseconds
  // Complex types
  Group(Vec<(String, Row)>), // struct type, child elements are tuples of key-value pairs
  List(Vec<Row>), // list of elements
  Map(Vec<(Row, Row)>) // list of key-value pairs
}

impl Row {
  pub fn new_bool(value: bool) -> Self {
    Row::Bool(value)
  }

  pub fn new_int32(logical_type: LogicalType, value: i32) -> Self {
    match logical_type {
      LogicalType::INT_8 => Row::Byte(value as i8),
      LogicalType::INT_16 => Row::Short(value as i16),
      LogicalType::INT_32 | LogicalType::NONE => Row::Int(value),
      _ => unimplemented!()
    }
  }

  pub fn new_int64(logical_type: LogicalType, value: i64) -> Self {
    match logical_type {
      LogicalType::INT_64 | LogicalType::NONE => Row::Long(value),
      _ => unimplemented!()
    }
  }

  pub fn new_int96(value: Int96) -> Self {
    // Converts nanosecond timestamps stored as INT96 into internal milliseconds

    let julian_to_unix_epoch_days: u64 = 2_440_588;
    let milli_seconds_in_a_day: u64 = 86_400_000;
    let nano_seconds_in_a_day: u64 = milli_seconds_in_a_day * 1_000_000;

    let days_since_epoch = value.data()[2] as u64 - julian_to_unix_epoch_days;
    let nanoseconds: u64 = ((value.data()[1] as u64) << 32) + value.data()[0] as u64;
    let nanos = days_since_epoch * nano_seconds_in_a_day + nanoseconds;
    let millis = nanos / 1_000_000;

    Row::Timestamp(millis)
  }

  pub fn new_float(value: f32) -> Self {
    Row::Float(value)
  }

  pub fn new_double(value: f64) -> Self {
    Row::Double(value)
  }

  pub fn new_byte_array(
    physical_type: PhysicalType,
    logical_type: LogicalType,
    value: ByteArray
  ) -> Self {
    match physical_type {
      PhysicalType::BYTE_ARRAY => {
        match logical_type {
          LogicalType::UTF8 | LogicalType::ENUM | LogicalType::JSON => {
            Row::Str(String::from_utf8(value.data().to_vec()).unwrap())
          },
          LogicalType::BSON | LogicalType::NONE => Row::Bytes(value),
          _ => unimplemented!()
        }
      },
      _ => unimplemented!()
    }
  }
}

impl fmt::Display for Row {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Row::Null => write!(f, "null"),
      Row::Bool(value) => write!(f, "{}", value),
      Row::Byte(value) => write!(f, "{}", value),
      Row::Short(value) => write!(f, "{}", value),
      Row::Int(value) => write!(f, "{}", value),
      Row::Long(value) => write!(f, "{}", value),
      Row::Float(value) => write!(f, "{:?}", value),
      Row::Double(value) => write!(f, "{:?}", value),
      Row::Str(ref value) => write!(f, "\"{}\"", value),
      Row::Bytes(ref value) => write!(f, "{:?}", value.data()),
      Row::Timestamp(value) => write!(f, "{}", value),
      Row::Group(ref fields) => {
        // Sort keys in ascending order for consistent display
        write!(f, "[")?;
        for (i, &(ref key, ref value)) in fields.iter().enumerate() {
          key.fmt(f)?;
          write!(f, ": ")?;
          value.fmt(f)?;
          if i < fields.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "]")
      },
      Row::List(ref fields) => {
        write!(f, "[")?;
        for (i, field) in fields.iter().enumerate() {
          field.fmt(f)?;
          if i < fields.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "]")
      },
      Row::Map(ref pairs) => {
        write!(f, "{{")?;
        for (i, &(ref key, ref value)) in pairs.iter().enumerate() {
          key.fmt(f)?;
          write!(f, ": ")?;
          value.fmt(f)?;
          if i < pairs.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "}}")
      }
    }
  }
}
