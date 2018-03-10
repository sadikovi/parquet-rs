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
use std::fmt;
use basic::{Type as PhysicalType, LogicalType};
use data_type::{ByteArray, Int96};

#[derive(Clone, Debug)]
pub enum Row {
  Null,
  Bool(bool),
  Byte(i8),
  Short(i16),
  Int(i32),
  Long(i64),
  Float(f32),
  Double(f64),
  Str(String),
  Bytes(ByteArray), // should we change to Vec<u8>?
  Group(HashMap<String, Row>)
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

  pub fn new_int96(_logical_type: LogicalType, _value: Int96) -> Self {
    unimplemented!();
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
      Row::Float(value) => write!(f, "{}", value),
      Row::Double(value) => write!(f, "{}", value),
      Row::Str(ref value) => write!(f, "{}", value),
      Row::Bytes(ref value) => write!(f, "{:?}", value.data()),
      Row::Group(ref map) => write!(f, "{:?}", map)
    }
  }
}
