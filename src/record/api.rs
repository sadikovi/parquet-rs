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

use schema::types::{Type as SchemaType};
use data_type::*;

pub trait RecordMaterializer {
  // this method is called once
  fn init(&mut self, schema: &SchemaType);

  fn root_converter(&mut self) -> &mut GroupConverter;

  fn consume_current_record(&mut self);
}

pub trait GroupConverter {
  fn start(&mut self);

  fn end(&mut self);

  fn child_group_converter(&mut self, ordinal: usize) -> &mut GroupConverter;

  fn child_primitive_converter(&mut self, ordinal: usize) -> &mut PrimitiveConverter;
}

pub trait PrimitiveConverter {
  fn add_boolean(&mut self, value: bool);

  fn add_int32(&mut self, value: i32);

  fn add_int64(&mut self, value: i64);

  fn add_int96(&mut self, value: Int96);

  fn add_float(&mut self, value: f32);

  fn add_double(&mut self, value: f64);

  fn add_byte_array(&mut self, value: ByteArray);

  fn add_fixed_len_byte_array(&mut self, value: ByteArray);
}
