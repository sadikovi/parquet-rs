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

extern crate parquet;

use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;

fn main() {
  let path = Path::new("data/complex.snappy.parquet");
  let file = File::open(&path).unwrap();
  let parquet_reader = SerializedFileReader::new(file).unwrap();
  let metadata = parquet_reader.metadata();

  for i in 0..metadata.num_row_groups() {
    let proj = parse_message_type("
      message spark_schema {
        optional group b {
          optional int32 _1;
          optional int32 _2;
        }
      }
    ").unwrap();
    let row_group_reader = parquet_reader.get_row_group(i).unwrap();
    let support = row_group_reader.get_read_support(proj).unwrap();
    println!("Num records: {}", support.num_rows());
    // read records
    for i in 0..support.num_rows() {
      println!("- row {}", i);
      traverse(0, 0, support.root_schema());
    }
  }
}

fn traverse(level: usize, index: usize, type_ref: &Type) {
  match *type_ref {
    Type::GroupType { basic_info: ref _info, ref fields } => {
      println!("{}Group type: {} ({})", "  ".repeat(level), index, type_ref.name());
      for i in 0..fields.len() {
        traverse(level + 1, i, &fields[i]);
      }
    },
    Type::PrimitiveType { .. } => {
      println!("{}Primitive type: {} ({})", "  ".repeat(level), index, type_ref.name());
    },
  }
}
