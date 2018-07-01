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
use std::rc::Rc;

use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;

fn main() {
  let file = File::create("./sample.parquet").unwrap();

  let message_type = "
  message spark_schema {
    REQUIRED INT32 a;
    REQUIRED BYTE_ARRAY b (UTF8);
    REQUIRED BOOLEAN c;
  }
  ";
  let schema = parse_message_type(message_type).unwrap();
  let properties = WriterProperties::builder().build();

  let mut writer = SerializedFileWriter::new(file, Rc::new(schema), Rc::new(properties));

  let mut row_group = writer.create_row_group();

  while row_group.has_next_column() {
    let col = row_group.next_column().unwrap();
    match col {
      ColumnWriter::BoolColumnWriter(ref mut typed) => {
        typed.write_batch(&[true, false, true], None, None).unwrap();
      },
      ColumnWriter::Int32ColumnWriter(ref mut typed) => {
        typed.write_batch(&[1, 2, 3], None, None).unwrap();
      },
      ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
        typed.write_batch(
          &[ByteArray::from("abc"), ByteArray::from("def"), ByteArray::from("ghi")],
          None,
          None
        ).unwrap();
      },
      _ => panic!("Unsupported column type")
    }
  }

  writer.append_row_group(row_group).unwrap();
  writer.close().unwrap();

  println!("File is written!");
}
