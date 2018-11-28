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
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::ColumnPath;

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
  let properties = WriterProperties::builder()
    .set_column_dictionary_enabled(ColumnPath::from("c"), false)
    .set_writer_version(WriterVersion::PARQUET_1_0)
    .build();

  let mut writer = SerializedFileWriter::new(file, Rc::new(schema), Rc::new(properties))
    .unwrap();

  let mut row_group = writer.next_row_group().unwrap();
  while let Some(mut col) = row_group.next_column().unwrap() {
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
    row_group.close_column(col).unwrap();
  }
  writer.close_row_group(row_group).unwrap();
  println!("File is written!");
}
