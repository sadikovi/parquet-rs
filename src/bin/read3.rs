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

use parquet::record::vector::ColumnVector;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::print_file_metadata;

fn main() {
  let path = Path::new("data/alltypes_plain.snappy.parquet");
  let file = File::open(&path).unwrap();
  let parquet_reader = SerializedFileReader::new(file).unwrap();
  let metadata = parquet_reader.metadata();

  print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());

  // CHANGE_ME: number of records requested, try changing it to return fewer or more records
  // note that table contains only 4 records, therefore 5+ records will be with default struct values.
  let batch_size = 8;
  println!("Reading batch of records: {}", batch_size);

  for i in 0..metadata.num_row_groups() {
    let row_group_reader = parquet_reader.get_row_group(i).unwrap();
    let row_group_metadata = metadata.row_group(i);

    println!("* Found {} columns", row_group_metadata.num_columns());

    for j in 0..row_group_metadata.num_columns() {
      let column = row_group_metadata.column(j);
      let column_reader = row_group_reader.get_column_reader(j).unwrap();
      let column_descr = column.column_descr_ptr();
      let column_path = &column.column_path().string()[..];
      let column_type = column.column_type();

      println!("- Reading column '{}' of type {}", column_path, column_type);

      let mut vector = ColumnVector::new(column_descr, column_reader, batch_size);
      while vector.consume().unwrap() {
        vector.print_test_value();
      }
    }
  }
}
