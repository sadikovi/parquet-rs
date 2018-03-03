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

use parquet::basic::*;
use parquet::data_type::*;
use parquet::column::reader::{ColumnReaderImpl, get_typed_column_reader};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::print_file_metadata;

// Run example
// ===
// $ cargo run --bin example-read
//
// Console output
// ===
// Reading batch of records: 4
// Record { b: 2, d_c: "abc", e: [1, 1] }
// Record { b: 3, d_c: "def", e: [10, 10] }
// Record { b: 4, d_c: "ghi", e: [100, 100] }
// Record { b: 4, d_c: "jkl", e: [1000, 1000] }
//

#[derive(Clone, Debug, Default)]
struct Record { b: i32, d_c: String, e: Vec<i32> }

// read values from column reader for requested num records, can read fewer records
// values_to_read can be smaller than total number of values, e.g. batch size
fn read_values<T: DataType>(
    reader: &mut ColumnReaderImpl<T>,
    values_to_read: usize) -> (usize, Vec<T::T>, Vec<i16>, Vec<i16>) where T: 'static {

  let mut values = vec![T::T::default(); values_to_read];
  let mut def_levels = vec![i16::default(); values_to_read];
  let mut rep_levels = vec![i16::default(); values_to_read];
  let mut curr_values_read = 0;
  let mut curr_levels_read = 0;
  loop {
    let (values_read, levels_read) = reader.read_batch(
      values_to_read,
      Some(&mut def_levels[curr_levels_read..]),
      Some(&mut rep_levels[curr_levels_read..]),
      &mut values[curr_values_read..]
    ).unwrap();

    println!("- Read batch, values_read: {}, levels_read: {}", values_read, levels_read);

    curr_values_read += values_read;
    curr_levels_read += levels_read;

    if values_read == 0 || curr_values_read == values_to_read {
      break;
    }
  }
  (curr_values_read, values, def_levels, rep_levels)
}

fn main() {
  // let path = Path::new("data/parquet-v1.snappy.parquet");
  // let path = Path::new("data/parquet-v2.snappy.parquet");
  // let path = Path::new("data/parquet-v2.gz.parquet");
  let path = Path::new("data/sample2.snappy.parquet");
  let file = File::open(&path).unwrap();
  let parquet_reader = SerializedFileReader::new(file).unwrap();
  let metadata = parquet_reader.metadata();

  print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());

  // CHANGE_ME: number of records requested, try changing it to return fewer or more records
  // note that table contains only 4 records, therefore 5+ records will be with default struct values.
  let batch_size = 3;
  println!("Reading batch of records: {}", batch_size);

  for i in 0..metadata.num_row_groups() {
    let row_group_reader = parquet_reader.get_row_group(i).unwrap();
    let row_group_metadata = metadata.row_group(i);

    println!("* Found {} columns", row_group_metadata.num_columns());

    for j in 0..row_group_metadata.num_columns() {
      let column = row_group_metadata.column(j);
      let column_reader = row_group_reader.get_column_reader(j).unwrap();

      println!("* max def level: {}, max rep level: {}",
        column.column_descr().max_def_level(), column.column_descr().max_rep_level());

      match (column.column_type(), &column.column_path().string()[..]) {
        (Type::INT32, "a") => {
          // field is UTF8, however, it is better to expliclty check logical type
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "b") => {
          // field is UTF8, however, it is better to expliclty check logical type
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "b._1") => {
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "b._2") => {
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "c.list.element") => {
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "e.list.element") => {
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        (Type::INT32, "b_struct.b_c_int") => {
          let mut typed_reader = get_typed_column_reader::<Int32Type>(column_reader);
          let (values_read, values, def, rep) = read_values(&mut typed_reader, batch_size);
          println!("Read values: {}, values: {:?}, def: {:?}, rep: {:?}", values_read, values, def, rep);
        },
        _ => { }, // skip column
      }
    }
  }
}
