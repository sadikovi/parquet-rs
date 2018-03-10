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
use basic::Repetition;
use file::reader::RowGroupReader;
use schema::types::{ColumnPath, SchemaDescriptor, Type};
use record::vector::ColumnVector;

pub enum ValueReader<'a> {
  PrimitiveReader(ColumnVector<'a>),
  OptionReader(i16, Box<ValueReader<'a>>),
  GroupReader(Repetition, i16, Vec<ValueReader<'a>>)
}

impl<'a> ValueReader<'a> {
  pub fn new(
    descr: &'a SchemaDescriptor,
    row_group_reader: &'a RowGroupReader
  ) -> Self {
    // prepare map of column paths for pruning
    let mut paths: HashMap<&ColumnPath, usize> = HashMap::new();
    for col_index in 0..row_group_reader.num_columns() {
      let col_meta = row_group_reader.metadata().column(col_index);
      let col_path = col_meta.column_path();
      paths.insert(col_path, col_index);
    }

    let mut readers = Vec::new();
    let mut path = Vec::new();
    for field in descr.root_schema().get_fields() {
      let reader = Self::reader_tree(field, &mut path, 0, 0, &paths, row_group_reader);
      readers.push(reader);
    }

    // Message type is always required with 0 definition level
    ValueReader::GroupReader(Repetition::REQUIRED, 0, readers)
  }

  fn reader_tree(
    field: &Type,
    mut path: &mut Vec<String>,
    mut curr_def_level: i16,
    mut curr_rep_level: i16,
    paths: &HashMap<&ColumnPath, usize>,
    row_group_reader: &'a RowGroupReader
  ) -> Self {
    assert!(field.get_basic_info().has_repetition());
    // Update current definition and repetition levels for this type
    let repetition = field.get_basic_info().repetition();
    match repetition {
      Repetition::OPTIONAL => {
        curr_def_level += 1;
      },
      Repetition::REPEATED => {
        curr_def_level += 1;
        curr_rep_level += 1;
      },
      _ => { }
    }

    path.push(String::from(field.name()));
    let reader = if field.is_primitive() {
      let col_path = ColumnPath::new(path.to_vec());
      let orig_index = *paths.get(&col_path).unwrap();
      let col_descr = row_group_reader.metadata().column(orig_index).column_descr_ptr();
      let col_reader = row_group_reader.get_column_reader(orig_index).unwrap();
      let col_vector = ColumnVector::new(col_descr, col_reader, 4);
      ValueReader::PrimitiveReader(col_vector)
    } else {
      // TODO: distinguish between repeated fields and struct fields
      let mut readers = Vec::new();
      for child in field.get_fields() {
        let reader = Self::reader_tree(child, &mut path,
          curr_def_level, curr_rep_level, paths, row_group_reader);
        readers.push(reader);
      }
      ValueReader::GroupReader(repetition, curr_def_level, readers)
    };
    path.pop();

    Self::option(repetition, curr_def_level, reader)
  }

  // == Value readers API ==

  fn option(repetition: Repetition, def_level: i16, reader: ValueReader<'a>) -> Self {
    if repetition == Repetition::OPTIONAL {
      ValueReader::OptionReader(def_level - 1, Box::new(reader))
    } else {
      reader
    }
  }

  fn read(&mut self) {
    match *self {
      ValueReader::PrimitiveReader(ref mut column) => {
        column.print_test_value();
        column.consume().unwrap();
      },
      ValueReader::OptionReader(def_level, ref mut reader) => {
        if reader.column_def_level() > def_level {
          reader.read();
        } else {
          reader.advance_columns();
        }
      },
      ValueReader::GroupReader(repetition, def_level, ref mut readers) => {
        for reader in readers {
          if repetition != Repetition::OPTIONAL || reader.column_def_level() > def_level {
            reader.read();
          } else {
            reader.advance_columns();
          }
        }
      }
    }
  }

  fn column_def_level(&self) -> i16 {
    match *self {
      ValueReader::PrimitiveReader(ref column) => column.current_def_level(),
      ValueReader::OptionReader(_, ref reader) => reader.column_def_level(),
      ValueReader::GroupReader(_, _, ref readers) => {
        readers.first().unwrap().column_def_level()
      },
    }
  }

  fn advance_columns(&mut self) {
    match *self {
      ValueReader::PrimitiveReader(ref mut column) => {
        column.consume().unwrap();
      },
      ValueReader::OptionReader(_, ref mut reader) => {
        reader.advance_columns();
      },
      ValueReader::GroupReader(_, _, ref mut readers) => {
        for reader in readers {
          reader.advance_columns();
        }
      }
    }
  }

  /// Reads records using current reader's record materializer.
  /// Reads at most `num_records` rows.
  pub fn read_records(&mut self, num_records: usize) {
    self.advance_columns();
    for i in 0..num_records {
      println!("--- record {} ---", i);
      self.read();
    }
  }
}
