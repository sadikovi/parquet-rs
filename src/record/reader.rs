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
use schema::types::{ColumnPath, SchemaDescriptor, TypePtr};
use record::api::Row;
use record::vector::ColumnVector;

/// Iterator of Rows
pub struct RowIter<'a> {
  root_reader: ValueReader<'a>,
  records_left: usize
}

impl<'a> RowIter<'a> {
  fn new(mut root_reader: ValueReader<'a>, num_records: usize) -> Self {
    // Prepare root reader by advancing all column vectors
    root_reader.advance_columns();
    Self { root_reader: root_reader, records_left: num_records }
  }
}

impl<'a> Iterator for RowIter<'a> {
  type Item = Row;

  fn next(&mut self) -> Option<Row> {
    if self.records_left > 0 {
      self.records_left -= 1;
      Some(self.root_reader.read())
    } else {
      None
    }
  }
}

pub enum ValueReader<'a> {
  PrimitiveReader(TypePtr, ColumnVector<'a>),
  OptionReader(i16, Box<ValueReader<'a>>),
  GroupReader(Option<TypePtr>, Repetition, i16, Vec<ValueReader<'a>>)
}

impl<'a> ValueReader<'a> {
  pub fn new(
    descr: SchemaDescriptor,
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
      let reader = Self::reader_tree(
        field.clone(), &mut path, 0, 0, &paths, row_group_reader);
      readers.push(reader);
    }

    ValueReader::GroupReader(None, Repetition::REQUIRED, 0, readers)
  }

  pub fn row_iter(
    descr: SchemaDescriptor,
    row_group_reader: &'a RowGroupReader
  ) -> RowIter<'a> {
    let num_records = row_group_reader.metadata().num_rows() as usize;
    RowIter::new(Self::new(descr, row_group_reader), num_records)
  }

  fn reader_tree(
    field: TypePtr,
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

    // TODO: add support for repeated fields, e.g. list or map
    assert!(repetition != Repetition::REPEATED, "REPEATED fields are not supported");

    path.push(String::from(field.name()));
    let reader = if field.is_primitive() {
      let col_path = ColumnPath::new(path.to_vec());
      let orig_index = *paths.get(&col_path).unwrap();
      let col_descr = row_group_reader.metadata().column(orig_index).column_descr_ptr();
      let col_reader = row_group_reader.get_column_reader(orig_index).unwrap();
      let col_vector = ColumnVector::new(col_descr, col_reader, 4);
      ValueReader::PrimitiveReader(field, col_vector)
    } else {
      let mut readers = Vec::new();
      for child in field.get_fields() {
        let reader = Self::reader_tree(child.clone(), &mut path,
          curr_def_level, curr_rep_level, paths, row_group_reader);
        readers.push(reader);
      }
      ValueReader::GroupReader(Some(field), repetition, curr_def_level, readers)
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

  fn read(&mut self) -> Row {
    match *self {
      ValueReader::PrimitiveReader(_, ref mut column) => {
        let value = column.current_value();
        column.consume().unwrap();
        value
      },
      ValueReader::OptionReader(def_level, ref mut reader) => {
        if reader.current_def_level() > def_level {
          reader.read()
        } else {
          reader.advance_columns();
          Row::Null
        }
      },
      ValueReader::GroupReader(_, repetition, def_level, ref mut readers) => {
        let mut fields = HashMap::new();
        for reader in readers {
          if repetition != Repetition::OPTIONAL || reader.current_def_level() > def_level {
            fields.insert(String::from(reader.field_name()), reader.read());
          } else {
            reader.advance_columns();
            fields.insert(String::from(reader.field_name()), Row::Null);
          }
        }
        Row::Group(fields)
      },
    }
  }

  fn field_name(&self) -> &str {
    match *self {
      ValueReader::PrimitiveReader(ref field, _) => field.name(),
      ValueReader::OptionReader(_, ref reader) => reader.field_name(),
      ValueReader::GroupReader(ref opt, _, _, _) => match opt {
        &Some(ref field) => field.name(),
        &None => panic!("Field is None for group reader"),
      }
    }
  }

  fn current_def_level(&self) -> i16 {
    match *self {
      ValueReader::PrimitiveReader(_, ref column) => column.current_def_level(),
      ValueReader::OptionReader(_, ref reader) => reader.current_def_level(),
      ValueReader::GroupReader(_, _, _, ref readers) => {
        readers.first().unwrap().current_def_level()
      },
    }
  }

  fn advance_columns(&mut self) {
    match *self {
      ValueReader::PrimitiveReader(_, ref mut column) => {
        column.consume().unwrap();
      },
      ValueReader::OptionReader(_, ref mut reader) => {
        reader.advance_columns();
      },
      ValueReader::GroupReader(_, _, _, ref mut readers) => {
        for reader in readers {
          reader.advance_columns();
        }
      },
    }
  }
}
