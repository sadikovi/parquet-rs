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
use basic::{LogicalType, Repetition};
use file::reader::RowGroupReader;
use schema::types::{ColumnPath, SchemaDescPtr, Type, TypePtr};
use record::api::Row;
use record::triplet::TripletIter;

/// Iterator of Rows
pub struct RowIter<'a> {
  root_reader: Reader<'a>,
  records_left: usize
}

impl<'a> RowIter<'a> {
  fn new(mut root_reader: Reader<'a>, num_records: usize) -> Self {
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

// TODO: Add builder pattern and make batch size configurable with default around
// 128/256 records.
const BATCH_SIZE: usize = 4;

/// Reader tree for record assembly
pub enum Reader<'a> {
  PrimitiveReader(TypePtr, TripletIter<'a>),
  OptionReader(i16, Box<Reader<'a>>),
  GroupReader(Option<TypePtr>, i16, Vec<Reader<'a>>),
  RepeatedReader(TypePtr, i16, i16, Box<Reader<'a>>),
  KeyValueReader(TypePtr, i16, i16, Box<Reader<'a>>, Box<Reader<'a>>)
}

impl<'a> Reader<'a> {
  pub fn new(
    descr: SchemaDescPtr,
    row_group_reader: &'a RowGroupReader
  ) -> Self {
    // prepare map of column paths for pruning
    let mut paths: HashMap<ColumnPath, usize> = HashMap::new();
    let row_group_metadata = row_group_reader.metadata();

    for col_index in 0..row_group_reader.num_columns() {
      let col_meta = row_group_metadata.column(col_index);
      // TODO: avoid cloning of column path
      let col_path = col_meta.column_path().clone();
      paths.insert(col_path, col_index);
    }

    let mut readers = Vec::new();
    let mut path = Vec::new();
    for field in descr.root_schema().get_fields() {
      let reader = Self::reader_tree(
        field.clone(), &mut path, 0, 0, &paths, row_group_reader);
      readers.push(reader);
    }

    // Return group reader for message type, we mark it as always required with
    // definition level 0
    Reader::GroupReader(None, 0, readers)
  }

  pub fn row_iter(
    descr: SchemaDescPtr,
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
    paths: &HashMap<ColumnPath, usize>,
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
      let column = TripletIter::new(col_descr, col_reader, BATCH_SIZE);
      Reader::PrimitiveReader(field, column)
    } else {
      match field.get_basic_info().logical_type() {
        // List types
        LogicalType::LIST => {
          assert_eq!(field.get_fields().len(), 1, "Invalid list type {:?}", field);

          let repeated_field = field.get_fields()[0].clone();
          assert_eq!(repeated_field.get_basic_info().repetition(),
            Repetition::REPEATED, "Invalid list type {:?}", field);

          // Support for backward compatible lists
          if Self::is_element_type(&repeated_field) {
            let reader = Self::reader_tree(repeated_field.clone(), &mut path,
              curr_def_level, curr_rep_level, paths, row_group_reader);

            Reader::RepeatedReader(
              field, curr_def_level, curr_rep_level, Box::new(reader))
          } else {
            let child_field = repeated_field.get_fields()[0].clone();

            path.push(String::from(repeated_field.name()));

            let reader = Self::reader_tree(child_field, &mut path,
              curr_def_level + 1, curr_rep_level + 1, paths, row_group_reader);

            path.pop();

            Reader::RepeatedReader(
              field, curr_def_level, curr_rep_level, Box::new(reader))
          }
        },
        // Map types (key-value pairs)
        LogicalType::MAP | LogicalType:: MAP_KEY_VALUE => {
          assert_eq!(field.get_fields().len(), 1, "Invalid map type: {:?}", field);
          assert!(!field.get_fields()[0].is_primitive(), "Invalid map type: {:?}", field);

          let key_value_type = field.get_fields()[0].clone();
          assert_eq!(key_value_type.get_basic_info().repetition(), Repetition::REPEATED,
            "Invalid map type: {:?}", field);
          assert_eq!(key_value_type.get_fields().len(), 2,
            "Invalid map type: {:?}", field);

          path.push(String::from(key_value_type.name()));

          let key_type = &key_value_type.get_fields()[0];
          // TODO: not sure if key can only be primitive, check this.
          assert!(key_type.is_primitive(),
            "Map key type is expected to be a primitive type, but found {:?}", key_type);
          let key_reader = Self::reader_tree(key_type.clone(), &mut path,
            curr_def_level + 1, curr_rep_level + 1, paths, row_group_reader);

          let value_type = &key_value_type.get_fields()[1];
          let value_reader = Self::reader_tree(value_type.clone(), &mut path,
            curr_def_level + 1, curr_rep_level + 1, paths, row_group_reader);

          path.pop();

          Reader::KeyValueReader(field, curr_def_level, curr_rep_level,
            Box::new(key_reader), Box::new(value_reader))
        },
        // Group types (structs)
        _ => {
          let mut readers = Vec::new();
          for child in field.get_fields() {
            let reader = Self::reader_tree(child.clone(), &mut path,
              curr_def_level, curr_rep_level, paths, row_group_reader);
            readers.push(reader);
          }
          Reader::GroupReader(Some(field), curr_def_level, readers)
        }
      }
    };
    path.pop();

    Self::option(repetition, curr_def_level, reader)
  }

  // == Value readers API ==

  fn option(repetition: Repetition, def_level: i16, reader: Reader<'a>) -> Self {
    if repetition == Repetition::OPTIONAL {
      Reader::OptionReader(def_level - 1, Box::new(reader))
    } else {
      reader
    }
  }

  /// Returns true if repeated type is an element type for the list
  fn is_element_type(repeated_type: &Type) -> bool {
    // For legacy 2-level list types with primitive element type, e.g.:
    //
    //    // ARRAY<INT> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated int32 element;
    //    }
    //
    repeated_type.is_primitive() ||
    // For legacy 2-level list types whose element type is a group type with 2 or more
    // fields, e.g.:
    //
    //    // ARRAY<STRUCT<str: STRING, num: INT>> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated group element {
    //        required binary str (UTF8);
    //        required int32 num;
    //      };
    //    }
    //
    repeated_type.is_group() && repeated_type.get_fields().len() > 1 ||
    // For legacy 2-level list types generated by parquet-avro (Parquet version < 1.6.0),
    // e.g.:
    //
    //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated group array {
    //        required binary str (UTF8);
    //      };
    //    }
    //
    repeated_type.name() == "array" ||
    // For Parquet data generated by parquet-thrift, e.g.:
    //
    //    // ARRAY<STRUCT<str: STRING>> (nullable list, non-null elements)
    //    optional group my_list (LIST) {
    //      repeated group my_list_tuple {
    //        required binary str (UTF8);
    //      };
    //    }
    //
    repeated_type.name().ends_with("_tuple")
  }

  fn read(&mut self) -> Row {
    match *self {
      Reader::PrimitiveReader(_, ref mut column) => {
        let value = column.current_value();
        column.consume().unwrap();
        value
      },
      Reader::OptionReader(def_level, ref mut reader) => {
        if reader.current_def_level() > def_level {
          reader.read()
        } else {
          reader.advance_columns();
          Row::Null
        }
      },
      Reader::GroupReader(_, def_level, ref mut readers) => {
        let mut fields = Vec::new();
        for reader in readers {
          if reader.repetition() != Repetition::OPTIONAL ||
              reader.current_def_level() > def_level {
            fields.push((String::from(reader.field_name()), reader.read()));
          } else {
            reader.advance_columns();
            fields.push((String::from(reader.field_name()), Row::Null));
          }
        }
        Row::Group(fields)
      },
      Reader::RepeatedReader(_, def_level, rep_level, ref mut reader) => {
        let mut elements = Vec::new();
        loop {
          if reader.current_def_level() > def_level {
            elements.push(reader.read());
          } else {
            reader.advance_columns();
            // If the current definition level is equal to the definition level of this
            // repeated type, then the result is an empty list and the repetition level
            // will always be <= rl.
            break;
          }

          // this covers case when we are out of repetition levels and should close the
          // group, or there are no values left to buffer.
          if !reader.has_next() || reader.current_rep_level() <= rep_level {
            break;
          }
        }
        Row::List(elements)
      },
      Reader::KeyValueReader(_, def_level, rep_level,
          ref mut keys, ref mut values) => {

        let mut pairs = Vec::new();
        loop {
          if keys.current_def_level() > def_level {
            pairs.push((keys.read(), values.read()));
          } else {
            keys.advance_columns();
            values.advance_columns();
            // if the current definition level is equal to the definition level of this
            // repeated type, then the result is an empty list and the repetition level
            // will always be <= rl.
            break;
          }

          // this covers case when we are out of repetition levels and should close the
          // group, or there are no values left to buffer.
          if !keys.has_next() || keys.current_rep_level() <= rep_level {
            break;
          }
        }

        Row::Map(pairs)
      }
    }
  }

  fn field_name(&self) -> &str {
    match *self {
      Reader::PrimitiveReader(ref field, _) => field.name(),
      Reader::OptionReader(_, ref reader) => reader.field_name(),
      Reader::GroupReader(ref opt, _, _) => match opt {
        &Some(ref field) => field.name(),
        &None => panic!("Field is None for group reader"),
      },
      Reader::RepeatedReader(ref field, _, _, _) => field.name(),
      Reader::KeyValueReader(ref field, _, _, _, _) => field.name(),
    }
  }

  fn repetition(&self) -> Repetition {
    match *self {
      Reader::PrimitiveReader(ref field, _) => {
        field.get_basic_info().repetition()
      },
      Reader::OptionReader(_, ref reader) => {
        reader.repetition()
      },
      Reader::GroupReader(ref field, _, _) => {
        assert!(field.is_some(), "Message type does not repetition level");
        field.as_ref().unwrap().get_basic_info().repetition()
      },
      Reader::RepeatedReader(ref field, _, _, _) => {
        field.get_basic_info().repetition()
      },
      Reader::KeyValueReader(ref field, _, _, _, _) => {
        field.get_basic_info().repetition()
      }
    }
  }

  fn has_next(&self) -> bool {
    match *self {
      Reader::PrimitiveReader(_, ref column) => column.has_next(),
      Reader::OptionReader(_, ref reader) => reader.has_next(),
      Reader::GroupReader(_, _, ref readers) => readers.first().unwrap().has_next(),
      Reader::RepeatedReader(_, _, _, ref reader) => reader.has_next(),
      Reader::KeyValueReader(_, _, _, ref keys, _) => keys.has_next(),
    }
  }

  fn current_def_level(&self) -> i16 {
    match *self {
      Reader::PrimitiveReader(_, ref column) => column.current_def_level(),
      Reader::OptionReader(_, ref reader) => reader.current_def_level(),
      Reader::GroupReader(_, _, ref readers) => {
        readers.first().unwrap().current_def_level()
      },
      Reader::RepeatedReader(_, _, _, ref reader) => reader.current_def_level(),
      Reader::KeyValueReader(_, _, _, ref keys, _) => keys.current_def_level(),
    }
  }

  fn current_rep_level(&self) -> i16 {
    match *self {
      Reader::PrimitiveReader(_, ref column) => column.current_rep_level(),
      Reader::OptionReader(_, ref reader) => reader.current_rep_level(),
      Reader::GroupReader(_, _, ref readers) => {
        readers.first().unwrap().current_rep_level()
      },
      Reader::RepeatedReader(_, _, _, ref reader) => reader.current_rep_level(),
      Reader::KeyValueReader(_, _, _, ref keys, _) => keys.current_rep_level(),
    }
  }

  fn advance_columns(&mut self) {
    match *self {
      Reader::PrimitiveReader(_, ref mut column) => {
        column.consume().unwrap();
      },
      Reader::OptionReader(_, ref mut reader) => {
        reader.advance_columns();
      },
      Reader::GroupReader(_, _, ref mut readers) => {
        for reader in readers {
          reader.advance_columns();
        }
      },
      Reader::RepeatedReader(_, _, _, ref mut reader) => {
        reader.advance_columns();
      },
      Reader::KeyValueReader(_, _, _, ref mut keys, ref mut values) => {
        keys.advance_columns();
        values.advance_columns();
      },
    }
  }
}
