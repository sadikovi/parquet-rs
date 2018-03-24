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
use file::reader::{FileReader, RowGroupReader};
use schema::types::{ColumnPath, SchemaDescPtr, Type, TypePtr};
use record::api::Row;
use record::triplet::TripletIter;


/// Default batch size for a reader
const DEFAULT_BATCH_SIZE: usize = 256;

/// Tree builder for `Reader` enum.
/// Serves as a container of options for building a reader tree and a builder.
pub struct TreeBuilder {
  // Batch size (>= 1) for triplet iterators
  batch_size: usize
}

impl TreeBuilder {
  /// Creates new tree builder with default parameters.
  pub fn new() -> Self {
    Self { batch_size: DEFAULT_BATCH_SIZE }
  }

  /// Sets batch size for this tree builder.
  pub fn with_batch_size(mut self, batch_size: usize) -> Self {
    self.batch_size = batch_size;
    self
  }

  /// Creates new root reader for provided schema and row group.
  pub fn build(
    &self,
    descr: SchemaDescPtr,
    row_group_reader: &RowGroupReader
  ) -> Reader {
    // Prepare map of column paths for pruning
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
      let reader = self.reader_tree(
        field.clone(), &mut path, 0, 0, &paths, row_group_reader);
      readers.push(reader);
    }

    // Return group reader for message type,
    // it is always required with definition level 0
    Reader::GroupReader(None, 0, readers)
  }

  /// Creates `Row` iterator directly from schema descriptor and row group.
  pub fn as_row_iter(
    &self,
    descr: SchemaDescPtr,
    row_group_reader: &RowGroupReader
  ) -> RowIter {
    let num_records = row_group_reader.metadata().num_rows() as usize;
    RowIter::new(self.build(descr, row_group_reader), num_records)
  }

  /// Builds tree of readers for the current schema recursively.
  fn reader_tree(
    &self,
    field: TypePtr,
    mut path: &mut Vec<String>,
    mut curr_def_level: i16,
    mut curr_rep_level: i16,
    paths: &HashMap<ColumnPath, usize>,
    row_group_reader: &RowGroupReader
  ) -> Reader {
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
      let column = TripletIter::new(col_descr, col_reader, self.batch_size);
      Reader::PrimitiveReader(field, column)
    } else {
      match field.get_basic_info().logical_type() {
        // List types
        LogicalType::LIST => {
          assert_eq!(field.get_fields().len(), 1, "Invalid list type {:?}", field);

          let repeated_field = field.get_fields()[0].clone();
          assert_eq!(repeated_field.get_basic_info().repetition(),
            Repetition::REPEATED, "Invalid list type {:?}", field);

          if Reader::is_element_type(&repeated_field) {
            // Support for backward compatible lists
            let reader = self.reader_tree(repeated_field.clone(), &mut path,
              curr_def_level, curr_rep_level, paths, row_group_reader);

            Reader::RepeatedReader(
              field, curr_def_level, curr_rep_level, Box::new(reader))
          } else {
            let child_field = repeated_field.get_fields()[0].clone();

            path.push(String::from(repeated_field.name()));

            let reader = self.reader_tree(child_field, &mut path,
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
          assert!(key_type.is_primitive(),
            "Map key type is expected to be a primitive type, but found {:?}", key_type);
          let key_reader = self.reader_tree(key_type.clone(), &mut path,
            curr_def_level + 1, curr_rep_level + 1, paths, row_group_reader);

          let value_type = &key_value_type.get_fields()[1];
          let value_reader = self.reader_tree(value_type.clone(), &mut path,
            curr_def_level + 1, curr_rep_level + 1, paths, row_group_reader);

          path.pop();

          Reader::KeyValueReader(field, curr_def_level, curr_rep_level,
            Box::new(key_reader), Box::new(value_reader))
        },
        // Group types (structs)
        _ => {
          let mut readers = Vec::new();
          for child in field.get_fields() {
            let reader = self.reader_tree(child.clone(), &mut path,
              curr_def_level, curr_rep_level, paths, row_group_reader);
            readers.push(reader);
          }
          Reader::GroupReader(Some(field), curr_def_level, readers)
        }
      }
    };
    path.pop();

    Reader::option(repetition, curr_def_level, reader)
  }
}

/// Reader tree for record assembly
pub enum Reader {
  // Primitive reader with type information and triplet iterator
  PrimitiveReader(TypePtr, TripletIter),
  // Optional reader with definition level of a parent and a reader
  OptionReader(i16, Box<Reader>),
  // Group (struct) reader with type information, definition level and list of child
  // readers. When it represents message type, type information is None
  GroupReader(Option<TypePtr>, i16, Vec<Reader>),
  // Reader for repeated values, e.g. lists, contains type information, definition level,
  // repetition level and a child reader
  RepeatedReader(TypePtr, i16, i16, Box<Reader>),
  // Reader of key-value pairs, e.g. maps, contains type information, definition level,
  // repetition level, child reader for keys and child reader for values
  KeyValueReader(TypePtr, i16, i16, Box<Reader>, Box<Reader>)
}

impl Reader {
  /// Wraps reader in option reader based on repetition.
  fn option(repetition: Repetition, def_level: i16, reader: Reader) -> Self {
    if repetition == Repetition::OPTIONAL {
      Reader::OptionReader(def_level - 1, Box::new(reader))
    } else {
      reader
    }
  }

  /// Returns true if repeated type is an element type for the list.
  /// Used to determine legacy list types.
  /// This method is copied from Spark Parquet reader.
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

  /// Reads current record as `Row` from the reader tree.
  /// Automatically advances all necessary readers.
  fn read(&mut self) -> Row {
    match *self {
      Reader::PrimitiveReader(_, ref mut column) => {
        let value = column.current_value();
        column.read_next().unwrap();
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

          // This covers case when we are out of repetition levels and should close the
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
            // If the current definition level is equal to the definition level of this
            // repeated type, then the result is an empty list and the repetition level
            // will always be <= rl.
            break;
          }

          // This covers case when we are out of repetition levels and should close the
          // group, or there are no values left to buffer.
          if !keys.has_next() || keys.current_rep_level() <= rep_level {
            break;
          }
        }

        Row::Map(pairs)
      }
    }
  }

  /// Returns field name for the current reader.
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

  /// Returns repetition for the current reader.
  fn repetition(&self) -> Repetition {
    match *self {
      Reader::PrimitiveReader(ref field, _) => {
        field.get_basic_info().repetition()
      },
      Reader::OptionReader(_, ref reader) => {
        reader.repetition()
      },
      Reader::GroupReader(ref opt, _, _) => match opt {
        &Some(ref field) => field.get_basic_info().repetition(),
        &None => panic!("Field is None for group reader"),
      },
      Reader::RepeatedReader(ref field, _, _, _) => {
        field.get_basic_info().repetition()
      },
      Reader::KeyValueReader(ref field, _, _, _, _) => {
        field.get_basic_info().repetition()
      }
    }
  }

  /// Returns true, if current reader has more values, false otherwise.
  /// Method does not advance internal iterator.
  fn has_next(&self) -> bool {
    match *self {
      Reader::PrimitiveReader(_, ref column) => column.has_next(),
      Reader::OptionReader(_, ref reader) => reader.has_next(),
      Reader::GroupReader(_, _, ref readers) => readers.first().unwrap().has_next(),
      Reader::RepeatedReader(_, _, _, ref reader) => reader.has_next(),
      Reader::KeyValueReader(_, _, _, ref keys, _) => keys.has_next(),
    }
  }

  /// Returns current definition level,
  /// Method does not advance internal iterator.
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

  /// Returns current repetition level.
  /// Method does not advance internal iterator.
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

  /// Advances leaf columns for the current reader.
  fn advance_columns(&mut self) {
    match *self {
      Reader::PrimitiveReader(_, ref mut column) => {
        column.read_next().unwrap();
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

// ----------------------------------------------------------------------
// Row iterators

/// Iterator of `Row`s over all row groups in file reader.
pub struct FileRowIter<'a> {
  descr: SchemaDescPtr,
  file_reader: &'a FileReader,
  current_row_group: usize,
  num_row_groups: usize,
  row_iter: Option<RowIter>
}

impl<'a> FileRowIter<'a> {
  pub fn new(descr: SchemaDescPtr, file_reader: &'a FileReader) -> Self {
    let num_row_groups = file_reader.num_row_groups();

    Self {
      descr: descr,
      file_reader: file_reader,
      current_row_group: 0,
      num_row_groups: num_row_groups,
      row_iter: None
    }
  }
}

impl<'a> Iterator for FileRowIter<'a> {
  type Item = Row;

  fn next(&mut self) -> Option<Row> {
    let mut row = None;
    if let Some(ref mut iter) = self.row_iter {
      row = iter.next();
    }

    while row.is_none() && self.current_row_group < self.num_row_groups {
      // We do not expect any failures when accessing a row group
      let row_group_reader =
        self.file_reader.get_row_group(self.current_row_group).unwrap();
      self.current_row_group += 1;
      let mut iter = row_group_reader.get_row_iter(self.descr.clone());
      row = iter.next();
      self.row_iter = Some(iter);
    }

    row
  }
}

/// Iterator of `Row`s.
pub struct RowIter {
  root_reader: Reader,
  records_left: usize
}

impl RowIter {
  fn new(mut root_reader: Reader, num_records: usize) -> Self {
    // Prepare root reader by advancing all column vectors
    root_reader.advance_columns();
    Self { root_reader: root_reader, records_left: num_records }
  }
}

impl Iterator for RowIter {
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
