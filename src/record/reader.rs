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

use record::api::RecordMaterializer;
use record::vector::ColumnVector;
use schema::types::SchemaDescriptor;

/// Record reader that is initialized for a row group.
pub struct RecordReader<'a> {
  proj_descr: &'a SchemaDescriptor,
  record_mat: &'a mut Box<RecordMaterializer>,
  column_vectors: Vec<ColumnVector<'a>>
}

impl<'a> RecordReader<'a> {

  /// Creates new record reader (usually per row group).
  /// Requires projected schema, record materializer to use and column vectors.
  /// Column vectors should match leaf nodes in the projected schema.
  pub fn new(
    proj_descr: &'a SchemaDescriptor,
    record_mat: &'a mut Box<RecordMaterializer>,
    column_vectors: Vec<ColumnVector<'a>>
  ) -> Self {
    // Make sure that number of leaves matches number of column vectors
    assert_eq!(
      proj_descr.num_columns(),
      column_vectors.len(),
      "Number of columns mismatch, expected: {}, found: {}",
        proj_descr.num_columns(), column_vectors.len()
    );

    Self {
      proj_descr: proj_descr,
      record_mat: record_mat,
      column_vectors: column_vectors
    }
  }

  /// Reads records using current reader's record materializer.
  /// Reads at most `num_records` rows.
  pub fn read_records(&mut self, num_records: usize) {
    for i in 0..num_records {
      println!("  + reading row {}, num column readers: {}", i, self.column_vectors.len());
      self.read_record();
    }
  }

  fn consume(&mut self) {
    // consume values for all columns
    for col in &mut self.column_vectors[..] {
      let has_next = col.consume().unwrap();
      assert!(has_next, "Premature end of column vector");
    }
  }

  fn read_record(&mut self) {
    self.consume();
    self.record_mat.root_converter().start();
    let cols = &self.column_vectors[..];
    for col in cols {
      println!("    * traverse column, def level: {}", col.current_def_level());
    }
    self.record_mat.root_converter().end();
    self.record_mat.consume_current_record();
  }
}
