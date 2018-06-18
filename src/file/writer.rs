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

//! Contains file writer API.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::rc::Rc;

use errors::Result;
use file::PARQUET_MAGIC;
use file::properties::WriterProperties;
use schema::types::{SchemaDescriptor, Type};

// ----------------------------------------------------------------------
// APIs for file & row group writers

pub trait FileWriter {
  /// Initialises the file, writes Parquet magic.
  fn init(&mut self) -> Result<()>;

  /// Appends the next row group.
  /// Method should also perform all necessary metadata update.
  fn append_row_group(&mut self, writer: Box<RowGroupWriter>) -> Result<()>;

  /// Closes the file.
  /// Metadata and Parquet magic should be written in this step.
  fn close(&mut self) -> Result<()>;
}

// ----------------------------------------------------------------------
// Implementation for file writer

pub struct SerializedFileWriter {
  buf: BufWriter<File>,
  descr: SchemaDescriptor,
  properties: WriterProperties
}

impl SerializedFileWriter {
  pub fn new(file: File, schema: Type, props: WriterProperties) -> Self {
    Self {
      buf: BufWriter::new(file),
      descr: SchemaDescriptor::new(Rc::new(schema)),
      properties: props
    }
  }
}

impl FileWriter for SerializedFileWriter {
  fn init(&mut self) -> Result<()> {
    self.buf.write(&PARQUET_MAGIC)?;
    Ok(())
  }

  fn append_row_group(&mut self, _writer: Box<RowGroupWriter>) -> Result<()> {
    unimplemented!();
  }

  fn close(&mut self) -> Result<()> {
    unimplemented!();
  }
}

pub trait RowGroupWriter {
  /// Total number of rows that will be written by this row group writer.
  fn num_rows(&self) -> usize;

  /// Number of columns in this row group.
  fn num_columns(&self) -> usize;

  /// Returns column writer for the next column to write.
  /// Columns are written directly to disk, so there is no way of modifying existing
  /// column after requesting next column writer.
  fn next_column(&mut self) -> Result<Box<ColumnWriter>>;

  fn current_column_idx(&self) -> usize;

  /// Closes the row group.
  fn close(&mut self) -> Result<()>;
}

pub trait ColumnWriter {
  fn write_dictionary_page(&mut self) -> Result<()>;

  fn check_dictionary_size_limit(&self);

  fn add_data_page(&mut self) -> Result<()>;

  fn write_definition_levels(&mut self, num_levels: usize, levels: &[u16]) -> Result<()>;

  fn write_repetition_levels(&mut self, num_levels: usize, levels: &[u16]) -> Result<()>;

  fn flush_buffered_data_pages(&mut self) -> Result<()>;

  fn close(&mut self) -> Result<()>;
}
