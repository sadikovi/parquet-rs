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

//! Reader and writer properties.

use std::collections::HashMap;

use basic::{Compression, Encoding};
use schema::types::ColumnPath;

// TODO: Add reader properties.

/// Parquet writer version.
///
/// Basic constant, which is not part of the Thrift definition.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriterVersion {
  PARQUET_1_0,
  PARQUET_2_0
}

const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
const DEFAULT_WRITE_BATCH_SIZE: usize = 1024;
const DEFAULT_WRITER_VERSION: WriterVersion = WriterVersion::PARQUET_1_0;
const DEFAULT_ENCODING: Encoding = Encoding::PLAIN;
const DEFAULT_COMPRESSION: Compression = Compression::UNCOMPRESSED;
const DEFAULT_DICTIONARY_ENABLED: bool = true;
const DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT: usize = DEFAULT_PAGE_SIZE;
const DEFAULT_STATISTICS_ENABLED: bool = true;
const DEFAULT_MAX_STATISTICS_SIZE: usize = 4096;
const DEFAULT_MAX_ROW_GROUP_LENGTH: usize = 64 * 1024 * 1024;
// TODO: Generate default created by string
const DEFAULT_CREATED_BY: &str = "parquet-rs version x.y.z (build ABC)";

/// Writer properties.
///
/// It is created as an immutable data structure, use [`WriterPropertiesBuilder`] to
/// assemble the properties.
#[derive(Debug, Clone)]
pub struct WriterProperties {
  dictionary_pagesize_limit: usize,
  write_batch_size: usize,
  max_row_group_length: usize,
  writer_version: WriterVersion,
  created_by: String,
  default_column_properties: ColumnProperties,
  column_properties: HashMap<ColumnPath, ColumnProperties>
}

impl WriterProperties {
  /// Returns builder for writer properties.
  pub fn builder() -> WriterPropertiesBuilder {
    WriterPropertiesBuilder::with_defaults()
  }

  /// Returns dictionary page size limit.
  pub fn dictionary_pagesize_limit(&self) -> usize {
    self.dictionary_pagesize_limit
  }

  /// Returns configured batch size.
  pub fn write_batch_size(&self) -> usize {
    self.write_batch_size
  }

  /// Returns max size for a row group.
  pub fn max_row_group_length(&self) -> usize {
    self.max_row_group_length
  }

  /// Returns configured writer version.
  pub fn writer_version(&self) -> WriterVersion {
    self.writer_version
  }

  /// Returns encoding for a data page, when dictionary encoding is enabled.
  #[inline]
  pub fn data_page_dictionary_encoding(&self) -> Encoding {
    match self.writer_version {
      WriterVersion::PARQUET_1_0 => Encoding::PLAIN_DICTIONARY,
      WriterVersion::PARQUET_2_0 => Encoding::RLE_DICTIONARY
    }
  }

  /// Returns encoding for dictionary page, when dictionary encoding is enabled.
  #[inline]
  pub fn dictionary_page_encoding(&self) -> Encoding {
    match self.writer_version {
      WriterVersion::PARQUET_1_0 => Encoding::PLAIN_DICTIONARY,
      WriterVersion::PARQUET_2_0 => Encoding::PLAIN
    }
  }

  /// Returns (fallback) encoding for a column.
  pub fn encoding(&self, col: &ColumnPath) -> Encoding {
    match self.column_properties.get(col) {
      Some(props) if props.encoding().is_some() => props.encoding().unwrap(),
      _ => self.default_column_properties.encoding().unwrap_or(DEFAULT_ENCODING)
    }
  }

  /// Returns compression codec for a column.
  pub fn compression(&self, col: &ColumnPath) -> Compression {
    match self.column_properties.get(col) {
      Some(props) if props.compression().is_some() => props.compression().unwrap(),
      _ => self.default_column_properties.compression().unwrap_or(DEFAULT_COMPRESSION)
    }
  }

  /// Returns `true` if dictionary encoding is enabled for a column.
  pub fn dictionary_enabled(&self, col: &ColumnPath) -> bool {
    match self.column_properties.get(col) {
      Some(props) if props.dictionary_enabled().is_some() => {
        props.dictionary_enabled().unwrap()
      },
      _ => {
        self.default_column_properties
          .dictionary_enabled().unwrap_or(DEFAULT_DICTIONARY_ENABLED)
      }
    }
  }

  /// Returns `true` if statistics are enabled for a column.
  pub fn statistics_enabled(&self, col: &ColumnPath) -> bool {
    match self.column_properties.get(col) {
      Some(props) if props.statistics_enabled().is_some() => {
        props.statistics_enabled().unwrap()
      },
      _ => {
        self.default_column_properties
          .statistics_enabled().unwrap_or(DEFAULT_STATISTICS_ENABLED)
      }
    }
  }

  /// Returns max size for statistics.
  /// Only applicable if statistics are enabled.
  pub fn max_statistics_size(&self, col: &ColumnPath) -> usize {
    match self.column_properties.get(col) {
      Some(props) if props.max_statistics_size().is_some() => {
        props.max_statistics_size().unwrap()
      },
      _ => {
        self.default_column_properties
          .max_statistics_size().unwrap_or(DEFAULT_MAX_STATISTICS_SIZE)
      }
    }
  }
}

/// Container for column properties that can be changed as part of writer.
///
/// If a field is `None`, that means that no specific value has been set for this column,
/// so some default value must be used.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProperties {
  encoding: Option<Encoding>,
  codec: Option<Compression>,
  dictionary_enabled: Option<bool>,
  statistics_enabled: Option<bool>,
  max_statistics_size: Option<usize>
}

impl ColumnProperties {
  /// Initialise column properties with default values.
  fn new() -> Self {
    Self {
      encoding: None,
      codec: None,
      dictionary_enabled: None,
      statistics_enabled: None,
      max_statistics_size: None
    }
  }

  /// Sets encoding for this column.
  ///
  /// This field is treated as fallback encoding, and should not be dictionary encoding.
  /// For that there is an additional boolean flag provided.
  fn set_encoding(&mut self, value: Encoding) {
    if value == Encoding::PLAIN_DICTIONARY || value == Encoding::RLE_DICTIONARY {
      panic!("Dictionary encoding can not be used as fallback encoding");
    }
    self.encoding = Some(value);
  }

  /// Sets compression codec for this column.
  fn set_compression(&mut self, value: Compression) {
    self.codec = Some(value);
  }

  /// Sets whether or not dictionary encoding is enabled for this column.
  fn set_dictionary_enabled(&mut self, enabled: bool) {
    self.dictionary_enabled = Some(enabled);
  }

  /// Sets whether or not statistics are enabled for this column.
  fn set_statistics_enabled(&mut self, enabled: bool) {
    self.statistics_enabled = Some(enabled);
  }

  /// Sets max size for statistics for this column.
  fn set_max_statistics_size(&mut self, value: usize) {
    self.max_statistics_size = Some(value);
  }

  /// Returns optional encoding for this column.
  pub fn encoding(&self) -> Option<Encoding> {
    self.encoding
  }

  /// Returns optional compression codec for this column.
  pub fn compression(&self) -> Option<Compression> {
    self.codec
  }

  /// Returns `Some(true)` if dictionary encoding is enabled for this column, if disabled
  /// then returns `Some(false)`. If result is `None`, then no setting has been provided.
  pub fn dictionary_enabled(&self) -> Option<bool> {
    self.dictionary_enabled
  }

  /// Returns `Some(true)` if statistics are enabled for this column, if disabled then
  /// returns `Some(false)`. If result is `None`, then no setting has been provided.
  pub fn statistics_enabled(&self) -> Option<bool> {
    self.statistics_enabled
  }

  /// Returns optional max size in bytes for statistics.
  pub fn max_statistics_size(&self) -> Option<usize> {
    self.max_statistics_size
  }
}

/// Writer properties builder.
pub struct WriterPropertiesBuilder {
  dictionary_pagesize_limit: usize,
  write_batch_size: usize,
  max_row_group_length: usize,
  writer_version: WriterVersion,
  created_by: String,
  default_column_properties: ColumnProperties,
  column_properties: HashMap<ColumnPath, ColumnProperties>
}

impl WriterPropertiesBuilder {
  /// Returns default state of the builder.
  fn with_defaults() -> Self {
    Self {
      dictionary_pagesize_limit: DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT,
      write_batch_size: DEFAULT_WRITE_BATCH_SIZE,
      max_row_group_length: DEFAULT_MAX_ROW_GROUP_LENGTH,
      writer_version: DEFAULT_WRITER_VERSION,
      created_by: DEFAULT_CREATED_BY.to_string(),
      default_column_properties: ColumnProperties::new(),
      column_properties: HashMap::new()
    }
  }

  /// Finalizes the configuration and returns immutable writer properties struct.
  pub fn build(self) -> WriterProperties {
    WriterProperties {
      dictionary_pagesize_limit: self.dictionary_pagesize_limit,
      write_batch_size: self.write_batch_size,
      max_row_group_length: self.max_row_group_length,
      writer_version: self.writer_version,
      created_by: self.created_by,
      default_column_properties: self.default_column_properties,
      column_properties: self.column_properties
    }
  }

  // ----------------------------------------------------------------------
  // Writer properies related to a file

  /// Sets writer version.
  pub fn set_writer_version(mut self, value: WriterVersion) -> Self {
    self.writer_version = value;
    self
  }

  /// Sets dictionary page size limit.
  pub fn set_dictionary_pagesize_limit(mut self, value: usize) -> Self {
    self.dictionary_pagesize_limit = value;
    self
  }

  /// Sets write batch size.
  pub fn set_write_batch_size(mut self, value: usize) -> Self {
    self.write_batch_size = value;
    self
  }

  /// Sets max size for a row group.
  pub fn set_max_row_group_length(mut self, value: usize) -> Self {
    self.max_row_group_length = value;
    self
  }

  /// Sets "created by" property.
  pub fn set_created_by(mut self, value: String) -> Self {
    self.created_by = value;
    self
  }

  // ----------------------------------------------------------------------
  // Setters for any column (global)

  /// Sets (fallback) encoding for any column.
  pub fn set_encoding(mut self, value: Encoding) -> Self {
    self.default_column_properties.set_encoding(value);
    self
  }

  /// Sets compression codec encoding for any column.
  pub fn set_compression(mut self, value: Compression) -> Self {
    self.default_column_properties.set_compression(value);
    self
  }

  /// Sets flag to enable/disable dictionary encoding for any column.
  pub fn set_dictionary_enabled(mut self, value: bool) -> Self {
    self.default_column_properties.set_dictionary_enabled(value);
    self
  }

  /// Sets flag to enable/disable statistics for any column.
  pub fn set_statistics_enabled(mut self, value: bool) -> Self {
    self.default_column_properties.set_statistics_enabled(value);
    self
  }

  /// Sets max statistics size for any column.
  /// Applicable only if statistics are enabled.
  pub fn set_max_statistics_size(mut self, value: usize) -> Self {
    self.default_column_properties.set_max_statistics_size(value);
    self
  }

  // ----------------------------------------------------------------------
  // Setters for a specific column

  /// Helper method to get existing or new mutable reference of column properties.
  fn get_mut_props(&mut self, col: ColumnPath) -> &mut ColumnProperties {
    self.column_properties.entry(col).or_insert(ColumnProperties::new())
  }

  /// Sets encoding for a column.
  /// Takes precedence over globally defined settings.
  pub fn set_col_encoding(mut self, col: ColumnPath, value: Encoding) -> Self {
    self.get_mut_props(col).set_encoding(value);
    self
  }

  /// Sets compression codec for a column.
  /// Takes precedence over globally defined settings.
  pub fn set_col_compression(mut self, col: ColumnPath, value: Compression) -> Self {
    self.get_mut_props(col).set_compression(value);
    self
  }

  /// Sets flag to enable/disable dictionary encoding for a column.
  /// Takes precedence over globally defined settings.
  pub fn set_col_dictionary_enabled(mut self, col: ColumnPath, value: bool) -> Self {
    self.get_mut_props(col).set_dictionary_enabled(value);
    self
  }

  /// Sets flag to enable/disable statistics for a column.
  /// Takes precedence over globally defined settings.
  pub fn set_col_statistics_enabled(mut self, col: ColumnPath, value: bool) -> Self {
    self.get_mut_props(col).set_statistics_enabled(value);
    self
  }

  /// Sets max size for statistics for a column.
  /// Takes precedence over globally defined settings.
  pub fn set_col_max_statistics_size(mut self, col: ColumnPath, value: usize) -> Self {
    self.get_mut_props(col).set_max_statistics_size(value);
    self
  }
}
