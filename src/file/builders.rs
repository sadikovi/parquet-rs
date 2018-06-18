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

//! Contains metadata builders.

use file::properties::WriterProperties;
use parquet_format::{ColumnChunk, ColumnMetaData};
use schema::types::ColumnDescPtr;

// Builder for column chunk metadata.
pub struct ColumnChunkMetaDataBuilder {
  column_chunk: ColumnChunk
}

impl ColumnChunkMetaDataBuilder {
  pub fn new(properties: &WriterProperties, column_descr: ColumnDescPtr) -> Self {
    let column_metadata = ColumnMetaData {
      // Type of this column *
      type_: column_descr.physical_type().into(),
      // Set of all encodings used for this column.
      encodings: vec![],
      // Path in schema.
      path_in_schema: vec![],
      // Compression codec.
      codec: properties.compression(column_descr.path()).into(),
      // Number of values in this column.
      num_values: 0,
      // Total byte size of all uncompressed pages in this column chunk.
      total_uncompressed_size: 0,
      // Total byte size of all compressed pages in this column chunk.
      total_compressed_size: 0,
      // Optional key/value metadata.
      key_value_metadata: None,
      // Byte offset from beginning of file to first data page.
      data_page_offset: 0,
      // Byte offset from beginning of file to root index page.
      index_page_offset: None,
      // Byte offset from the beginning of file to first (only) dictionary page.
      dictionary_page_offset: None,
      // Optional statistics for this column chunk.
      statistics: None,
      // Set of all encodings used for pages in this column chunk.
      encoding_stats: None
    };

    Self {
      column_chunk: ColumnChunk {
        // Path is relative to the current file.
        file_path: None,
        // Byte offset in file_path to the ColumnMetaData
        file_offset: 0,
        meta_data: Some(column_metadata),
        offset_index_offset: None,
        offset_index_length: None,
        column_index_offset: None,
        column_index_length: None
      }
    }
  }

  pub fn set_file_path(&mut self, path: String) {
    self.column_chunk.file_path = Some(path);
  }
}

// ColumnChunkMetaDataBuilder
// RowGroupMetaDataBuilder
// FileMetaDataBuilder
// ParquetMetaDataBuilder
