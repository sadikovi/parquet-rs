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

use basic::{Compression as CodecType};
use parquet_format::{ColumnChunk, ColumnMetaData};
use schema::types::ColumnDescPtr;

/*
// Builder for column chunk metadata.
pub struct ColumnChunkMetaDataBuilder {
  // Column descriptor
  column_descr: ColumnDescPtr,
  // Total bytes written by column writer
  total_bytes_written: u64,
  // Dictionary encoding
  dictionary_encoding: Option<Encoding>,
  // Data page encoding for dictionary
  dictionary_datapage_encoding: Option<Encoding>,
  // Fallback encoding
  encoding: Option<Encoding>,
  // Thrift column chunk (can be updated)
  column_chunk: ColumnChunk
}

impl ColumnChunkMetaDataBuilder {
  pub fn new(column_descr: ColumnDescPtr) -> Self {
    let column_metadata = ColumnMetaData {
      // Type of this column *
      type_: column_descr.physical_type().into(),
      // Set of all encodings used for this column.
      encodings: vec![],
      // Path in schema.
      path_in_schema: Vec::from(column_descr.path().as_slice()),
      // Compression codec.
      codec: CodecType::UNCOMPRESSED.into(),
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
      column_descr: column_descr,
      dictionary_encoding: None,
      dictionary_datapage_encoding: None,
      encoding: None,
      total_bytes_written: 0,
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

  /// Sets compression for this column chunk.
  #[inline]
  pub fn set_compression(&mut self, codec: CodecType) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.codec = codec.into();
  }

  /// Sets file path for column chunk.
  #[inline]
  pub fn set_file_path(&mut self, path: String) {
    self.column_chunk.file_path = Some(path);
  }

  /// Sets total number of bytes written for this column chunk.
  /// This is not part of the actual column chunk metadata, but it is used for row groups.
  #[inline]
  pub fn set_total_bytes_written(&mut self, total_bytes_written: u64) {
    self.total_bytes_written = total_bytes_written;
  }

  /// Sets number of values in this column chunk.
  #[inline]
  pub fn set_num_values(&mut self, num_values: u64) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.num_values = num_values as i64;
  }

  /// Sets total compressed size in bytes for this column chunk.
  #[inline]
  pub fn set_total_compressed_size(&mut self, total_compressed_size: u64) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.total_compressed_size = total_compressed_size as i64;
  }

  /// Sets total uncompressed size in bytes for this column chunk.
  #[inline]
  pub fn set_total_uncompressed_size(&mut self, total_uncompressed_size: u64) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.total_uncompressed_size = total_uncompressed_size as i64;
  }

  /// Sets dictionary page offset in bytes, if dictionary page exists.
  #[inline]
  pub fn set_dictionary_page_offset(&mut self, dictionary_page_offset: Option<u64>) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.dictionary_page_offset = dictionary_page_offset.map(|v| v as i64);
  }

  /// Sets data page offset in bytes, defaults to position 0.
  #[inline]
  pub fn set_data_page_offset(&mut self, data_page_offset: Option<u64>) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.data_page_offset = data_page_offset.unwrap_or(0) as i64;
  }

  /// Sets all dictionary related encodings, if enabled.
  #[inline]
  pub fn set_dictionary_encoding(&mut self, dict_enc: Encoding, datapage_enc: Encoding) {
    self.dictionary_encoding = Some(dict_enc);
    // Encoding of data pages when dictionary is enabled.
    self.dictionary_datapage_encoding = Some(datapage_enc);
  }

  /// Sets fallback encoding.
  #[inline]
  pub fn set_encoding(&mut self, encoding: Encoding) {
    self.encoding = Some(encoding);
  }

  /// Finalises column chunk metadata and sets Thrift definition.
  ///
  /// Should be called after all options above are set.
  /// After calling this method, column chunk metadata builder should be considered
  /// read-only.
  pub fn finish(&mut self) {
    let metadata = self.column_chunk.meta_data.as_mut().unwrap();
    metadata.thrift_encodings = Vec::new();

    if let Some(dict_offset) = metadata.dictionary_page_offset {
      assert!(self.dictionary_encoding.is_some(), "Dictionary is not set");
      assert!(self.dictionary_datapage_encoding.is_some(), "Dictionary is not set");

      self.column_chunk.file_offset = dict_offset + metadata.total_compressed_size;
      metadata.thrift_encodings.push(self.dictionary_encoding.unwrap().into());
      metadata.thrift_encodings.push(self.dictionary_datapage_encoding.unwrap().into());
      if fallback {
        metadta.thrift_encodings.push(self.encoding.into());
      }
    } else {
      self.column_chunk.file_offset = metadata.data_page_offset + metadata.total_compressed_size;
      metadta.thrift_encodings.push(self.encoding.into());
    }
  }

  /*
  pub fn finish(
    &mut self,
    num_values: i64,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    data_page_offset: i64,
    dictionary_page_offset: Option<i64>
  )
  {
    let meta_data = self.column_chunk.meta_data.as_mut().unwrap();
    meta_data.num_values = num_values;
    meta_data.total_compressed_size = total_compressed_size;
    meta_data.total_uncompressed_size = uncompressed_size;
    meta_data.data_page_offset = data_page_offset;
    meta_data.dictionary_page_offset = dictionary_page_offset;
    meta_data.index_page_offset = None;

    let mut thrift_encodings = Vec::new();

    if let Some(dict_offset) = dictionary_page_offset {
      self.column_chunk.file_offset = dict_offset + compressed_size;
      // TODO: parquet-cpp forces PLAIN encoding for data page in dictionary case,
      // but we store whatever V1 encoding is set.
      thrift_encodings.push(self.properties.dictionary_page_encoding().into());
      thrift_encodings.push(self.properties.data_page_dictionary_encoding().into());
    } else {
      // dictionary is not enabled
      self.column_chunk.file_offset = data_page_offset + compressed_size;
      thrift_encodings.push(self.properties.encoding(self.column_descr.path()).into());
    }
    if dictionary_fallback {
      match self.properties.writer_version() {
        WriterVersion::PARQUET_1_0 => thrift_encodings.push(Encoding::PLAIN.into()),
        WriterVersion::PARQUET_2_0 => panic!("Not supported for V2")
      }
    }
    meta_data.encodings = thrift_encodings;
  }
  */
}

// ColumnChunkMetaDataBuilder
// RowGroupMetaDataBuilder
// FileMetaDataBuilder
// ParquetMetaDataBuilder
*/
