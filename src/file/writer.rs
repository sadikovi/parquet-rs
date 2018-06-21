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

use basic::{Compression, Encoding, PageType};
use column::page::Page;
use compression::{Codec, create_codec};
use errors::Result;
use file::statistics::Statistics;
use parquet_format::{DictionaryPageHeader, PageHeader};
use util::io::PosWrite;
use util::memory::ByteBufferPtr;

/// Serialized page writer.
///
/// Writes and serializes data pages into output stream,
/// and provides proxy for comporession.
pub struct SerializedPageWriter {
  compressor: Option<Box<Codec>>,
  dictionary_page_offset: Option<u64>,
  total_uncompressed_size: u64,
  total_compressed_size: u64
}

impl SerializedPageWriter {
  /// Creates new page writer.
  pub fn new(codec: Compression) -> Self {
    Self {
      compressor: create_codec(codec).expect("Codec is supported"),
      dictionary_page_offset: None,
      total_uncompressed_size: 0,
      total_compressed_size: 0
    }
  }

  /// Returns true, if page writer has a compressor set.
  #[inline]
  pub fn has_compressor(&self) -> bool {
    self.compressor.is_some()
  }

  /// Compresses input buffer bytes into output buffer.
  /// Fails if compressor is not set.
  #[inline]
  pub fn compress(&mut self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<()> {
    assert!(self.has_compressor());
    self.compressor.as_mut().unwrap().compress(input_buf, output_buf)
  }

  /// Writes dictionary page into output stream.
  /// Should be written once per column.
  pub fn write_dictionary_page(
    &mut self,
    page: Page,
    sink: &mut PosWrite
  ) -> Result<usize> {
    match page {
      Page::DictionaryPage { buf, num_values, encoding, is_sorted } => {
        let uncompressed_size = buf.len();
        let buf = if self.has_compressor() {
          // TODO: reuse output buffer?
          let mut output_buf = Vec::with_capacity(uncompressed_size);
          self.compress(buf.data(), &mut output_buf)?;
          ByteBufferPtr::new(output_buf)
        } else {
          buf
        };
        let compressed_size = buf.len();

        // Create page headers
        let dictionary_page_header = DictionaryPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          is_sorted: Some(is_sorted)
        };

        let page_header = PageHeader {
          type_: PageType::DICTIONARY_PAGE.into(),
          uncompressed_page_size: uncompressed_size as i32,
          compressed_page_size: compressed_size as i32,
          // TODO: Add support for crc checksum
          crc: None,
          data_page_header: None,
          index_page_header: None,
          dictionary_page_header: Some(dictionary_page_header),
          data_page_header_v2: None,
        };

        let start_pos = sink.pos();
        assert!(self.dictionary_page_offset.is_none(), "Dictionary page is already set");
        self.dictionary_page_offset = Some(start_pos);
        let header_size = self.serialize_page_header(page_header, sink)?;
        sink.write_all(buf.data())?;

        self.total_uncompressed_size += (uncompressed_size + header_size) as u64;
        self.total_compressed_size += (compressed_size + header_size) as u64;

        // Return number of bytes written
        let bytes_written = (sink.pos() - start_pos) as usize;
        Ok(bytes_written)
      }
      _ => panic!("Write dictionary page only")
    }
  }

  pub fn write_data_page(
    &mut self,
    page: CompressedPage,
    sink: &mut PosWrite
  ) -> Result<usize> {
    unimplemented!();
  }

  fn serialize_page_header(
    &mut self,
    _page_header: PageHeader,
    _sink: &mut PosWrite
  ) -> Result<usize> {
    unimplemented!();
  }
}

/// Helper struct to represent pages with potentially compressed buffer or concatenated
/// buffer (def levels + rep levels + compressed values) for data page v2, so not to
/// break the assumption that `Page` buffer is uncompressed.
pub enum CompressedPage {
  DataPage {
    uncompressed_size: usize,
    buf: ByteBufferPtr,
    num_values: u32,
    encoding: Encoding,
    def_level_encoding: Encoding,
    rep_level_encoding: Encoding,
    statistics: Option<Statistics>
  },
  DataPageV2 {
    uncompressed_size: usize,
    buf: ByteBufferPtr,
    num_values: u32,
    encoding: Encoding,
    num_nulls: u32,
    num_rows: u32,
    def_levels_byte_len: u32,
    rep_levels_byte_len: u32,
    is_compressed: bool,
    statistics: Option<Statistics>
  }
}
