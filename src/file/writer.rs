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

use std::io::Write;

use basic::{Compression, Encoding, PageType};
use column::page::Page;
use compression::{Codec, create_codec};
use errors::Result;
use file::statistics::Statistics;
use parquet_format::{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, PageHeader};
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use util::io::{Position, TOutputStream};
use util::memory::ByteBufferPtr;

/// Serialized page writer.
///
/// Writes and serializes data pages into output stream,
/// and provides proxy for comporession.
pub struct SerializedPageWriter<T: Write + Position> {
  sink: T,
  compressor: Option<Box<Codec>>,
  dictionary_page_offset: Option<u64>,
  data_page_offset: Option<u64>,
  total_uncompressed_size: u64,
  total_compressed_size: u64,
  num_values: u64
}

impl<T: Write + Position> SerializedPageWriter<T> {
  /// Creates new page writer.
  pub fn new(codec: Compression, sink: T) -> Self {
    Self {
      sink: sink,
      compressor: create_codec(codec).expect("Codec is supported"),
      dictionary_page_offset: None,
      data_page_offset: None,
      total_uncompressed_size: 0,
      total_compressed_size: 0,
      num_values: 0
    }
  }

  /// Returns dictionary page offset, if set.
  #[inline]
  pub fn dictionary_page_offset(&self) -> Option<u64> {
    self.dictionary_page_offset
  }

  /// Returns data page (either v1 or v2) offset, if set.
  #[inline]
  pub fn data_page_offset(&self) -> Option<u64> {
    self.data_page_offset
  }

  /// Returns total uncompressed size so far.
  #[inline]
  pub fn total_uncompressed_size(&self) -> u64 {
    self.total_uncompressed_size
  }

  /// Returns total compressed size so far.
  #[inline]
  pub fn total_compressed_size(&self) -> u64 {
    self.total_compressed_size
  }

  /// Returns number of values so far.
  #[inline]
  pub fn num_values(&self) -> u64 {
    self.num_values
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
  pub fn write_dictionary_page(&mut self, page: Page) -> Result<usize> {
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

        let start_pos = self.sink.pos();
        assert!(self.dictionary_page_offset.is_none(), "Dictionary page is already set");
        self.dictionary_page_offset = Some(start_pos);
        let header_size = self.serialize_page_header(page_header)?;
        self.sink.write_all(buf.data())?;

        self.total_uncompressed_size += (uncompressed_size + header_size) as u64;
        self.total_compressed_size += (compressed_size + header_size) as u64;

        // Return number of bytes written
        let bytes_written = (self.sink.pos() - start_pos) as usize;
        Ok(bytes_written)
      }
      _ => panic!("Write dictionary page only")
    }
  }

  /// Writes compressed data page into output stream.
  pub fn write_data_page(&mut self, page: CompressedPage) -> Result<usize> {
    let uncompressed_size = page.uncompressed_size();
    let compressed_size = page.compressed_size();
    let num_values = page.num_values();
    let encoding = page.encoding();

    let mut page_header = PageHeader {
      type_: page.page_type().into(),
      uncompressed_page_size: uncompressed_size as i32,
      compressed_page_size: compressed_size as i32,
      // TODO: Add support for crc checksum
      crc: None,
      data_page_header: None,
      index_page_header: None,
      dictionary_page_header: None,
      data_page_header_v2: None
    };

    match page {
      CompressedPage::DataPage { def_level_encoding, rep_level_encoding, .. } => {
        let data_page_header = DataPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          definition_level_encoding: def_level_encoding.into(),
          repetition_level_encoding: rep_level_encoding.into(),
          // TODO: Process statistics
          statistics: None
        };

        page_header.data_page_header = Some(data_page_header);
      },
      CompressedPage::DataPageV2 {
        num_nulls,
        num_rows,
        def_levels_byte_len,
        rep_levels_byte_len,
        is_compressed,
        ..
      } => {
        let data_page_header_v2 = DataPageHeaderV2 {
          num_values: num_values as i32,
          num_nulls: num_nulls as i32,
          num_rows: num_rows as i32,
          encoding: encoding.into(),
          definition_levels_byte_length: def_levels_byte_len as i32,
          repetition_levels_byte_length: rep_levels_byte_len as i32,
          is_compressed: Some(is_compressed),
          // TODO: Process statistics
          statistics: None
        };

        page_header.data_page_header_v2 = Some(data_page_header_v2);
      }
    }

    let start_pos = self.sink.pos();
    if self.data_page_offset.is_none() {
      self.data_page_offset = Some(start_pos);
    }

    let header_size = self.serialize_page_header(page_header)?;
    self.sink.write_all(page.data())?;

    self.total_uncompressed_size += (uncompressed_size + header_size) as u64;
    self.total_compressed_size += (compressed_size + header_size) as u64;
    self.num_values += num_values as u64;

    let bytes_written = (self.sink.pos() - start_pos) as usize;
    Ok(bytes_written)
  }

  /// Serializes page header into Thrift.
  fn serialize_page_header(&mut self, page_header: PageHeader) -> Result<usize> {
    let start_pos = self.sink.pos();
    {
      let transport = TOutputStream::new(&mut self.sink);
      let mut protocol = TCompactOutputProtocol::new(transport);
      page_header.write_to_out_protocol(&mut protocol)?;
      protocol.flush()?;
    }
    Ok((self.sink.pos() - start_pos) as usize)
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

impl CompressedPage {
  /// Returns page type.
  pub fn page_type(&self) -> PageType {
    match self {
      CompressedPage::DataPage { .. } => PageType::DATA_PAGE,
      CompressedPage::DataPageV2 { .. } => PageType::DATA_PAGE_V2
    }
  }

  /// Returns uncompressed size in bytes.
  pub fn uncompressed_size(&self) -> usize {
    match *self {
      CompressedPage::DataPage { uncompressed_size, .. } => uncompressed_size,
      CompressedPage::DataPageV2 { uncompressed_size, .. } => uncompressed_size
    }
  }

  /// Returns compressed size in bytes.
  ///
  /// Note that it is assumed that buffer is compressed, but it may not be. In this
  /// case compressed size will be equal to uncompressed size.
  pub fn compressed_size(&self) -> usize {
    match self {
      CompressedPage::DataPage { buf, .. } => buf.len(),
      CompressedPage::DataPageV2 { buf, .. } => buf.len()
    }
  }

  /// Number of values in the data page.
  pub fn num_values(&self) -> u32 {
    match *self {
      CompressedPage::DataPage { num_values, .. } => num_values,
      CompressedPage::DataPageV2 { num_values, .. } => num_values
    }
  }

  /// Returns encoding for values in the data page.
  pub fn encoding(&self) -> Encoding {
    match *self {
      CompressedPage::DataPage { encoding, .. } => encoding,
      CompressedPage::DataPageV2 { encoding, .. } => encoding
    }
  }

  /// Returns slice of compressed buffer in data page.
  pub fn data(&self) -> &[u8] {
    match self {
      CompressedPage::DataPage { buf, .. } => buf.data(),
      CompressedPage::DataPageV2 { buf, .. } => buf.data()
    }
  }
}
