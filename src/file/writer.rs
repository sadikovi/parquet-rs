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

use basic::{Compression, PageType};
use column::page::{CompressedPage, PageWriter};
use compression::{Codec, create_codec};
use errors::Result;
use file::metadata::ColumnChunkMetaData;
use parquet_format::{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, PageHeader};
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use util::io::{Position, TOutputStream};

// TODO: Clean up metrics that we collect in page writer, see if we actually use any.

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
  num_values: u32
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

impl<T: Write + Position> PageWriter for SerializedPageWriter<T> {
  fn write_page(&mut self, page: CompressedPage) -> Result<usize> {
    let uncompressed_size = page.uncompressed_size();
    let compressed_size = page.compressed_size();
    let num_values = page.num_values();
    let encoding = page.encoding();
    let page_type = page.page_type();

    let mut page_header = PageHeader {
      type_: page_type.into(),
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
      },
      CompressedPage::DictionaryPage { is_sorted, .. } => {
        let dictionary_page_header = DictionaryPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          is_sorted: Some(is_sorted)
        };
        page_header.dictionary_page_header = Some(dictionary_page_header);
      }
    }

    let start_pos = self.sink.pos();

    match page_type {
      PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => {
        if self.data_page_offset.is_none() {
          self.data_page_offset = Some(start_pos);
        }
      },
      PageType::DICTIONARY_PAGE => {
        assert!(self.dictionary_page_offset.is_none(), "Dictionary page is already set");
        self.dictionary_page_offset = Some(start_pos);
      }
      _ => {
        // Do nothing
      }
    }

    let header_size = self.serialize_page_header(page_header)?;
    self.sink.write_all(page.data())?;

    self.total_uncompressed_size += (uncompressed_size + header_size) as u64;
    self.total_compressed_size += (compressed_size + header_size) as u64;
    self.num_values += num_values;

    let bytes_written = (self.sink.pos() - start_pos) as usize;
    Ok(bytes_written)
  }

  fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> Result<()> {
    unimplemented!();
  }

  #[inline]
  fn dictionary_page_offset(&self) -> Option<u64> {
    self.dictionary_page_offset
  }

  #[inline]
  fn data_page_offset(&self) -> u64 {
    self.data_page_offset.unwrap_or(0)
  }

  #[inline]
  fn total_uncompressed_size(&self) -> u64 {
    self.total_uncompressed_size
  }

  #[inline]
  fn total_compressed_size(&self) -> u64 {
    self.total_compressed_size
  }

  #[inline]
  fn num_values(&self) -> u32 {
    self.num_values
  }
}
