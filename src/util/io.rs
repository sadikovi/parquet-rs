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

use std::cmp;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::sync::Mutex;

use thrift::transport::TWriteTransport;

/// Struct that represents a slice of a file data with independent start position and
/// length. Internally clones provided file handle, wraps with BufReader and resets
/// position before any read.
///
/// This is workaround and alternative for `file.try_clone()` method. It clones `File`
/// while preserving independent position, which is not available with `try_clone()`.
///
/// Designed after `arrow::io::RandomAccessFile`.
pub struct FileChunk {
  reader: Mutex<BufReader<File>>,
  start: usize, // start position in a file
  end: usize // end position in a file
}

impl FileChunk {
  /// Creates new file reader with start and length from a file handle
  pub fn new(fd: &File, start: usize, length: usize) -> Self {
    Self {
      reader: Mutex::new(BufReader::new(fd.try_clone().unwrap())),
      start: start,
      end: start + length
    }
  }
}

impl Read for FileChunk {
  fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
    let mut reader = self.reader.lock()
      .map_err(|err| Error::new(ErrorKind::Other, err.to_string()))?;

    let bytes_to_read = cmp::min(buf.len(), self.end - self.start);
    let buf = &mut buf[0..bytes_to_read];

    reader.seek(SeekFrom::Start(self.start as u64))?;
    let res = reader.read(buf);
    if let Ok(bytes_read) = res {
      self.start += bytes_read;
    }

    res
  }
}

/// Positional write trait.
///
/// Tracks the current position in the stream.
/// Should be used together with `Write` trait.
pub trait Position {
  /// Returns the current position in the stream.
  fn pos(&self) -> usize;
}

/// Output stream as a thin wrapper for `PosWrite`.
/// Wraps positional write steam to work as transport in Thrift.
pub struct TOutputStream<'a> {
  out: &'a mut Write
}

impl<'a> TOutputStream<'a> {
  pub fn new(stream: &'a mut Write) -> Self {
    Self { out: stream }
  }
}

impl<'a> Write for TOutputStream<'a> {
  fn write(&mut self, buf: &[u8]) -> Result<usize> {
    self.out.write(buf)
  }

  fn flush(&mut self) -> Result<()> {
    self.out.flush()
  }
}

impl<'a> TWriteTransport for TOutputStream<'a> {
}


#[cfg(test)]
mod tests {
  use super::*;
  use util::test_common::get_test_file;

  #[test]
  fn test_io_read_fully() {
    let mut buf = vec![0; 8];
    let mut chunk = FileChunk::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

    let bytes_read = chunk.read(&mut buf[..]).unwrap();
    assert_eq!(bytes_read, 4);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1', 0, 0, 0, 0]);
  }

  #[test]
  fn test_io_read_in_chunks() {
    let mut buf = vec![0; 4];
    let mut chunk = FileChunk::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

    let bytes_read = chunk.read(&mut buf[0..2]).unwrap();
    assert_eq!(bytes_read, 2);
    let bytes_read = chunk.read(&mut buf[2..]).unwrap();
    assert_eq!(bytes_read, 2);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
  }

  #[test]
  fn test_io_seek_switch() {
    let mut buf = vec![0; 4];
    let mut file = get_test_file("alltypes_plain.parquet");
    let mut chunk = FileChunk::new(&file, 0, 4);

    file.seek(SeekFrom::Start(5 as u64)).expect("File seek to a position");

    let bytes_read = chunk.read(&mut buf[..]).unwrap();
    assert_eq!(bytes_read, 4);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
  }
}
