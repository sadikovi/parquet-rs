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
use std::io::{BufReader, Read, Result, Seek, SeekFrom};

/// Struct to provide clone of file handle with independent start and length.
/// Internally clones provided file handle and uses BufReader.
///
/// This is an alternative for `file.try_clone()` method, but it creates copy
/// with independent position as opposite to shared one.
///
pub struct FileHandle {
  reader: BufReader<File>,
  start: usize, // start position in a file
  end: usize // end position in a file
}

impl FileHandle {
  /// Creates new file reader with start and length from a file handle
  pub fn new(fd: &File, start: usize, length: usize) -> Self {
    Self {
      reader: BufReader::new(fd.try_clone().unwrap()),
      start: start,
      end: start + length
    }
  }
}

impl Read for FileHandle {
  fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
    let bytes_to_read = cmp::min(buf.len(), self.end - self.start);
    let buf = &mut buf[0..bytes_to_read];

    self.reader.seek(SeekFrom::Start(self.start as u64))?;
    let res = self.reader.read(buf);
    if let Ok(bytes_read) = res {
      self.start += bytes_read;
    }

    res
  }
}
