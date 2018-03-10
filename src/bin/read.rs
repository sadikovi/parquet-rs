extern crate parquet;

use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_schema};
use parquet::schema::parser::parse_message_type;

fn main() {
  let path = Path::new("data/sample4.snappy.parquet");
  let file = File::open(&path).unwrap();
  let parquet_reader = SerializedFileReader::new(file).unwrap();
  let metadata = parquet_reader.metadata();
  print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
  println!();

  // assign full schema as projected schema
  let mut schema = Vec::new();
  print_schema(&mut schema, metadata.file_metadata().schema_descr().root_schema());
  let schema = parse_message_type(&String::from_utf8(schema).unwrap()).unwrap();

  println!("Projected schema");
  print_schema(&mut std::io::stdout(), &schema);
  println!();

  parquet_reader.read_data(schema);
}
