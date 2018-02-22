extern crate parquet;

use std::fs::File;
use std::path::Path;
use std::rc::Rc;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_schema};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use parquet::record::api::{GroupConverter, RecordMaterializer};

struct MyRecord {}

impl MyRecord {}

impl RecordMaterializer for MyRecord {
  fn get_root_converter(&mut self, schema: &Type) -> &GroupConverter {
    println!("creating root converter for schema: {:?}", schema);
    unimplemented!();
  }

  fn consume_current_record(&mut self) {
    println!("Consume current record");
  }
}

fn main() {
    let path = Path::new("data/sample2.snappy.parquet");
    let file = File::open(&path).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let metadata = parquet_reader.metadata();
    print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
    println!();

    let schema = "
      message spark_schema {
        required int32 a;
      }
    ";
    let schema = Rc::new(parse_message_type(schema).unwrap());
    println!("Projected schema");
    print_schema(&mut std::io::stdout(), &schema);
    println!();

    let recmat = Rc::new(MyRecord {});
    parquet_reader.read_data(schema, recmat);
}
