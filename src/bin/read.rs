extern crate parquet;

use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_schema};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use parquet::record::api::{GroupConverter, RecordMaterializer, PrimitiveConverter};

struct MyRecord {
  root: MyGroup
}

impl MyRecord {
  fn new() -> Self {
    MyRecord { root: MyGroup {} }
  }
}

impl RecordMaterializer for MyRecord {
  fn init(&mut self, _schema: &Type) {
    println!("* preparing RM");
    self.root = MyGroup {};
  }

  fn get_root_converter(&mut self) -> &mut GroupConverter {
    &mut self.root
  }

  fn consume_current_record(&mut self) {
    println!("    consume current record");
  }
}

struct MyGroup {}

impl GroupConverter for MyGroup {
  fn start(&mut self) {
    println!("    start group");
  }

  fn end(&mut self) {
    println!("    end group");
  }

  fn get_child_group_converter(&self, _ordinal: usize) -> &mut GroupConverter {
    unimplemented!();
  }

  fn get_child_primitive_converter(&self, _ordinal: usize) -> &mut PrimitiveConverter {
    unimplemented!();
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
    let schema = parse_message_type(schema).unwrap();
    println!("Projected schema");
    print_schema(&mut std::io::stdout(), &schema);
    println!();

    let mut rm = Box::new(MyRecord::new()) as Box<RecordMaterializer>;
    parquet_reader.read_data(schema, &mut rm);
}
