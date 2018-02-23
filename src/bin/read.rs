extern crate parquet;

use std::fs::File;
use std::path::Path;

use parquet::data_type::*;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::{print_file_metadata, print_schema};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use parquet::record::api::{GroupConverter, RecordMaterializer, PrimitiveConverter};

// converters
#[derive(Debug)]
enum Converter {
  Group(Vec<Converter>),
  Primitive,
}

impl Converter {
  fn new_group() -> Self {
    Converter::Group(Vec::new())
  }

  fn new_primitive() -> Self {
    Converter::Primitive {}
  }

  fn add_child(&mut self, child: Converter) {
    match self {
      &mut Converter::Group(ref mut children) => children.push(child),
      &mut Converter::Primitive => panic!("cannot add child for primitive converter"),
    }
  }
}

impl GroupConverter for Converter {
  fn start(&mut self) {
    match self {
      &mut Converter::Group(_) => println!("    start group"),
      &mut Converter::Primitive => panic!("primitive converter"),
    }
  }

  fn end(&mut self) {
    match self {
      &mut Converter::Group(_) => println!("    end group"),
      &mut Converter::Primitive => panic!("primitive converter"),
    }
  }

  fn child_group_converter(&mut self, ordinal: usize) -> &mut GroupConverter {
    match self {
      &mut Converter::Group(ref mut children) => &mut children[ordinal],
      &mut Converter::Primitive => panic!("primitive converter"),
    }
  }

  fn child_primitive_converter(&mut self, ordinal: usize) -> &mut PrimitiveConverter {
    match self {
      &mut Converter::Group(ref mut children) => &mut children[ordinal],
      &mut Converter::Primitive => panic!("primitive converter"),
    }
  }
}

impl PrimitiveConverter for Converter {
  fn add_boolean(&mut self, value: bool) { println!(" >>> add {}", value); }

  fn add_int32(&mut self, value: i32) { println!(" >>> add {}", value); }

  fn add_int64(&mut self, value: i64) { println!(" >>> add {}", value); }

  fn add_int96(&mut self, value: Int96) { println!(" >>> add {:?}", value); }

  fn add_float(&mut self, value: f32) { println!(" >>> add {}", value); }

  fn add_double(&mut self, value: f64) { println!(" >>> add {}", value); }

  fn add_byte_array(&mut self, value: ByteArray) { println!(" >>> add {:?}", value); }

  fn add_fixed_len_byte_array(&mut self, value: ByteArray) { println!(" >>> add {:?}", value); }
}

#[derive(Debug)]
struct MyRecord {
  root: Converter
}

impl MyRecord {
  fn new() -> Self {
    MyRecord { root: Converter::new_group() }
  }

  fn init_recur(schema: &Type, parent: &mut Converter) {
    for ptr in schema.get_fields() {
      if ptr.is_primitive() {
        parent.add_child(Converter::new_primitive());
      } else {
        let mut group = Converter::new_group();
        Self::init_recur(ptr, &mut group);
        parent.add_child(group);
      }
    }
  }
}

impl RecordMaterializer for MyRecord {
  fn init(&mut self, schema: &Type) {
    println!("* preparing RM");
    assert!(schema.is_schema());
    Self::init_recur(schema, &mut self.root);
  }

  fn root_converter(&mut self) -> &mut GroupConverter {
    &mut self.root
  }

  fn consume_current_record(&mut self) {
    println!("    consume current record");
  }
}

fn main() {
    let path = Path::new("data/sample2.snappy.parquet");
    let file = File::open(&path).unwrap();
    let parquet_reader = SerializedFileReader::new(file).unwrap();
    let metadata = parquet_reader.metadata();
    print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
    println!();

    /*
    let schema = "
      message spark_schema {
        required int32 a;
      }
    ";
    */

    let schema = "
      message spark_schema {
        REQUIRED INT32 a;
        OPTIONAL group b {
          OPTIONAL INT32 _1;
          OPTIONAL BOOLEAN _2;
        }
      }
    ";
    let schema = parse_message_type(schema).unwrap();
    println!("Projected schema");
    print_schema(&mut std::io::stdout(), &schema);
    println!();

    let mut rm = Box::new(MyRecord::new()) as Box<RecordMaterializer>;
    parquet_reader.read_data(schema, &mut rm);
}
