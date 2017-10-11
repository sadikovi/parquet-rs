extern crate parquet;

use parquet::schema::parser::parse_message_type;

fn main() {
  let schema = "
  message schema {
    required int32 a;
    required int64 b;
    optional binary c (UTF8);
    required group d {
      required int32 a;
      required int64 b;
      optional binary c (UTF8);
    }
    required group e (LIST) {
      repeated group list {
        required int32 element;
      }
    }
  }
  ";
  println!("{}", schema);
  println!("{:?}", parse_message_type(schema).unwrap());
}
