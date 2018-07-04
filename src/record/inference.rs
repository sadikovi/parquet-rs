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

//! Methods to infer schema from `Row`.

use std::collections::{HashMap, VecDeque};

use errors::{ParquetError, Result};
use record::api::Row;
use schema::types::{Type as SchemaType, TypePtr};

/// Infers schema from a set of `Row`s.
/// Returns error if the list of rows is empty.
pub fn infer_schema(rows: &[Row]) -> Result<SchemaType> {
  let len = rows.len();
  if len == 0 {
    Err(general_err!("Could not infer schema from empty slice"))
  } else if len == 1 {
    Ok(infer_from_row(&rows[0]))
  } else {
    merge_schema(
      infer_schema(&rows[0..len/2])?,
      infer_schema(&rows[len/2..])?,
    )
  }
}

/// Method to infer schema from a single row.
fn infer_from_row(row: &Row) -> SchemaType {
  unimplemented!();
}

/// Merges two schemas into one.
/// Represents the best attempt at merging schema, returns error in case of
/// mismatch.
fn merge_schema(left: SchemaType, right: SchemaType) -> Result<SchemaType> {
  if !left.is_schema() {
    return Err(general_err!("Expected message type, found {:?}", left));
  }
  if !right.is_schema() {
    return Err(general_err!("Expected message type, found {:?}", right));
  }

  let mut fields = topological(left.get_fields(), right.get_fields())?;
  // In order to merge 2 sets of fields we apply topological sort to ensure that schema
  // can be merged (no cycles), and we also preserve the relative order of the fields,
  // e.g.
  // [a, b] and [c, a, d] => [c, a, b, d]

  // {"a": 1, "b": 2}
  // {"c": 3, "a": 1, "d": 4}
  // {"c": 3, "b": 2, "d": 4}
  // [c, a, b, d]

  // Schemas can have different set of fields, but the ones that match will have the same
  // type, e.g. Primitive == Primitive, Group == Group, but never Primitive != Group.
  // {"a": 1, "b": 2}
  // {"a": 1, "c": 3, "d": 4}
  // {"a" not null, "b" null, "c" null, "d" null}
  // {"a": 1, "b": 2}
  // {"b": 2, "a": 1}
  SchemaType::group_type_builder("schema")
    .with_fields(&mut fields)
    .build()
}

/// Performs topological sort on fields to reconstruct the order.
fn topological(first: &[TypePtr], second: &[TypePtr]) -> Result<Vec<TypePtr>> {
  let len = first.len() + second.len();

  // Prepare links
  let mut links: HashMap<&str, TypePtr> = HashMap::with_capacity(len);
  add_links(&mut links, first)?;
  add_links(&mut links, second)?;

  // Prepare edges
  let mut edges: HashMap<&str, Vec<&str>> = HashMap::with_capacity(len);
  add_edges(&mut edges, first);
  add_edges(&mut edges, second);

  // Prepare counts
  let mut counts = collect_counts(&edges);

  // Perform sort
  let mut fields = Vec::new();
  let mut queue = VecDeque::new();
  for key in edges.keys() {
    if !counts.contains_key(key) {
      queue.push_back(*key);
    }
  }
  while let Some(key) = queue.pop_front() {
    fields.push(links.get(key).unwrap().clone());
    for elem in &edges[key] {
      if let Some(value) = counts.get_mut(elem) {
        *value -= 1;
        if *value == 0 {
          queue.push_back(elem);
        }
      }
    }
  }

  for &val in counts.values() {
    if val != 0 {
      return Err(general_err!("Found cycle"));
    }
  }

  Ok(fields)
}

/// Adds links from vertices.
fn add_links<'a, 'b>(
  links: &'a mut HashMap<&'b str, TypePtr>,
  vertices: &'b [TypePtr]
) -> Result<()> {
  for ptr in vertices {
    if !links.contains_key(ptr.name()) {
      links.insert(ptr.name(), ptr.clone());
    } else {
      let exp = &links[ptr.name()];
      if exp.is_group() && ptr.is_group() {
        unimplemented!();
      } else if exp.is_primitive() && ptr.is_primitive() {
        unimplemented!();
      } else {
        return Err(general_err!("Type mismatch: {:?} != {:?}", exp, ptr));
      }
    }
  }

  Ok(())
}

/// Adds/updates edges from vertices.
fn add_edges<'a, 'b>(
  edges: &'a mut HashMap<&'b str, Vec<&'b str>>,
  vertices: &'b [TypePtr]
) {
  let len = vertices.len();

  for i in 0..len {
    let key = vertices[i].name();
    if !edges.contains_key(key) {
      edges.insert(key, vec![]);
    }
    if i < len - 1 {
      edges.get_mut(key).unwrap().push(vertices[i + 1].name());
    }
  }
}

/// Collects inverse counts.
fn collect_counts<'a>(
  edges: &'a HashMap<&'a str, Vec<&'a str>>
) -> HashMap<&'a str, usize> {
  let mut counts: HashMap<&str, usize> = HashMap::with_capacity(edges.len());
  for key in edges.keys() {
    for elem in &edges[key] {
      if !counts.contains_key(elem) {
        counts.insert(elem, 1);
      } else {
        let value = counts.get_mut(elem).unwrap();
        *value += 1;
      }
    }
  }

  counts
}


#[cfg(test)]
mod tests {
  use std::rc::Rc;

  use super::*;

  #[test]
  fn test_topological_sort() {
    let first = &[
      Rc::new(SchemaType::group_type_builder("a").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("b").build().unwrap())
    ];

    let second = &[
      Rc::new(SchemaType::group_type_builder("c").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("a").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("d").build().unwrap())
    ];

    println!("{:?}", topological(first, second).unwrap());
  }

  #[test]
  fn test_topological_sort_cycle() {
    let first = &[
      Rc::new(SchemaType::group_type_builder("a").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("b").build().unwrap())
    ];

    let second = &[
      Rc::new(SchemaType::group_type_builder("c").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("b").build().unwrap()),
      Rc::new(SchemaType::group_type_builder("a").build().unwrap())
    ];

    assert!(topological(first, second).is_err());
  }
}
