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

use std::cmp;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use basic::Repetition;
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
  unimplemented!();
}

/// Performs topological sort on fields to reconstruct the order.
fn topological(first: &[TypePtr], second: &[TypePtr]) -> Result<Vec<TypePtr>> {
  let len = first.len() + second.len();

  // Prepare links
  let mut links: HashMap<&str, TypePtr> = HashMap::with_capacity(len);
  merge_and_update_types(&mut links, first, second)?;

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
fn merge_and_update_types<'a, 'b>(
  links: &'a mut HashMap<&'b str, TypePtr>,
  left: &'b [TypePtr],
  right: &'b [TypePtr]
) -> Result<()> {
  for tpe in left {
    links.insert(tpe.name(), tpe.clone());
  }
  for tpe in right {
    let upd = match links.get(tpe.name()) {
      Some(obj) => merge_types(tpe, obj)?,
      None => tpe.clone()
    };
    links.insert(tpe.name(), upd);
  }
  Ok(())
}

/// Merges two types into one, if possible.
/// Both types have the same name.
fn merge_types(left: &TypePtr, right: &TypePtr) -> Result<TypePtr> {
  let left_info = left.get_basic_info();
  let right_info = right.get_basic_info();

  let compatible =
    left_info.name() == right_info.name() &&
    left_info.logical_type() == right_info.logical_type();

  if left.is_group() && right.is_group() && compatible {
    let builder = SchemaType::group_type_builder(left_info.name())
      .with_logical_type(left_info.logical_type())
      .with_fields(&mut topological(left.get_fields(), right.get_fields())?);

    if left_info.has_repetition() && right_info.has_repetition() {
      let left_rep = left_info.repetition();
      let right_rep = right_info.repetition();
      let repetition = merge_repetition(left_rep, right_rep)?;
      return Ok(Rc::new(builder.with_repetition(repetition).build()?));
    } else if !left_info.has_repetition() && !right_info.has_repetition() {
      return Ok(Rc::new(builder.build()?));
    }
  } else if left.is_primitive() && right.is_primitive() {
    let repetition = merge_repetition(left_info.repetition(), right_info.repetition())?;
    let mut max_length = 0;
    let mut max_scale = 0;
    let mut max_precision = 0;

    match **left {
      SchemaType::PrimitiveType { physical_type, type_length, scale, precision, .. } => {
        max_length = cmp::max(max_length, type_length);
        max_precision = cmp::max(max_precision, precision);
        max_scale = cmp::max(max_scale, scale);
      },
      _ => { }
    }

    match **right {
      SchemaType::PrimitiveType { physical_type, type_length, scale, precision, .. } => {
        max_length = cmp::max(max_length, type_length);
        max_precision = cmp::max(max_precision, precision);
        max_scale = cmp::max(max_scale, scale);
      },
      _ => { }
    }

    let builder =
      SchemaType::primitive_type_builder(left.name(), left.get_physical_type())
        .with_repetition(repetition)
        .with_logical_type(left_info.logical_type())
        .with_length(max_length)
        .with_precision(max_precision)
        .with_scale(max_scale);

    return Ok(Rc::new(builder.build()?));
  }

  return Err(general_err!("Cannot merge types: {:?} != {:?}", left, right));
}

/// Merges repetition, if possible.
fn merge_repetition(rep1: Repetition, rep2: Repetition) -> Result<Repetition> {
  if rep1 == rep2 {
    Ok(rep1)
  } else if rep1 == Repetition::REQUIRED && rep2 == Repetition::OPTIONAL {
    Ok(Repetition::OPTIONAL)
  } else if rep1 == Repetition::OPTIONAL && rep2 == Repetition::REQUIRED {
    Ok(Repetition::OPTIONAL)
  } else {
    Err(general_err!("Cannot merge repetitions: {} and {}", rep1, rep2))
  }
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
