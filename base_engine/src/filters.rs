use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::ops::Index;

/// Inner elements of each Filter are OR
/// Filters themselves(eg a Vec of Filters) are AND
/// 
/// TODO Currently works for Utf8 columns only. In the future 
/// parsing logic will be added based on column datatype
#[derive(Serialize, Deserialize, Debug, Hash, Clone)]
pub enum FilterE {
    /// On Same as In, but better for 1 field only
    Eq(Vec<(String, String)>),
    Neq(Vec<(String, String)>),
    In(Vec<(String, Vec<String>)>),
    NotIn(Vec<(String, Vec<String>)>),
}

impl FilterE{
    pub fn to_expr(&self)->Expr{
        match self {
            FilterE::Eq(f)          => fltr_eq_or_builder(f),
            FilterE::Neq(f)         => fltr_neq_or_builder(f),
            FilterE::In(f)     => fltr_in_or_builder(f),
            FilterE::NotIn(f)  => fltr_not_in_or_builder(f),
        }
    }
}

pub(crate) fn fltr_in_or_builder(_f: &Vec<(String, Vec<String>)>) -> Expr {
    let (a, b) = _f.index(0);
    let s = Series::new("filter", b);
    let mut e = col(a).cast(DataType::Utf8).is_in(s.lit());

    for (i, j) in _f.iter().skip(0) {
        let s = Series::new("filter", j);
        e = e.or(col(i).cast(DataType::Utf8).is_in(s.lit()));
    }
    e
}

pub(crate) fn fltr_not_in_or_builder(_f: &Vec<(String, Vec<String>)>) -> Expr {
    let (a, b) = _f.index(0);
    let s = Series::new("filter", b);
    let mut e = col(a).cast(DataType::Utf8).is_in(s.lit()).not();

    for (i, j) in _f.iter().skip(0) {
        let s = Series::new("filter", j);
        e = e.or(col(i).cast(DataType::Utf8).is_in(s.lit()).not());
    }
    e
}

pub(crate) fn fltr_eq_or_builder(_f: &Vec<(String, String)>) -> Expr {
    let (a, b) = _f.index(0);
    let mut e: Expr = col(a).cast(DataType::Utf8).eq(lit::<&str>(b));
    for (i, j) in _f.iter().skip(1) {
        e = e.or(col(i).cast(DataType::Utf8).eq(lit::<&str>(j)))
    }
    e
}

pub(crate) fn fltr_neq_or_builder(_f: &Vec<(String, String)>) -> Expr {
    let (a, b) = _f.index(0);
    println!("a: {}, b: {}", a, b);
    let mut e: Expr = col(a).cast(DataType::Utf8).neq(lit::<&str>(b));
    for (i, j) in _f.iter().skip(1) {
        e = e.or(col(i).cast(DataType::Utf8).neq(lit::<&str>(j)))
    }
    e
}
