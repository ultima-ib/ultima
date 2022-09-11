use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::ops::Index;

/// Inner elements of each Filter are OR
/// Filters themselves are AND
/// Inside single Filter expect first element(X) of (X ,.) to be from the same file
/// ie OR "ON" filters have to be on columns of the same file
#[derive(Serialize, Deserialize, Debug, Hash, Clone)]
pub enum FilterE {
    /// On Same as In, but better for 1 field only
    Eq(Vec<(String, String)>),
    Neq(Vec<(String, String)>),
    In(Vec<(String, Vec<String>)>),
    NotIn(Vec<(String, Vec<String>)>),
}
impl Literal for FilterE {
    fn lit(self)->Expr{
        match self {
            FilterE::Eq(f)          => fltr_eq_or_builder(&f),
            FilterE::Neq(f)         => fltr_neq_or_builder(&f),
            FilterE::In(f)     => fltr_in_or_builder(&f),
            FilterE::NotIn(f)  => fltr_not_in_or_builder(&f),
        }
    }
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
    let mut e = col(a).is_in(s.lit());

    for (i, j) in _f.iter().skip(0) {
        let s = Series::new("filter", j);
        e = e.or(col(i).is_in(s.lit()));
    }
    e
}

pub(crate) fn fltr_not_in_or_builder(_f: &Vec<(String, Vec<String>)>) -> Expr {
    let (a, b) = _f.index(0);
    let s = Series::new("filter", b);
    let mut e = col(a).is_in(s.lit()).not();

    for (i, j) in _f.iter().skip(0) {
        let s = Series::new("filter", j);
        e = e.or(col(i).is_in(s.lit()).not());
    }
    e
}

/// To add categorical here
pub(crate) fn fltr_eq_or_builder(_f: &Vec<(String, String)>) -> Expr {
    let (a, b) = _f.index(0);
    let mut e: Expr = col(a).eq(lit::<&str>(b));
    for (i, j) in _f.iter().skip(1) {
        e = e.or(col(i).eq(lit::<&str>(j)))
    }
    e
}

/// To add categorical here
pub(crate) fn fltr_neq_or_builder(_f: &Vec<(String, String)>) -> Expr {
    let (a, b) = _f.index(0);
    println!("a: {}, b: {}", a, b);
    let mut e: Expr = col(a).neq(lit::<&str>(b));
    for (i, j) in _f.iter().skip(1) {
        e = e.or(col(i).neq(lit::<&str>(j)))
    }
    e
}
