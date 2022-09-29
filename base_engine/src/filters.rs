use polars::prelude::*;
use serde::{Deserialize, Serialize};

/// This type represents the convention.
/// Outer elements are AND, innder elements are OR
pub(crate) type AndOrFltrChain = Vec<Vec<FilterE>>;

/// Inner elements of each Filter are OR
/// Filters themselves(eg a Vec of Filters) are AND
/// 
/// TODO Currently works for Utf8 columns only. In the future 
/// parsing logic will be added based on column datatype
/// (Column, Value(s))
#[derive(Serialize, Deserialize, Debug, Hash, Clone)]
pub enum FilterE {
    /// On Same as In, but better for 1 field only
    Eq(String, String),
    Neq(String, String),
    In(String, Vec<String>),
    NotIn(String, Vec<String>),
}

impl FilterE{
    pub fn to_expr(&self)->Expr{
        match self {
            FilterE::Eq(c, v)          => fltr_eq_or_builder(c, v),
            FilterE::Neq(c, v)         => fltr_neq_or_builder(c, v),
            FilterE::In(c, vs)     => fltr_in_or_builder(c, vs),
            FilterE::NotIn(c, vs)  => fltr_not_in_or_builder(c, vs),
        }
    }
}

pub(crate) fn fltr_in_or_builder(c: &str, vs: &Vec<String>) -> Expr {
    let s = Series::new("filter", vs);
    col(c).cast(DataType::Utf8).is_in(s.lit())
}

pub(crate) fn fltr_not_in_or_builder(c: &str, vs: &Vec<String>) -> Expr {
    let s = Series::new("filter", vs);
    col(c).cast(DataType::Utf8).is_in(s.lit()).not()
}

pub(crate) fn fltr_eq_or_builder(c: &str, v: &str) -> Expr {
    col(c).cast(DataType::Utf8).eq(lit::<&str>(v))
}

pub(crate) fn fltr_neq_or_builder(c: &str, v: &str) -> Expr {
    col(c).cast(DataType::Utf8).neq(lit::<&str>(v))
}

pub(crate) fn fltr_chain(chain: &AndOrFltrChain) -> Option<Expr>{
    let mut res: Option<Expr> = None;

    // Loop from outer vec to inner
    for inner in chain {
        if !inner.is_empty() {
            let mut it = inner.iter();
            let mut inner_res = it.next()
                .unwrap()
                .to_expr();
            for f in it {
                inner_res = inner_res.or(f.to_expr())
            }

            res = match res {
                // If res is already Some we chain it
                Some(f) => Some(f.and(inner_res)),
                // If res is still None we set it to inner_res
                _ => Some(inner_res)
            }
        }
    }
    res
}
