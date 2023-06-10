use polars::prelude::*;
use serde::{Deserialize, Serialize};

/// This type represents the convention.
/// Outer elements are AND, innder elements are OR
pub type AndOrFltrChain = Vec<Vec<FilterE>>;

/// Inner elements of each Filter are OR
/// Filters themselves(eg a Vec of Filters) are AND
///
///
/// (Column, Value(s))
///
///
#[derive(Serialize, Deserialize, Debug, Hash, Clone, Eq, PartialEq)]
#[serde(tag = "op")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum FilterE {
    /// On Same as In, but better for 1 field only
    Eq {
        field: String,
        value: Option<String>,
    },
    Neq {
        field: String,
        value: Option<String>,
    },
    In {
        field: String,
        value: Vec<Option<String>>,
    },
    NotIn {
        field: String,
        value: Vec<Option<String>>,
    },
}

impl FilterE {
    pub fn to_expr(&self) -> Expr {
        match self {
            FilterE::Eq { field: c, value: v } => fltr_eq(c, v),
            FilterE::Neq { field: c, value: v } => fltr_neq(c, v),
            FilterE::In {
                field: c,
                value: vs,
            } => fltr_in(c, vs),
            FilterE::NotIn {
                field: c,
                value: vs,
            } => fltr_not_in(c, vs),
        }
    }
}

pub(crate) fn fltr_in(c: &str, vs: &Vec<Option<String>>) -> Expr {
    let s = Series::new("filter", vs);
    col(c).cast(DataType::Utf8).is_in(s.lit())
}

pub(crate) fn fltr_not_in(c: &str, vs: &Vec<Option<String>>) -> Expr {
    let s = Series::new("filter", vs);
    col(c).cast(DataType::Utf8).is_in(s.lit()).not()
}

pub(crate) fn fltr_eq(c: &str, v: &Option<String>) -> Expr {
    match v {
        None => col(c).is_null(),
        Some(v) => col(c).cast(DataType::Utf8).eq(lit::<&str>(v)),
    }
}

pub(crate) fn fltr_neq(c: &str, v: &Option<String>) -> Expr {
    match v {
        None => col(c).is_not_null(),
        Some(v) => col(c).cast(DataType::Utf8).neq(lit::<&str>(v)),
    }
}

pub fn fltr_chain(chain: &AndOrFltrChain) -> Option<Expr> {
    let mut res: Option<Expr> = None;

    // Loop from outer vec to inner
    for inner in chain {
        if !inner.is_empty() {
            let mut it = inner.iter();
            let mut inner_res = it.next().unwrap().to_expr();
            for f in it {
                inner_res = inner_res.or(f.to_expr())
            }

            res = match res {
                // If res is already Some we chain it
                Some(f) => Some(f.and(inner_res)),
                // If res is still None we set it to inner_res
                _ => Some(inner_res),
            }
        }
    }
    res
}
