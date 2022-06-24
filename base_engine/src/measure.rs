use polars::prelude::*;

pub struct Measure<'a>{
    pub calculator: Box<dyn FnMut()->Expr + Send + Sync + 'a>,
    pub name: &'a str,
    pub req_columns: Vec<&'a str>,
    pub aggregation: Option<&'a str>,
}