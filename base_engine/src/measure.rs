use polars::prelude::*;

pub struct Measure<'a>{
    pub calculator: Box<dyn FnMut()->Expr + Send + Sync + 'a>,
    pub name: &'a str,
    pub req_columns: Vec<&'a str>,
    //this field is to restrict aggregation option to certain type only
    //for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'a str>,
}