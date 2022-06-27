use polars::prelude::*;
use std::{fmt::Debug, collections::HashMap};
use derivative::Derivative;

type O = Option<HashMap<String, String>>;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Measure<'a>{
    pub name: &'a str,
    /// Main function which performs the calculation
    #[derivative(Debug="ignore")]
    pub calculator: Box<dyn FnMut()->Expr + Send + Sync + 'a>,
    ///Bespoke measures assume presence of these columns
    pub req_columns: Vec<&'a str>,
    ///this field is to restrict aggregation option to certain type only
    ///for example where it makes sence to aggregate with "first" and not "sum"
    pub aggregation: Option<&'a str>,
    // List of files
    //pub file: Vec<File>,
}

pub trait Paramset {}

#[derive(Debug)]
pub enum File {
    F1,
    F2,
    F3
}