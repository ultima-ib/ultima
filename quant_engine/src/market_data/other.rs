//use std::ops::Deref;
use chrono::NaiveDate;
pub use crate::util::macros::ClassName;
pub use class_name_derive::ClassName;
use std::rc::Rc;
use std::collections::HashMap;
use std::cell::RefCell;

pub struct MarketData {
    _object: Rc<RefCell<HashMap<String, Box<dyn MarketDataObject>>>>,
}

/*
impl Deref for MarketData {
    type Target = HashMap<String, Box<dyn MarketDataObject>>;
    fn deref(&self) -> &Self::Target {
        &self._object //pointer to Vec
    }
}
*/

pub trait MarketDataObject{
    //fn cls_name(&self) -> String; 
}

#[derive(ClassName, Debug)]
pub struct Ccy {
    pub label: String,
    pub holidays: Vec<NaiveDate>,
    }

impl Ccy {
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn holidays(&self) -> &Vec<NaiveDate> {
        &self.holidays
    }
}

impl MarketDataObject for Ccy {}
