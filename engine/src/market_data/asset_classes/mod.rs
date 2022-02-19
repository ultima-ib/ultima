pub mod fx;
use chrono::NaiveDate;
pub use crate::util::macros::ClassName;
pub use class_name_derive::ClassName;

pub struct MarketData {

}

#[derive(ClassName)]
pub struct Ccy {
    _holidays: Vec<NaiveDate>,
    }

pub trait MarketDataObject{
    //fn cls_name(&self) -> String; 
}