use crate::util::statics::{derive_jargon, SPOT_CONVENVENTION};
use crate::util::dates::{spot_date};
pub use crate::util::macros::ClassName;
pub use class_name_derive::ClassName;
use  crate::market_data::{Ccy, MarketDataObject};
use chrono::{NaiveDate, Datelike, Weekday};
use std::rc::Rc;
use std::cell::RefCell;
use super::fx_spot::{Spot, FxSpot};

use chrono::Duration;

//#[derive(ClassName)]
pub struct FxPair <F> {
    pub label: String,
    pub spot: F,
    pub jargon: String,
    pub ccy1: Rc<RefCell<Ccy>>,
    pub ccy2: Rc<RefCell<Ccy>>,
    pub ccy_base: Rc<RefCell<Ccy>>,
}

impl<FX: FxSpot> FxPair<FX> {

    pub fn new (label: String, spot: FX, ccy1: Rc<RefCell<Ccy>>, ccy2: Rc<RefCell<Ccy>>, ccy_base: Rc<RefCell<Ccy>>) -> Self {
        let jargon = derive_jargon(&label);
        Self{label, spot, jargon, ccy1, ccy2, ccy_base}
    }

    pub fn obj_name() -> String {
        String::from("FxPair")
    }

    pub fn spot_duration(&self) -> Duration {
        if let Some(i) = SPOT_CONVENVENTION.get(&self.label) {
            Duration::days(*i as i64)
        } else {
            Duration::days(2)
        }
    }
    
    pub fn spot_from_horizon_date(&self, horizon: &NaiveDate) -> NaiveDate{
        let ccy1 = self.ccy1.borrow(); // https://users.rust-lang.org/t/creates-a-temporary-which-is-freed-while-still-in-use-again/29211
        let ccy2 = self.ccy2.borrow(); // expr assigned to a variable lives longer as same expr used as part of another expr
        let ccy3 = self.ccy_base.borrow();
        let duration = self.spot_duration(); //.num_days();
        spot_date(horizon, duration.num_days(), ccy1.label(), ccy1.holidays(), 
        ccy2.label(), ccy2.holidays(), ccy3.holidays())
    } 
}