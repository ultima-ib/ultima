use crate::util::statics::{derive_jargon, SPOT_CONVENVENTION};
pub use crate::util::macros::ClassName;
pub use class_name_derive::ClassName;
use  super::{MarketDataObject, Ccy};
use chrono::NaiveDate;

use chrono::Duration;

#[derive(ClassName)]
pub struct FxPair {
    pub label: String,
    pub spot: f64,
    pub jargon: String,
    pub spot_duration: Duration,
}

impl FxPair {

    pub fn new(label: String, spot: f64) -> Self {
        let spot_duration = if let Some(i) = SPOT_CONVENVENTION.get(&label) {
            Duration::days(*i as i64)
        } else {
            Duration::days(2)
        };
        let jargon = derive_jargon(&label);
        Self{label, spot, jargon, spot_duration}
    }

    //pub fn cls_name(&self) -> String {
    //    self.class_name()
    //}

    //pub fn cls_name() -> String {
    //    FxPair::class_name()
    //}

    pub fn name() -> String {
        String::from("FxPair")
    }

    pub fn spot_from_horizon_date(horizon: NaiveDate){

    }
}

impl MarketDataObject for FxPair {}

trait FxPairDates {}
trait MarketDataObjectCreator {
    fn new_fxpair(label: String, spot: f64) -> FxPair;
}

/*
struct MarketData {}

impl MarketDataObjectCreator for MarketData {
    fn new_fxpair(label: String, spot: f64) -> FxPair {
        FxPair::new(label, spot)
    }
} */