use crate::util::statics::derive_jargon;
pub use crate::util::macros::ClassName;
pub use class_name_derive::ClassName;

#[derive(ClassName)]
pub struct FxPair {
    pub label: String,
    pub spot: f64,
    pub jargon: String,
}

impl FxPair {
    pub fn new(label: String, spot: f64) -> Self {
        let jargon = derive_jargon(&label);
        Self{label, spot, jargon}
    }
    pub fn cls_name(&self) -> String {
        self.class_name()
    }
}
trait FxPairDates {}
trait MarketDataObjectCreator {
    fn new_fxpair(label: String, spot: f64) -> FxPair;
}

struct MarketData {}

impl MarketDataObjectCreator for MarketData {
    fn new_fxpair(label: String, spot: f64) -> FxPair {
        let jargon = derive_jargon(&label);
        FxPair{label, spot, jargon}
    }
}