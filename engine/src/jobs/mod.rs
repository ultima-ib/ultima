use crate::market_data::{fx};

// portfolio: T, market_data: T
pub(crate) fn mtm() {
    let _p2 = fx::FxPair::new("GBPUSD".into(), 1.48);
    println!("{}", _p2.jargon);
    println!("{}", _p2.cls_name());
}