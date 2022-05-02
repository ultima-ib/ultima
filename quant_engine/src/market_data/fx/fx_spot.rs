/// To be moved to general Spot
use std::collections::HashMap;
pub struct Spot {
    val: f64
}

pub trait FxSpot{
    fn new(val: f64) -> Self;
    fn obj_name() -> String;
}

impl FxSpot for Spot {
    fn new(val: f64) -> Self {
        Self{val}
    }

    fn obj_name() -> String {
        String::from("FxSpot")
    }
}