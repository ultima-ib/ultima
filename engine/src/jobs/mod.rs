use crate::market_data::{fx};

// portfolio: T, market_data: T
pub(crate) fn mtm() {
    let _p2 = fx::FxPair::new("GBPUSD".into(), 1.48);
    println!("{}", _p2.jargon);
    //println!("{}", _p2.cls_name());
    let animals: Vec<Box<dyn animal>>;
    let user_input = "Cat,Persik";
    let user_input = user_input.split(",");
    match user_input.nth(0) {
        "Cat" => animals.push(Cat (user_input.nth(0))),
        _ => animals.push(Dog(user_input.nth(0)))
    } 

}

trait animal{}

struct Dog{
    age: u8,
}

impl animal for Dog{}

struct Cat {
    name: String,
}
impl animal for Cat{}