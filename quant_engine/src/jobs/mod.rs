use crate::market_data::fx::fx_spot::Spot;
use crate::market_data::{Ccy, fx};
use crate::products::Product;
use crate::products::fx_vanilla::FxVanilla;
use std::error::Error;
use std::str::FromStr;
use chrono::NaiveDate;
use std::rc::Rc;
use std::cell::RefCell;
use yearfrac::DayCountConvention;

// portfolio: T, market_data: T
pub(crate) fn mtm() -> Result<(), Box<dyn Error>> {
    let dcc = DayCountConvention::from_str("act/act")?;
    //let dcc = DayCountConvention::from_int(5)?;
    let t = format!("Anatoly {:?}", dcc);
    println!("{}", t);
    let gbp = Ccy {label: "GBP".into(), holidays: vec![NaiveDate::from_ymd(2015, 3, 14)]};
    let usd = Ccy {label: "USD".into(), holidays: vec![NaiveDate::from_ymd(2015, 3, 15)]};
    let gbp_p = Rc::new(RefCell::new( gbp ));
    let usd_p = Rc::new(RefCell::new( usd ));
    let _spot: Spot = fx::fx_spot::FxSpot::new(1.64);
    let p1 = fx::fx_pair::FxPair::new("GBPUSD".into(),_spot, Rc::clone(&gbp_p),
     Rc::clone(&usd_p), Rc::clone(&usd_p) );
    println!("{}", p1.jargon);
    let hor = NaiveDate::from_ymd(2015, 3, 13);
    let x = p1.spot_from_horizon_date(&hor);
    println!("{}", x);
    let _portfolio: Vec<Box<dyn Product>> = vec![];
    let opt_str: String = "FxVanilla,001,USD,EUR,10,15.1".into();
    let _l = opt_str.lines(); // use later for portfolio initialization
    //create a vanilla instance
    let v = FxVanilla::from_csv(&opt_str); 
    match v {
        Ok(_) => Ok(()),
        Err(ref e) => {
            println!("Couldn't parse trade: {}", opt_str);
            if let Some(source) = e.source() {
                println!("Error caused by: {}", source);
            }
            panic!("Error: {}", e);
        }
    }
        
    //println!("{:?}", v);
}