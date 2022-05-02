use crate::util::statics::derive_jargon;
use chrono::NaiveDate;

use crate::util::dates::spot_date;

#[test]
fn test_derive_jargon() {
    let jargon = derive_jargon("GBPUSD");
    assert_eq!("Cable", jargon);
}

#[test]
fn test_spot_date(){

    let gbp = "GBP";
    let gbph: Vec<NaiveDate> =  vec![];
    let eur = "EUR";
    let eurh: Vec<NaiveDate> =  vec![NaiveDate::from_ymd(2009, 5, 1)];
    let usd = "USD";
    let usdh: Vec<NaiveDate> =  vec![NaiveDate::from_ymd(2009, 10, 12), 
    NaiveDate::from_ymd(2009, 11, 11)];
    let _try = "TRY";
    let _tryh: Vec<NaiveDate> =  vec![];
    let cad = "CAD";
    let cadh: Vec<NaiveDate> =  vec![NaiveDate::from_ymd(2009, 8, 3)];
    let aud = "AUD";
    let audh: Vec<NaiveDate> =  vec![];
    let nzd = "NZD";
    let nzdh: Vec<NaiveDate> =  vec![];
    let brl = "BRL";
    let brlh: Vec<NaiveDate> =  vec![];
    let mxn = "MXN";
    let mxnh: Vec<NaiveDate> =  vec![];

    //EURUSD
    let dt = NaiveDate::from_ymd(2009, 9, 28);
    let d: i64 = 2;
    let spot = spot_date(&dt, d, gbp, &gbph, 
        eur, &eurh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 9, 30), spot);

    //USDTRY  
    let dt = NaiveDate::from_ymd(2009, 2, 12);
    let d: i64 = 1;
    let spot = spot_date(&dt, d, usd, &usdh, 
        _try, &_tryh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 2, 13), spot);

/*  //GBPUSD from a weeked TBC
    let dt = NaiveDate::from_ymd(2009, 6, 20);
    let d: i64 = 2;
    let spot = spot_date(dt, d, ccy1, &h1, ccy2, &h2, &h3);
    println!("GBPUSD spot: {}", spot);
*/

    //EURUSD
    let dt = NaiveDate::from_ymd(2009, 4, 29);
    let d: i64 = 2;
    let spot = spot_date(&dt, d, eur, &eurh,
         usd, &usdh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 05, 04), spot);

    //USDCAD
    let dt = NaiveDate::from_ymd(2009, 7, 31);
    let d: i64 = 1;
    let spot = spot_date(&dt, d, usd, &usdh, 
        cad, &cadh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 08, 04), spot);

    //AUDNZD
    let dt = NaiveDate::from_ymd(2009, 10, 8);
    let d: i64 = 2;
    let spot = spot_date(&dt, d, aud, &audh,
         nzd, &nzdh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 10, 13), spot);

    //USDBRL, USDMXN
    let dt = NaiveDate::from_ymd(2009, 11, 10);
    let d: i64 = 2;
    let spot = spot_date(&dt, d, usd, &usdh, 
        brl, &brlh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 11, 12), spot);
    let spot = spot_date(&dt, d, usd, &usdh, 
        mxn, &mxnh, &usdh);
    assert_eq!(NaiveDate::from_ymd(2009, 11, 13), spot);
}