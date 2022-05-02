use chrono::{NaiveDate, Datelike, Weekday};
use chrono::Duration;

use crate::util::statics::{THU_FRI_WEEKEND, FRI_SAT_WEEKEND};

pub fn spot_date(horizon: &NaiveDate, duration: i64, ccy1: &str, holiday1: &Vec<NaiveDate>, 
    ccy2: &str, holiday2: &Vec<NaiveDate>, holiday_base: &Vec<NaiveDate>) -> NaiveDate {
        
    let mut res: Vec<NaiveDate> = Vec::with_capacity(2);
    for (ccy, holiday) in [(ccy1, holiday1), (ccy2, holiday2)].iter() {
        let mut spot_date = *horizon;
        for d in 0..duration {
            if d==1 || *ccy=="USD"{ //T+1 USD holiday is an exception
                spot_date = today_or_next_good_business_date(spot_date, ccy, &vec![]);
            } else {
                spot_date = today_or_next_good_business_date(spot_date, ccy, holiday);
            }
            if d == 1 {
                if ["ARS", "MXN", "CLP"].contains(ccy) {
                    spot_date = today_or_next_good_business_date(spot_date, "USD", &holiday_base);
                }
            }
            spot_date += Duration::days(1);
        }
        res.push(spot_date);
    }
    let max_dt = *res.iter().max().unwrap();
    // Finally, make sure max is a g.b.d. in both ccy's and usd
    let max = today_or_next_good_business_date(max_dt, ccy1, holiday1);
    let max = today_or_next_good_business_date(max, ccy2, holiday2);
    let max = today_or_next_good_business_date(max, "USD", &holiday_base);
    max
}

pub fn today_or_next_good_business_date(dt: NaiveDate, ccy: &str,holiday: &Vec<NaiveDate>) -> NaiveDate {
    if holiday.contains(&dt) {
        today_or_next_good_business_date(dt + Duration::days(1), ccy,  holiday)
    } else if THU_FRI_WEEKEND.contains(&ccy) {
        match dt.weekday() {
            Weekday::Thu =>  today_or_next_good_business_date(dt + Duration::days(2), ccy,  holiday),
            Weekday::Fri => today_or_next_good_business_date(dt + Duration::days(1), ccy,  holiday),
            _ => dt
        }
    } else if FRI_SAT_WEEKEND.contains(&ccy) {
        match dt.weekday() {
            Weekday::Fri =>  today_or_next_good_business_date(dt + Duration::days(2), ccy,  holiday),
            Weekday::Sat => today_or_next_good_business_date(dt + Duration::days(1), ccy,  holiday),
            _ => dt
        }
    } else {
        match dt.weekday() {
            Weekday::Sat =>  today_or_next_good_business_date(dt + Duration::days(2), ccy,  holiday),
            Weekday::Sun => today_or_next_good_business_date(dt + Duration::days(1), ccy,  holiday),
            _ => dt
        }
    }
}