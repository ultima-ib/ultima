use super::{Product, Result, BadProductInputError};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct FxVanilla {
    trade_id: String,
    buy_ccy: String,
    sell_ccy: String,
    buy_ammount: f64,
    sell_ammount: f64,
    //expiry_date: T
}

impl Product for FxVanilla{
    fn from_csv(x: &String) -> Result<Self> {
        let input = x.split(',')
        .collect::<Vec<_>>();

        if input.len() != 6{
            return Err(BadProductInputError::InvalidLine)
        }

        Ok(Self{
            trade_id: input[1].into(),
            buy_ccy: input[2].into(),
            sell_ccy: input[3].into(),
            buy_ammount: input[4].parse::<f64>()?,
            sell_ammount: input[5].parse::<f64>()?,
        })
    }
}



