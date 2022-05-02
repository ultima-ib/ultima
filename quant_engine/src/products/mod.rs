pub mod fx; // bring in fx into scope
pub use fx::*; // making fx availiable to super mods

mod errors;
pub use errors::*;

type Result<T> = std::result::Result<T, BadProductInputError>;

/// expected to be implemented for every struct representing a financial product
pub trait Product {
    fn from_csv(x: &String) -> Result<Self> where Self: Sized;
    fn mtm(&self) -> f64 {1.0}
}

impl dyn Product{
    fn from_str(trade: String, x: String) -> Result<Box<dyn Product>>{
        let trade_type = trade.split_once(",")
            .ok_or(BadProductInputError::InvalidLine).unwrap();

        match trade_type.0 {
            /* Other options
            "FxVanilla" => fx::FxVanilla::from_comma_separated(&x).map(|p| -> Box<dyn Product> { Box::new(p)}),
            "FxVanilla" => match fx::FxVanilla::from_comma_separated(&x){
                Ok(p) => Ok(Box::new(p)),
                Err(e) => Err(e)
            },  */

            "FxVanilla" => Ok( Box::new( fx::FxVanilla::from_csv(&x)? )) ,
            
            _ => Err(BadProductInputError::ProductTypeDoesNotExist),
        }
    }
}

pub struct Portfolio{
    trades: Vec<Box<dyn Product>>
}

use std::error;
use std::fmt;
use std::num::ParseFloatError;

#[derive(Debug)]
pub enum BadProductInputError {
    InvalidLine,
    ProductTypeDoesNotExist,
    Parse(ParseFloatError)
}

impl From<ParseFloatError> for BadProductInputError {
    fn from(err: ParseFloatError) -> BadProductInputError {
        BadProductInputError::Parse(err)
    }
}

impl error::Error for BadProductInputError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            BadProductInputError::InvalidLine => None,
            BadProductInputError::Parse(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for BadProductInputError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BadProductInputError::InvalidLine =>
                write!(f, "Invalid number of args for this "),
            BadProductInputError::Parse(ref err) =>
                write!(f, "the provided string could not be parsed as float: {}", err),
            _ => 
                write!(f, "Other error"),
        }
    }
}


