use crate::util::statics::fx_jargon;
pub struct FxPair {
    pub label: String,
    pub spot: f64,
    pub jargon: String,
}

impl FxPair {
    pub fn new(label: String, spot: f64) -> Self {
        let jargon: String;
        match fx_jargon().get(&*label){
            Some(v) => {jargon = String::from(*v)},
            None =>  {jargon = String::from(&label)}
        };
        Self{label, spot, jargon}
    }
}
trait FxPairDates {}