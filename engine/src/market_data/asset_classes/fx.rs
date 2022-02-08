use crate::util::statics::fx_jargon;
pub struct FxPair {
    pub label: String,
    pub spot: f64,
    pub jargon: String,
}

impl FxPair {
    pub fn new(label: String, spot: f64) -> Self {
        let mut jargon = None;
        let _fx_jargon = fx_jargon();
        for (pair, _jargon) in _fx_jargon.iter() {
            if pair == &label {
                jargon = Some(String::from(*_jargon));
                break
            }
        }
        if let None = jargon {
            jargon = Some(String::from(&label))
        }
        /*
        match fx_jargon().get(&*label){
            Some(v) => {jargon = String::from(*v)},
            None =>  {jargon = String::from(&label)}
        }; */
        Self{label, spot, jargon: jargon.unwrap()}
    }
}
trait FxPairDates {}