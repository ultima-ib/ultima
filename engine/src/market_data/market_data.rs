use std::ops::Deref;

pub struct MarketData<T> {
    _object: Rc<Vec<T>>,
}

impl Deref for MarketData {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self._object //pointer to Vec
    }
}