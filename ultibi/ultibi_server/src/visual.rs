use std::sync::{Arc, RwLock};

use ultibi_core::DataSet;

pub trait VisualDataSet {
    fn ui(self);
}

//<T: DataSet + 'static + ?Sized>
impl VisualDataSet for Arc<RwLock<dyn DataSet>> {
    //fn ui(self: Arc<Self>){
    //    crate::run_server(self )
    //}
    /// Spins up a server on localhost
    fn ui(self) {
        crate::run_server(self)
    }
}
