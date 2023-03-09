use std::sync::{Arc, RwLock};

use ultibi_core::DataSet;

pub trait VisualDataSet {
    //fn ui(self: Arc<Self>);
    fn ui(self, streaming: bool);
}

//<T: DataSet + 'static + ?Sized>
impl VisualDataSet for Arc<RwLock<dyn DataSet>> {
    //fn ui(self: Arc<Self>){
    //    crate::run_server(self )
    //}
    /// Spins up a server on localhost
    fn ui(self, streaming: bool) {
        crate::run_server(self, streaming)
    }
}
