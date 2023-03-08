use std::sync::Arc;

use ultibi_core::DataSet;

pub trait VisualDataSet {
    //fn ui(self: Arc<Self>);
    fn ui(self);
}

impl<T: DataSet + 'static> VisualDataSet for T {
    //fn ui(self: Arc<Self>){
    //    crate::run_server(self )
    //}
    fn ui(self){
        crate::run_server(self.into() )
    }
}