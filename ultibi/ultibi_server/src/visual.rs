use std::sync::Arc;

use ultibi_core::DataSet;

pub trait VisualDataSet {
    fn ui(self: Arc<Self>);
}

impl<T: DataSet + 'static> VisualDataSet for T {
    fn ui(self: Arc<Self>){
        crate::run_server(self )
    }
}