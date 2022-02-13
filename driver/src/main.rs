use engine::{Config};
use std::{process};

fn main() {
    let conf: Config = Config{job_type: "MTM"};
    if let Err(e) = engine::run(conf){
        eprintln!("Application error: {}", e);
        process::exit(1);
    };
    }
