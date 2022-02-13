use std::error::Error;

mod jobs;
mod market_data;
mod util;
mod products;

#[cfg(test)]
mod tests;

pub fn run(conf: Config) -> Result<(), Box<dyn Error>> {
    if conf.job_type == "MTM"{
        jobs::mtm()
    }
    
    Ok(())
}

pub struct Config<'a> {
    pub job_type: &'a str
}
