use std::error::Error;

mod jobs;
mod market_data;
mod util;
mod products;

#[cfg(test)]
mod tests;

pub fn run(conf: Config) -> Result<(), Box<dyn Error>> {
    if conf.job_type == "MTM" {
        jobs::mtm()
    } else {
        Ok(())
    } 
}

pub struct Config {
    pub job_type: String
}

pub enum JobType {
    MTM
}
