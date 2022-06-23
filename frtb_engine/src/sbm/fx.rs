use polars::prelude::*;

pub fn example () -> Expr {
    let s: &mut [Expr] = &mut [col("DeltaSpot"), col("Delta0.25Y")];
    //1. Look up cache for given request
    //   If found, return series as Expr
    //2. If not found - calculate
    //   Save to cache 
    fn pa_fa(s: &mut [Series])->Result<Series>{
        let u = s[2].f64()?;
        let n = s[1].f64()?;
        let n_iter = n.into_iter();
    
        let c: Vec<f64> = n_iter.zip(u.into_iter()).map(
        // ignore this line   let c: Vec<f64> = n_iter.zip(u).map( 
            |(n,u)|{
                n.unwrap().powf(1.777)*u.unwrap().sqrt()
            }
        ).collect();
    
        Ok(Series::new("Ant", c))
    }

    let o = GetOutput::from_type(DataType::Float64);
    lit::<f64>(0.0).apply_many(pa_fa, s, o) 
}