use engine;
use engine::FxPair;

fn main() {
    let _p = engine::market_data::asset_classes::fx::FxPair::new("USDEUR".into(), 1.32);
    let _p2 = FxPair::new("GBPUSD".into(), 1.48);
    println!("{}", _p2.jargon)
}
