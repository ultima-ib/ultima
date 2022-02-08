use std::collections::HashMap;

pub (crate) fn fx_jargon<'a> () -> [(&'a str, &'a str); 19] {
        [
        ("EURUSD", "Euro-dollar"),
        ("USDJPY", "Dollar-yen"),
        ("EURJPY", "Euro-yen"),
        ("GBPUSD", "Cable"),
        ("EURGBP", "Euro-sterling"),
        ("USDCHF", "Dollar-swiss"),
        ("AUDUSD", "Aussie-dollar"),
        ("NZDUSD", "Kiwi-dollar"),
        ("USDCAD", "Dollar-cad"),
        ("EURNOK", "Euro-nokkie"),
        ("EURSEK", "Euro-stockie"),
        ("EURDKK", "Euro-danish"),
        ("EURHUF", "Euro-huff"),
        ("EURPLN", "Euro-polish"),
        ("USDTRY", "Dollar-try"),
        ("USDZAR", "Dollar-rand"),
        ("USDMXN", "Dollar-mex"),
        ("USDBRL", "Dollar-brazil"),
        ("USDSGD", "Dollar-sing")
    ]
}