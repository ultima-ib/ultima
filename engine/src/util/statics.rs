const FX_JARGON: [(&str, &str); 19] =
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
    ];

pub(crate) fn derive_jargon(key: &str) -> String {
    let mut jargon: Option<String> = None;
    for (pair, _jargon) in FX_JARGON.iter() {
        if pair == &key {
            jargon = Some(String::from(*_jargon));
            break
        }
    }
    if let None = jargon {
        jargon = Some(String::from(key));
    }
    jargon.unwrap()
}