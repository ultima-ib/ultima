use crate::util::statics::derive_jargon;

#[test]
fn test_derive_jargon() {
    let jargon = derive_jargon("GBPUSD");
    assert_eq!("Cable", jargon);
}