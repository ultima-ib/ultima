#[test]
#[cfg(feature = "db")]
fn test_query_create() {
    use ultibi_core::filters::FilterE;

    let fltr = vec![
        vec![
            FilterE::Eq {
                field: "RiskCategory".into(),
                value: Some("Delta".into()),
            },
            FilterE::Neq {
                field: "RiskCategory".into(),
                value: Some("Vega".into()),
            },
        ],
        vec![FilterE::In {
            field: "RiskClass".into(),
            value: vec![Some("FX".into()), Some("Commodity".into()), None],
        }],
        vec![FilterE::NotIn {
            field: "CommodityLocation".into(),
            value: vec![Some("China".into()), Some("NY".into())],
        }],
        vec![FilterE::NotIn {
            field: "RiskFactor".into(),
            value: vec![Some("EURUSD".into()), Some("GBPEUR".into()), None],
        }],
    ];

    let expected = r#"SELECT * FROM delta
    WHERE ((RiskCategory = 'Delta') OR (RiskCategory != 'Vega' OR RiskCategory IS NULL))
    AND ((RiskClass = 'FX' OR RiskClass = 'Commodity' OR RiskClass IS NULL))
    AND ((CommodityLocation != 'China' AND CommodityLocation != 'NY' OR CommodityLocation IS NULL))
	AND ((RiskFactor != 'EURUSD' AND RiskFactor != 'GBPEUR'))"#
        .replace('\n', "")
        .replace('\t', " ")
        .replace("    ", " ");

    let res = ultibi_core::datasource::fltr_chain_to_sql_query("delta", &fltr);

    assert_eq!(expected, res);
}
