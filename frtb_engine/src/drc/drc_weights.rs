use polars::prelude::*;
use std::collections::BTreeMap;

pub(crate) fn dcr_nonsec_default_weights() -> DataFrame {
    let s0 = Series::new("AAA", &[0.005]);
    let s1 = Series::new("AA", &[0.02]);
    let s2 = Series::new("A", &[0.03]);
    let s3 = Series::new("BBB", &[0.06]);
    let s4 = Series::new("BAA", &[0.06]);
    let s5 = Series::new("BB", &[0.15]);
    let s6 = Series::new("BA", &[0.15]);
    let s7 = Series::new("B", &[0.30]);
    let s8 = Series::new("CCC", &[0.50]);
    let s9 = Series::new("CAA", &[0.50]);
    let s10 = Series::new("CA", &[0.50]);
    let s11 = Series::new("UNRATED", &[0.15]);
    let s12 = Series::new("NORATING", &[0.15]);
    let s13 = Series::new("DEFAULTED", &[1.]);

    let weights_list = Series::new(
        "Weights",
        &[s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13],
    );
    let cq = Series::new(
        "CreditQuality",
        &[
            "AAA",
            "AA",
            "A",
            "BBB",
            "Baa",
            "BB",
            "Ba",
            "B",
            "CCC",
            "CAA",
            "CA",
            "UNRATED",
            "NORATING",
            "DEFAULTED",
        ],
    );

    let s6 = Series::new("RiskClass", &["DRC_nonSec"; 14]);
    let s7 = Series::new("RiskCategory", &["DRC"; 14]);

    DataFrame::new(vec![weights_list, cq, s6, s7]).expect("Couldn't get DRC Non Sec Weights Frame")
    // We should not fail on default frame
}

#[cfg(feature = "CRR2")]
pub(crate) fn drc_nonsec_weights_frame_crr2() -> DataFrame {
    let s1 = Series::new("AA", &[0.005]);
    let weights_list = Series::new("WeightsCRR2", &[s1]);
    let cq = Series::new("CreditQuality", &["AA"]);

    let s6 = Series::new("RiskClass", &["DRC_nonSec"; 1]);
    let s7 = Series::new("RiskCategory", &["DRC"; 1]);

    DataFrame::new(vec![weights_list, cq, s6, s7]).expect("Couldn't get DRC Non Sec Weights Frame")
    // We should not fail on default frame
}

///CreditQuality_Seniority - Weight
pub(crate) fn _drc_secnonctp_weights_frame() -> DataFrame {
    let (key, weight): (Vec<String>, Vec<f64>) = drc_secnonctp_weights_raw()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v / 100.))
        .unzip();
    //let seniority_arr = Utf8Chunked::from_iter(key);
    let len = key.len();

    df![
        "Key" => key,
        "RiskWeightDRC" => weight,
        "RiskClass" => vec!["DRC_Sec_nonCTP"; len],
        "RiskCategory" => vec!["DRC"; len],
    ]
    .unwrap() // We must not fail on default frame
    .lazy()
    .with_column(concat_list([col("RiskWeightDRC")]).unwrap())
    .collect()
    .unwrap() // we should never fail on default frame
}

///CreditQuality_Seniority - Weight
pub(crate) fn drc_secnonctp_weights_frame() -> DataFrame {
    let (key, weight): (Vec<String>, Vec<f64>) = drc_secnonctp_weights_raw()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v / 100.))
        .unzip();
    let seniority_arr = Utf8Chunked::from_iter(key);
    df![
        "Key" => seniority_arr.into_series(),
        "RiskWeightDRC" => Series::from_vec("RiskWeight",weight),
    ]
    .unwrap()
}

/// 22.34
pub fn drc_secnonctp_weights_raw() -> BTreeMap<&'static str, f64> {
    BTreeMap::from([
        ("AAA_SENIOR", 1.2f64),
        ("AA+_SENIOR", 1.2),
        ("AA_SENIOR", 2.0),
        ("AA-_SENIOR", 2.4),
        ("A+_SENIOR", 3.2),
        ("A_SENIOR", 4.0),
        ("A-_SENIOR", 4.8),
        ("BBB+_SENIOR", 6.0),
        ("BBB_SENIOR", 7.2),
        ("BBB-_SENIOR", 9.6),
        ("BB+_SENIOR", 11.2),
        ("BB_SENIOR", 12.8),
        ("BB-_SENIOR", 16.0),
        ("B+_SENIOR", 20.0),
        ("B_SENIOR", 24.8),
        ("B-_SENIOR", 30.4),
        ("CCC+_SENIOR", 36.8),
        ("CCC_SENIOR", 36.8),
        ("CCC-_SENIOR", 36.8),
        ("D_SENIOR", 100.0),
        ("UNDERATD_SENIOR", 100.0),
        ("OTHER_SENIOR", 100.0),
        ("AAA_JUNIOR", 1.2),
        ("AA+_JUNIOR", 1.2),
        ("AA_JUNIOR", 2.4),
        ("AA-_JUNIOR", 3.2),
        ("A+_JUNIOR", 4.8),
        ("A_JUNIOR", 6.4),
        ("A-_JUNIOR", 9.6),
        ("BBB+_JUNIOR", 13.6),
        ("BBB_JUNIOR", 17.6),
        ("BBB-_JUNIOR", 26.4),
        ("BB+_JUNIOR	", 37.6),
        ("BB_JUNIOR", 49.6),
        ("BB-_JUNIOR", 60.0),
        ("B+_JUNIOR	", 72.0),
        ("B_JUNIOR", 84.0),
        ("B-_JUNIOR", 90.4),
        ("CCC+_JUNIOR", 100.0),
        ("CCC_JUNIOR", 100.0),
        ("CCC-_JUNIOR", 100.0),
        ("D_JUNIOR", 100.0),
        ("UNDERATD_JUNIOR", 100.0),
        ("OTHER_JUNIOR", 100.0),
        ("A-1_JUNIOR", 1.2),
        ("A-1_SENIOR", 1.2),
        ("P-1_JUNIOR", 1.2),
        ("P-1_SENIOR", 1.2),
        ("A-2_JUNIOR", 4.0),
        ("A-2_SENIOR", 4.0),
        ("P-2_JUNIOR", 4.0),
        ("P-2_SENIOR", 4.0),
        ("A-3_JUNIOR", 8.0),
        ("A-3_SENIOR", 8.0),
        ("P-3_JUNIOR", 8.0),
        ("P-3_SENIOR", 8.0),
        ("UNDERATD_JUNIOR", 100.0),
        ("UNDERATD_SENIOR", 100.0),
        ("AAA_NONSENIOR", 1.2),
        ("AA+_NONSENIOR", 1.2),
        ("AA_NONSENIOR", 2.4),
        ("AA-_NONSENIOR", 3.2),
        ("A+_NONSENIOR", 4.8),
        ("A_NONSENIOR", 6.4),
        ("A-_NONSENIOR", 9.6),
        ("BBB+_NONSENIOR", 13.6),
        ("BBB_NONSENIOR", 17.6),
        ("BBB-_NONSENIOR", 26.4),
        ("BB+_NONSENIOR", 37.6),
        ("BB_NONSENIOR", 49.6),
        ("BB-_NONSENIOR", 60.0),
        ("B+_NONSENIOR", 72.0),
        ("B_NONSENIOR", 84.0),
        ("B-_NONSENIOR", 90.4),
        ("CCC+_NONSENIOR", 100.0),
        ("CCC_NONSENIOR", 100.0),
        ("CCC-_NONSENIOR", 100.0),
        ("D_NONSENIOR", 100.0),
        ("UNDERATD_NONSENIOR", 100.0),
        ("OTHER_NONSENIOR", 100.0),
        ("A-1_NONSENIOR", 1.2),
        ("P-1_NONSENIOR", 1.2),
        ("A-2_NONSENIOR", 4.0),
        ("P-2_NONSENIOR", 4.0),
        ("A-3_NONSENIOR", 8.0),
        ("P-3_NONSENIOR", 8.0),
        ("UNDERATD_NONSENIOR", 100.0),
        ("D_NONSENIOR", 100.0),
        ("AAA_SUBORDINATE", 1.2),
        ("AA+_SUBORDINATE", 1.2),
        ("AA_SUBORDINATE", 2.4),
        ("AA-_SUBORDINATE", 3.2),
        ("A+_SUBORDINATE", 4.8),
        ("A_SUBORDINATE", 6.4),
        ("A-_SUBORDINATE", 9.6),
        ("BBB+_SUBORDINATE", 13.6),
        ("BBB_SUBORDINATE", 17.6),
        ("BBB-_SUBORDINATE", 26.4),
        ("BB+_SUBORDINATE", 37.6),
        ("BB_SUBORDINATE", 49.6),
        ("BB-_SUBORDINATE", 60.0),
        ("B+_SUBORDINATE", 72.0),
        ("B_SUBORDINATE", 84.0),
        ("B-_SUBORDINATE", 90.4),
        ("CC+_SUBORDINATE", 100.0),
        ("CC_SUBORDINATE", 100.0),
        ("CC-_SUBORDINATE", 100.0),
        ("D_SUBORDINATE", 100.0),
        ("UNDERATD_SUBORDINATE", 100.0),
        ("OTHER_SUBORDINATE", 100.0),
        ("A-1_SUBORDINATE", 1.2),
        ("P-1_SUBORDINATE", 1.2),
        ("A-2_SUBORDINATE", 4.0),
        ("P-2_SUBORDINATE", 4.0),
        ("A-3_SUBORDINATE", 8.0),
        ("P-3_SUBORDINATE", 8.0),
        ("UNDERATD_SUBORDINATE", 100.0),
        ("D_SUBORDINATE", 100.0),
    ])
}

/// DRC Seniority
/// as per 22.19
/// Neened to determine
pub fn with_drc_seniority(lf: LazyFrame) -> LazyFrame {
    let drc_sen = df![
        "SeniorityRank" => [5, 4, 3, 2, 1, 0],
        "RiskFactorType" => ["Covered", "SeniorSecured", "SeniorUnsecured", "Unrated", "NonSenior", "Equity"],
        "RiskClass" => vec!["DRC_nonSec"; 6],
        "RiskCategory" => vec!["DRC"; 6],
    ]
    .unwrap() // We must not fail on default frame
    .lazy();

    let join_on = [col("RiskClass"), col("RiskCategory"), col("RiskFactorType")];
    lf.join(drc_sen, join_on.clone(), join_on, JoinType::Left.into())
}
