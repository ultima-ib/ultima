use polars::prelude::*;
use std::collections::HashMap;

pub(crate) fn drc_nonsec_weights() -> HashMap<String, Expr> {
    HashMap::from([
        (
            "^(?i)AAA$".to_string(),
            Series::new("", &[0.005]).lit().list(),
        ),
        (
            "^(?i)AA$".to_string(),
            Series::new("", &[0.02]).lit().list(),
        ),
        ("^(?i)A$".to_string(), Series::new("", &[0.03]).lit().list()),
        (
            "^(?i)BBB$".to_string(),
            Series::new("", &[0.06]).lit().list(),
        ),
        (
            "^(?i)Baa$".to_string(),
            Series::new("", &[0.06]).lit().list(),
        ),
        (
            "^(?i)BB$".to_string(),
            Series::new("", &[0.15]).lit().list(),
        ),
        (
            "^(?i)Ba$".to_string(),
            Series::new("", &[0.15]).lit().list(),
        ),
        ("^(?i)B$".to_string(), Series::new("", &[0.30]).lit().list()),
        (
            "^(?i)CCC$".to_string(),
            Series::new("", &[0.50]).lit().list(),
        ),
        (
            "^(?i)Caa$".to_string(),
            Series::new("", &[0.50]).lit().list(),
        ),
        (
            "^(?i)Ca$".to_string(),
            Series::new("", &[0.50]).lit().list(),
        ),
        (
            "^(?i)UNRATED$".to_string(),
            Series::new("", &[0.15]).lit().list(),
        ),
        (
            "^(?i)NORATING$".to_string(),
            Series::new("", &[0.15]).lit().list(),
        ),
        (
            "^(?i)DEFAULTED$".to_string(),
            Series::new("", &[1.]).lit().list(),
        ),
    ])
}

#[cfg(feature = "CRR2")]
pub(crate) fn drc_nonsec_weights_crr2() -> HashMap<String, Expr> {
    HashMap::from([(
        "^(?i)AA$".to_string(),
        Series::new("", &[0.005]).lit().list(),
    )])
}

//pub(crate) fn drc_secnonctp_weights() -> HashMap<String, Expr> {
//    drc_secnonctp_weights_raw().into_iter().map(|(k, v)|(k.to_string(), Series::new("", &[v/100.]).lit().list())).collect()
//}

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
pub fn drc_secnonctp_weights_raw() -> HashMap<&'static str, f64> {
    HashMap::from([
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
