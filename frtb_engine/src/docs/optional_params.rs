//! # Overview
//! This document outlines optional parameters, their formats, possible values, and usage guidance.
//! Generally, there are two places where users can pass parameters.
//! # Contents
//!
//! * [Build Parameters](#buildparams)
//! * [Request Parameters](#some-key-differences)
//!
//! # Request Parameters
//! These are the optional_parameters of the incoming request.
//! `Parameter` | `Default` | `Parses into` | `Notes`
//! |-----|-----|-----|----------|
//! reporting_ccy|USD|str|Used to filter FX Risk Factors like ...CCY where CCY is the reporting_ccy
//! param_set|BCBS|str|One of: BCBS, CRR2. If CRR2, then `reporting_ccy` is EUR. Assumes prebuilt CRR2_Risk_Weights column, if column not present then defaults to BCBS.
//! base_csr_nonsec_tenor_rho|
//! base_csr_nonsec_diff_name_rho_per_bucket|
//! base_csr_nonsec_diff_basis_rho|
//! base_csr_nonsec_rating_gamma|
//! base_csr_nonsec_sector_gamma|
//! base_csr_ctp_tenor_rho|
//! base_csr_ctp_diff_name_rho_per_bucket|
//! base_csr_ctp_diff_basis_rho|
//! base_csr_ctp_rating_gamma|
//! base_csr_ctp_sector_gamma|
//!
//!
//!
//! # Build Parameters
//! These are build_parameters of the datasource_config.toml
//! Used for the prebuild of the DataSet
