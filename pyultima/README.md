# Python FRTB Aggregator

[This](https://ultimabi.uk/frtb-book/) is a free python binding to implementation of Fundamental Review of the Trading Book (FRTB SA). Supports multiple jurisdictions (BCBS, CRR2). Aggregates at any level of breakdown. Supports everything you'd need in a proper production environment: Filtering, Overrides and WhatIf. Blazingly fast, parallel.

Currently covering SA (Standardised Approach), with some limitations, it will be extended to cover IMA (Internal Models Approach) and SA CVA shortly. Checkout our [userguide](https://ultimabi.uk/frtb-book/). 

# Contributing
You will need `python`, `make`.

## Before commit

First:
`cd pyultima`

Linting:
`make pre-commit`

Tests\examples:
`make test`

Playround:
`make build`