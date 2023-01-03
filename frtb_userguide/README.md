# Introduction

Checkout our userguide and [website](https://ultimabi.uk/).

# Contributing
You will need `python`, `make`, `wget` and [`mdbook`](https://github.com/rust-lang/mdBook/releases).

## Before commit

*All examples must be plased into src/examples and refered to from .md files as `{#include ./examples/...}`*

First:
`cd frtb_userguide`

While developing:
`mdbook watch --open`

Download data:
`make data`

Linting:
`make fmt`

Tests\examples:
`make run`
