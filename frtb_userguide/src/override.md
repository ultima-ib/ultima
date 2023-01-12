# Overrides

You can temporarily override any value in your dataset. This can be very useful to perform analysis. An override consists of three fields:

1. The **Field** (ie the column where you seek to override values).
   Note: at the time of writing you can only override **bool, str, number, list of numbers** types of columns.
1. The New **Value**.
1. The **[Filters](./filters.md)**. ie when to override the existing with the new value.

Let's do a couple of examples. Notice that under [BCBS](https://www.bis.org/bcbs/publ/d457.pdf) *page `59` table `2`*. the default risk weight is almost the same as under [CRR2](https://www.eba.europa.eu/regulation-and-policy/single-rulebook/interactive-single-rulebook/108763). Assuming that AA is Credit Quality Step 1, if we change the risk weight to `0.005` we would expect to get the same outcome. Let's test this out:

```python
{{#include ./examples/frtb_overrides.py:7:30}}
print(result)
```

Note that SensWeights is a list of numbers column (because of multiple tenors) and therefore we write it as a list `[0.005]`.

Let's compare this to a run **without overrides** but with **`jurisdiction` == `CRR2`**:

```python
{{#include ./examples/frtb_overrides.py:30:45}}
print(result)
```

Results should match!
