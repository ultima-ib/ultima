# Assign Weights

First, lets save the original dataset. You can get your dataset like this:

```python
{{#include ./examples/frtb_example.py:16:16}}
```

## Prepare

**Ultima assigns weights (and few other parameters) to your dataset as per the regulation**. Weights assignment is kept separately from the computation because it needs to be done once only. Make sure you **assign weights before doing any computations**.

```python
{{#include ./examples/frtb_example.py:18:20}}
print(ds.frame())
```

Now, let's see what happened. We will need a little helper function:

```python
{{#include ./examples/frtb_example.py:21:33}}
print(diff(ds.frame(), original))
```

At the time of writing this returns 6 new columns(names of the columns might change slightly but the meaning will always be the same):

| Column Name               | Explanation                                                                                                                  |
|---------------------------|------------------------------------------------------------------------------------------------------------------------------|
| SensWeights               | List of weights, one for each tenor. Eg \[1,1,1,1,1\] for any Vega risk.                                                       |
|  CurvatureRiskWeight      | Simply max of SensWeights if PnL Up or Down was provided, otherwise NULL. This is how we identify Curvature eligible trades. |
|  SensWeightsCRR2          | Same as SensWeights but under CRR2 rules.                                                                                    |
|  CurvatureRiskWeightCRR2  | Same logic as BCBS                                                                                                           |
|  ScaleFactor              | DRC scailing: yearfrac between COB and MaturityDate                                                                          |
|  SeniorityRank            | Mapping used internaly for DRC offsetting                                                                                    |
