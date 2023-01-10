# Prepare

First, lets save the original table. You can get your data like this:

```python
{{#include ./examples/frtb_input.py:15:15}}
```

## Assign Weights

First part of the calculation(assuming you've got your exposures/sensitivities) is to assign weights to them as per the regulation. **Ultima assigns weights (and few other parameters) to your exposures as per the regulation**. Make sure you **assign weights before doing any computations**:

```python
{{#include ./examples/frtb_input.py:18:20}}
print(ds.frame())
```

You will get an error if you try to assign twice. Now, let's see what happened. We will need a little helper function:

```python
{{#include ./examples/frtb_input.py:22:33}}
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
