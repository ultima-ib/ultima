import InputExample from '!!raw-loader!../../examples/input.py'
import CodeBlock from '@theme/CodeBlock';
import {takeLines} from '../../src/utils'

# Assign Weights

## Prepare

Ultima assigns weights (and few other parameters) to your dataset as per the regulation. Weights assignment is kept separately from the computation because it needs to be done once only. Let's see what columns are assigned.

<CodeBlock language="py">{`${takeLines(InputExample, 16, 33)}
print(diff(ds.frame(), original))`}</CodeBlock>


At the time of writing this returns 6 new columns(names of the columns might change slightly but the meaning will always be the same):

| Column Name             | Explanation                                                                                                                  |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------|
| SensWeights             | List of weights, one for each tenor. Eg \[1,1,1,1,1\] for any Vega risk.                                                     |
| CurvatureRiskWeight     | Simply max of SensWeights if PnL Up or Down was provided, otherwise NULL. This is how we identify Curvature eligible trades. |
| SensWeightsCRR2         | Same as SensWeights but under CRR2 rules.                                                                                    |
| CurvatureRiskWeightCRR2 | Same logic as BCBS                                                                                                           |
| ScaleFactor             | DRC scailing: yearfrac between COB and MaturityDate                                                                          |
| SeniorityRank           | Mapping used internaly for DRC offsetting                                                                                    |
