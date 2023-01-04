# FRTB SA - Input format

Any calculation/function needs inputs. In case of FRTB SA the **input is your portfolio sensitivities at trade level**. Currently expected format is like this: [Sensitivities.csv](https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/Delta.csv). This format is very similar to the industry standard [CRIF](https://www.isda.org/a/aBzTE/The-Future-of-Risk-Capital-and-Margin.pdf). Coming soon full CRIF support.

Note that the **[FRTB SA Paper](https://www.bis.org/bcbs/publ/d457.pdf)** actually well defines what a "RiskFactor" is and the formulas for "Sensitivities". See paragraphs `21.8-21.19` and `21.19-21.26` respectively.

# Expected Columns

Table below will outline which columns are expected, the useage and meaning behind them.
