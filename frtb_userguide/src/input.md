# FRTB SA - Input format
Any calculation/function needs inputs. In case of FRTB SA the input is your portfolio sensitivities at trade level. An example could look like this: [Sensitivities](https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/Delta.csv).

```python
```

Table below will outline which columns are expected. If you are missing a required column you will get an error. 

# Data Source Config
In principle, you are free to enrich this structure with as many columns as you want (for example Desk, Legal Entity etc). You can either do this manually or use `from_config`. Define your config as such: datasource_config.toml . 

# Validate
Coming soon, calling .validate() on your dataset will return Ok if you've got all the columns in the correct format or Error if something is missing.

# Prepare
Calling .prepare() on your dataset assigns weights and other metrics required by the regulation, that only need to be assigned once. 