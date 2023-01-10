# Filtering

Often, one would want **filter** their data. May be you are just interested in number of one particular subset (eg a Book). Or, if you are investigating an issue, this can be very useful to narrow down the potential underlying root cause.

Note: you specify type of filter with `"op"` argument.

### Equal (Eq)

In this example we breakdown by `Country` and `RiskClass`. We also filter on `Desk == FXOptions`.

```python
{{#include ./examples/frtb_filters.py:1:22}}
print(result)
```

### Not Equial (Neq)

In this example we breakdown by `Country` and `RiskClass`. We also filter on `Desk != FXOptions`.

```python
{{#include ./examples/frtb_filters.py:23:37}}
print(result)
```

### In (In)

`In` is similar to `Eq`, but instead of one value you can provide multiple. In this example we filter on `Desk` to be `FXOptions` or `Rates`.

```python
{{#include ./examples/frtb_filters.py:38:51}}
print(result)
```

### Not In (NotIn)

`NotIn` is similar to `Neq`, but instead of saying that column value should not be equal to A, you can say that it should not be equal neither to A not to B. In this example we filter on `Desk` not being one of `FXOptions` or `Rates`.

```python
{{#include ./examples/frtb_filters.py:53:67}}
print(result)
```

### Combining Filters

You can combine filters as much as you want. In all the previous examples notice that `filter`. Is a nested list, ie a list of lists. **It is important to keep in mind that inner filters of each list will be joined as `OR`, while outer filters will be joined as `AND`**. For example, here we want `LegalEntity` to be equal to `EMEA` OR `Country` to be `UK`, AND we also want `RiskClass either FX or Equity`.

```python
{{#include ./examples/frtb_filters.py:68:87}}
print(result)
```
