# Calculation

Specify what you want to calculate

```python
{{#include ./examples/frtb_example.py:38:58}}
```

Convert the dict into Ultima's Aggregation Request

```python
{{#include ./examples/frtb_example.py:60:60}}
print(request)
```

Finally, calculate and see results:

```python
{{#include ./examples/frtb_example.py:63:63}}
print(result)
print("Type: ", type(result))
```
