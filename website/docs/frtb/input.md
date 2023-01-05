import InputExample from '!!raw-loader!../../examples/input.py'
import CodeBlock from '@theme/CodeBlock';
import {takeLines} from '../../src/utils'

## Ultima DataSet

First step is to create a DataSet from your portfolio data. Think of a DataSet as a DataFrame with some special pre defined functions (eg "FX Delta Capital Charge").

## From Frame

In order to use our library you need to convert your portfolio into [polars](https://pola-rs.github.io/polars-book/user-guide/) [Dataframe](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html). You can either do this yourself(using any of the countless [IO](https://pola-rs.github.io/polars-book/user-guide/howcani/io/csv.html) operations supported. Note polars also supports [from_pandas](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.from_pandas.html) and [from_arrow](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.from_arrow.html)) or use a [config](#data-source-config).

<CodeBlock language="py">{`${takeLines(InputExample, 0, 10)}\nprint(df)`}</CodeBlock>


## Data Source Config

In principle, you are free to enrich this structure with as many columns as you want (for example Desk, Legal Entity etc). You can either do this manually or use `from_config`. Check out an example with explanations of each field: [datasource_config.toml](https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/datasource_config.toml).

<CodeBlock language="py">{`${takeLines(InputExample, 11, 14)}\nprint(df)`}</CodeBlock>


## Validate

If you are missing a required column you will get a runtime error during the execuiton of your request. Alternatively, coming soon, calling .validate() on your dataset will return Ok if you've got all the columns in the correct format or Error if something is missing.
