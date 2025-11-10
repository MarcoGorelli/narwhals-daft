# Narwhals-daft

Narwhals plugin for [Daft](https://github.com/Eventual-Inc/Daft)!

See [Narwhals](https://narwhals-dev.github.io/narwhals/) for Narwhals documentation and API.
This plugin allows Narwhals to accept Daft inputs (in addition to all the inputs it already
accepts).

## Installation

```console
pip install narwhals-daft
```

## Example

```py
import daft
import narwhals as nw
from narwhals.typing import IntoFrameT

data = {
    "animal": [
        "penguin",
        "dodo",
        "beluga",
        "narwhal",
        "cat",
        "dog",
        "hamster",
        "falcon",
    ],
    "awesomeness": [7, 5, 8, 15, 5, 4, 3, 9],
}
df_daft = daft.from_pydict(data)
df = nw.from_native(df_daft)
result = df.with_columns(
    relative_awesomeness=nw.col("awesomeness") / nw.col("awesomeness").max()
).filter(nw.col("relative_awesomeness") > 0.5)
print(result.to_native().collect())
```

```console
╭─────────┬─────────────┬──────────────────────╮
│ animal  ┆ awesomeness ┆ relative_awesomeness │
│ ---     ┆ ---         ┆ ---                  │
│ String  ┆ Int64       ┆ Float64              │
╞═════════╪═════════════╪══════════════════════╡
│ beluga  ┆ 8           ┆ 0.5333333333333333   │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ narwhal ┆ 15          ┆ 1                    │
├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ falcon  ┆ 9           ┆ 0.6                  │
╰─────────┴─────────────┴──────────────────────╯

(Showing first 3 of 3 rows)
```
