import narwhals as nw
import daft

df_native = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

df = nw.from_native(df_native)

result = df.select("a", nw.col("b") +1)
print(result)
