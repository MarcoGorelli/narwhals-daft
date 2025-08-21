import narwhals as nw
import daft

df_native = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})

df = nw.from_native(df_native, eager_only=False, series_only=False)

result = df.select("a", nw.col("b") * nw.col("a"))
print(result.collect())

# checking the new operators add & sub are working
# nice, these break if I comment out their respective functions!
result = df.select("a", nw.col("b") + nw.col("a"))
print(result.collect())

result = df.select("a", nw.col("b") - nw.col("a"))
print(result.collect())

print(type(df))
