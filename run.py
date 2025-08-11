import narwhals as nw
from narwhals.utils import Version
import daft
from narwhals_daft.dataframe import DaftLazyFrame

df_native = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
df_compliant = DaftLazyFrame(df_native, version=Version.MAIN)

# TODO we should't have to do the step above! how can we plug into nw without that extra step? 
df = nw.from_native(df_compliant)
result = df.select("a", nw.col("b") * nw.col("a"))
print(result.collect())

# checking the new operators add & sub are working
# nice, these break if I comment out their respective functions!
result = df.select("a", nw.col("b") + nw.col("a"))
print(result.collect())

result = df.select("a", nw.col("b") - nw.col("a"))
print(result.collect())

# `nw.from_native` returns the correct type <class 'narwhals.dataframe.LazyFrame>
print(type(df))

print('testing CI')
