import narwhals as nw
from narwhals.utils import Version
import daft
from narwhals_daft.dataframe import DaftLazyFrame

df_native = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
df_compliant = DaftLazyFrame(df_native, version=Version.MAIN)

df = nw.from_native(df_compliant)
result = df.select("a", nw.col("b") * nw.col("a"))
print(result.collect())
