import daft
from daft import udf
import re
pattern = re.compile(r"[a-z]*[^a-z]*")

def to_titlecase_py(s):
    if s is None:
        return None
    s = s.lower()
    parts = pattern.findall(s)
    capitalized_parts = [p.capitalize() for p in parts if p]
    return "".join(capitalized_parts)

# checking that the actual function works
# print(to_titlecase_py("hello world"))
# print(to_titlecase_py("PYTHON PROGRAMMING"))
# print(to_titlecase_py("dAtA EnGiNeErInG"))
# print(to_titlecase_py(None))

# now wrapping the function as a udf so that it can be applied to a series
@udf(return_dtype="string") # need to tell daft the type of each element in the container, not the container itself
def to_titlecase_udf(xs):
    # xs will be a Series or list of strings
    res = []
    for s in xs:
        res.append(to_titlecase_py(s))
    return res

df = daft.from_pydict({
    "names": ["hello world", "PYTHON PROGRAMMING", "dAtA EnGiNeErInG"]
})

# apply the udf
df = df.with_column("title_case", to_titlecase_udf(df["names"]))
df.show()
