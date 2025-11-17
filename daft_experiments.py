from __future__ import annotations

import daft

# Example data with multiple modes
df = daft.from_pydict(
    {
        "values": [1, 2, 2, 3, 3, 4, 5, 5]  # we have modes for 2,3 5
    }
)

# Step 1: Count occurrences and collect
counts_df = df.groupby("values").agg(daft.col("values").count().alias("count"))
counts_result = counts_df.collect()

# Step 2: Find max count from the collected data
counts_dict = counts_result.to_pydict()
max_count = max(counts_dict["count"])

# Step 3: Filter for all modes
modes = [
    value
    for value, count in zip(counts_dict["values"], counts_dict["count"])
    if count == max_count
]

print(modes)
print(modes[0])
