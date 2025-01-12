import polars as pl

# # df = pd.read_csv("data/measurements_small.txt", sep=";", header=None, names=["station_name", "measurement"], engine="pyarrow")
# df = pl.read_csv("data/measurements_small.txt", sep=";")
# df.write_csv("measurements_small.csv")

# Define schema explicitly
schema = {
    "name": pl.Utf8,  # Column name should be a string (Utf8 in Polars)
    "temparature": pl.Float64  # Column temparature should be a float (Float64 in Polars)
}

# Read CSV with the defined schema
df = pl.read_csv("data/measurements_small.txt", separator=';', schema=schema, has_header=True, ignore_errors=True)
df = df.drop_nulls()
df.write_csv("measurements_small.csv")