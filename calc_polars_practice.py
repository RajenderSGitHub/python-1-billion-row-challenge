import polars as pl 
import time

def timeit(func):
    """
    Decorator to measure time taken 
    """
    def wrapper(*args, **kwargs): 
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f" Time taken processing {func.__name__} is : {(end_time-start_time) * 1000:.2f} ms ")
        return result
    return wrapper


def _generate_report(df) -> pl.DataFrame: 
    """
    Function to add columns avg, max and min temparature by Location
    """
    df_result = df.groupby("name").agg(
                        pl.mean('temparature').alias("avg_temparature"),
                        pl.min('temparature').alias("min_temparature"),
                        pl.max('temparature').alias("max_temparature")
                )
    return df_result

@timeit
def main():
    print(f"main processing begins..")
    # Read a CSV file
    df = pl.read_csv("measurements_500.csv")
    print(f"DataFrame shape: {df.shape}")

    # Generate a report
    df_result = _generate_report(df)
    print(f"Result DataFrame: {df_result.shape}")
    print(df_result.head())
    df_result.write_csv("output/measurements_small_polars.csv")
    print(f"Processing complete....")

if __name__ == "__main__":
    main()
