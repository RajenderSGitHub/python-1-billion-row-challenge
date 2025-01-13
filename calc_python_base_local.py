
import time
from collections import Counter, defaultdict

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


def file_processing(filename, result):
    """
    Function to calculate avg, min and max temparature by the location
    """
    with open(filename, "r") as file:
        next(file)
        list = []
        for line in file:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            try:
                location, temperature = line.rsplit(',', 1)  # Ensure only two splits
                temperature = round(float(temperature), 2)
                if location not in result: 
                    result[location] = [temperature, temperature, temperature, 1]
                else:
                    result[location][0] = min(result[location][0], temperature)  # Update Min
                    result[location][1] = max(result[location][1], temperature)  # Update Max
                    result[location][2] += temperature  # Update Sum
                    result[location][3] += 1  # Increment Count
            except ValueError:
                print(f"Skipping malformed line: {line}")  # Log malformed lines
    return result


@timeit
def main() -> None:
    print(f"Processing begins...")
    result = defaultdict()
    filename = "measurements_500.csv"
    result = file_processing(filename, result)
    output_filename = "output/result_summary_500.txt"

    with open(output_filename, "w") as file:
        for key, value in result.items():
            avg = round(value[2] / value[3], 2)  # Calculate and round average
            # Create a tuple of location and values
            entry = (key, {"average": avg, "minimum": value[0], "maximum": value[1]})
            file.write(f"{entry}\n")

    print(f"Processing complete...")


if __name__ == "__main__":
    main()
