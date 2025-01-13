import time
from multiprocessing import Pool, cpu_count


def timeit(func):
    """
    Decorator to measure time taken.
    """
    def wrapper(*args, **kwargs): 
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Time taken processing {func.__name__} is: {(end_time - start_time) * 1000:.2f} ms")
        return result
    return wrapper


def process_chunk(lines):
    """
    Process a chunk of lines to calculate avg, min, and max temperature by location.
    """
    result = {}  # Use a regular dictionary
    for line in lines:
        line = line.strip()
        if not line:  # Skip empty lines
            continue
        try:
            location, temperature = line.rsplit(',', 1)
            temperature = round(float(temperature), 2)
            if location not in result:
                result[location] = [temperature, temperature, temperature, 1]
            else:
                result[location][0] = min(result[location][0], temperature)  # Update Min
                result[location][1] = max(result[location][1], temperature)  # Update Max
                result[location][2] += temperature  # Update Sum
                result[location][3] += 1  # Increment Count
        except ValueError:
            print(f"Skipping malformed line: {line}")
    return result


def merge_results(all_results):
    """
    Merge results from multiple processes.
    """
    final_result = {}
    for result in all_results:
        for location, values in result.items():
            if location not in final_result:
                final_result[location] = values
            else:
                final_result[location][0] = min(final_result[location][0], values[0])  # Min
                final_result[location][1] = max(final_result[location][1], values[1])  # Max
                final_result[location][2] += values[2]  # Sum
                final_result[location][3] += values[3]  # Count
    return final_result


@timeit
def main():
    print("Processing begins...")
    filename = "measurements_500.csv"
    output_filename = "output/result_summary_multip_500.txt"

    # Read the file and split it into chunks
    with open(filename, "r") as file:
        header = next(file)  # Skip header
        lines = file.readlines()

    num_processes = cpu_count()  # Use all available CPU cores
    chunk_size = len(lines) // num_processes
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Use multiprocessing to process chunks
    with Pool(processes=num_processes) as pool:
        all_results = pool.map(process_chunk, chunks)

    # Merge results from all processes
    final_result = merge_results(all_results)

    # Write the final result to a file
    with open(output_filename, "w") as file:
        for key, value in final_result.items():
            avg = round(value[2] / value[3], 2)  # Calculate and round average
            entry = (key, {"average": avg, "minimum": value[0], "maximum": value[1]})
            file.write(f"{entry}\n")

    print("Processing complete...")


if __name__ == "__main__":
    main()