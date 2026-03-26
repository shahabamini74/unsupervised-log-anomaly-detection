import pandas as pd
import sys

input_path = "/Users/shahab/Desktop/anomal_XXXX/2.parsing/output_parsing/HDFS.logl_structured.csv"

start = 0
chunksize = 100000

print(f"Checking for NaNs in {input_path}...")

try:
    for chunk in pd.read_csv(input_path, chunksize=chunksize):
        if chunk["Content"].isna().any():
            print("Found NaNs in Content column!")
            print(chunk[chunk["Content"].isna()])
            sys.exit(1)
            
        # Also check if ensure Content is always string
        # If it's not string, re.findall might fail
        non_strings = chunk[~chunk["Content"].apply(lambda x: isinstance(x, str))]
        if not non_strings.empty:
            print("Found non-string values in Content column!")
            print(non_strings)
            sys.exit(1)
            
        start += chunksize
        if start % 1000000 == 0:
            print(f"Processed {start} rows...")

    print("No NaNs or non-strings found.")

except Exception as e:
    print(f"Error: {e}")
