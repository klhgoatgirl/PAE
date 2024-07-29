import pandas as pd
import os
import numpy as np


# CHATGPT
'''
def validate_csv(file_path, max_file_size_mb=10):
    try:
        # Check file size
        if os.path.getsize(file_path) > max_file_size_mb * 1024 * 1024:
            return False
        
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Check if there's only one column
        if df.shape[1] != 1:
            return False
        
        # Check if there are exactly 8760 rows
        if df.shape[0] != 8760:
            return False
        
        # Check if all values are numeric and within 64-bit limits
        column_data = df.iloc[:, 0]
        if not pd.api.types.is_numeric_dtype(column_data):
            return False
        
        # Check for 64-bit limits
        # 64-bit float range: approximately -1.8e308 to 1.8e308
        # 64-bit integer range: -2^63 to 2^63 - 1
        min_int64 = np.iinfo(np.int64).min
        max_int64 = np.iinfo(np.int64).max
        min_float64 = np.finfo(np.float64).min
        max_float64 = np.finfo(np.float64).max
        
        if not column_data.apply(lambda x: (isinstance(x, (int, float)) and 
                                            (min_int64 <= x <= max_int64 or min_float64 <= x <= max_float64))).all():
            return False
        
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# Usage example
file_path = 'C:\Users\kaylee.hudson\Downloads\Year 1 Load.csv'
is_valid = validate_csv(file_path)
print("CSV is valid:", is_valid)
'''

# MY OWN CODE

def validate_csv(csv_file, file_size_max = 10):
    try:
        # Check file size
        if os.path.getsize(csv_file) > file_size_max * 1024 * 1024:
            return False

        # read CSV file
        df = pd.read_csv(csv_file)

        # Convert CSV file to numpy array
        df_np = df.to_numpy()

        # Check if there is one column and 8760 rows
        if df_np.shape[1] != 1 or df_np.shape[0] != 8760:
            return False

        # Check if all values are numeric and within 64-bit limits
        for val in df_np[0]:
            if (not (isinstance(val, int))) or (not (isinstance(val, float))):
                return False
            elif len(bin(val)[2:]) > 64:
                return False

        return True

    except:
        print("An error occurred.")
        return False

# Usage
csv_file = 'C:\Users\kaylee.hudson\Downloads\Year 1 Load.csv'
is_valid = validate_csv(csv_file)
if(is_valid):
    print("File is valid.")
else:
    print("File is invalid.")