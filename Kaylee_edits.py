import pandas as pd
import os
import numpy as np

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