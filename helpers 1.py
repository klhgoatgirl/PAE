from itertools import accumulate
import pandas as pd
import streamlit as st
from sqlalchemy.sql import text
import os
import pyarrow as pa
from pyarrow import parquet as pq


def calc_battery(data, hours, fixed_gen):
    '''
    data is a pandas dataframe with columns for energy consumption and PV output
    return dayMaxes a pandas dataframe of one year of data by hour
    '''
    # Add "n" rows to the end of sheet for forward looking calcs
    firstRows = data.iloc[:hours]
    data = pd.concat([data, firstRows], ignore_index=True)

    # Compute the net energy used/generated for each hour in the data
    data['Base Gen'] = (fixed_gen * -1)
    data['Net Energy'] = data.sum(axis=1)

    # define custom accumulator function to floor the sum to 0.
    # This is done to calculate the max battery size required without overcharging battery
    #TODO: This function is returning negative values when hours of resiliency is set to 1
    def customAcc(x):
        battCap = lambda acc, val: (acc + val) if (acc + val) > 0 else 0
        return max(accumulate(x, battCap))

    # Create a forward looking rolling window to apply the function to
    indexer = pd.api.indexers.FixedForwardWindowIndexer(window_size=hours)
    data['Rolling Sum'] = data['Net Energy'].rolling(window=indexer, min_periods=1).apply(customAcc)
    return data


def hourly_to_days(data, loads=None, gens=None):
    # Create a dataframe with the max capacity required for each day.
    data['Time'] = pd.date_range("2023-01-01", periods=len(data['Net Energy']), freq="h")
    data['Day'] = data['Time'].dt.day_of_year
    data[gens] *= -1

    # Remove the extra time values at the end of dataframe
    dayMaxes = data[0:8760].groupby(['Day']).quantile(1)

    dayMaxes["Ones"] = 1
    return dayMaxes


class Data_series():
    def __init__(self, name, units, is_gen, file, base=1, scaling=1) -> None:
        self.name = name
        self.units = units
        self.is_gen = is_gen
        if isinstance(file, pd.Series) or isinstance(file, pd.DataFrame) :
            self.profile = file
        else:
            self.profile_from_csv(file)
        self.base = int(base)
        self.scaling = int(scaling)
        self.scaled_data = self.set_scale(self.scaling)

    def __str__(self) -> str:
        return f"{self.name}, Units: {self.units}, is: {self.is_gen}"
    
    def set_scale(self, scaling):
        self.scaling = scaling
        self.scaled_data = self.profile * (self.scaling / self.base)
    
    def profile_from_csv(self, csv_file):
        self.profile = pd.read_csv(csv_file,header=None, names=[self.name])
        if self.units == "kBtu":
            self.profile *= 0.000293071
        if self.is_gen == "Generator":
            self.profile *= -1
    
    def metadata(self):
        metadata = {"units": self.units,
                "base_size": str(self.base),
                "scale": str(self.scaling),
                "is_gen": self.is_gen,
                "name": self.name}
        return metadata
    
    def pyarrow_field(self):
        return pa.field(self.name, type=pa.float64(), metadata=self.metadata())
    
    def update_series(self, name, units, gen, base):
        if isinstance(self.profile, pd.Series):
            self.profile.rename(name, inplace=True)
        if isinstance(self.profile, pd.DataFrame):
            self.profile.rename(columns={self.name : name}, inplace=True)
        if isinstance(self.scaled_data, pd.Series):
            self.scaled_data.rename(name, inplace=True)
        if isinstance(self.scaled_data, pd.DataFrame):
            self.scaled_data.rename(columns={self.name : name}, inplace=True)
        self.name = name
        self.units = units
        self.gen = gen
        self.base = base


class Data_model():
    def __init__(self, name) -> None:
        self.name = name
        self.data_profiles = {}

    def save_parquet(self):
        if not self.data_profiles:
            print("error")
            raise ValueError("Cannot save empty file")        
        else:
            df = []
            series_schema = []
            for series in self.data_profiles.values():
                df.append(series.profile)
                series_schema.append(series.pyarrow_field())
            schema = pa.schema(series_schema)
            df = pd.concat(df, axis=1)
            # df = df.astype(str)
            table = pa.Table.from_pandas(df, schema=schema)
            existing_metadata = table.schema.metadata
            file_metadata = {'building type': 'house'}
            merged_metadata = {**file_metadata, **existing_metadata}
            table = table.replace_schema_metadata(merged_metadata)
            file_path = os.path.join('models', f'{self.name}.parquet')
            pq.write_table(table, file_path)


    def add_series(self, data_series:Data_series):
        self.data_profiles[data_series.name] = data_series

    def create_schema(self):
        schema_list = []
        for series in self.data_profiles.values():
            schema_list.append(series.pyarrow_field())
        return pa.schema(schema_list)
    
    def update_series(self):
        pass

    def delete_series(self, name):
        del self.data_profiles[name]


def model_from_file(file):
    file_name = file.split('.', 1)[0]
    file_path = os.path.join('models',file)
    model = Data_model(file_name) 
    table = pq.read_table(file_path)
    table_schema = table.schema
    for column in table.column_names:
        field = table_schema.field(column)
        field_metadata = field.metadata
        decoded_metadata = {}
        for key, value in field_metadata.items():
            new_key = key.decode()
            new_val = value.decode()
            decoded_metadata[new_key] = new_val
        data = table.column(column).to_pandas()
        series = Data_series(
            decoded_metadata['name'],
            decoded_metadata['units'],
            decoded_metadata['is_gen'],
            data,
            decoded_metadata['base_size'],
            decoded_metadata['scale']
        )
        model.add_series(series)
    return model


def get_model_files():
    path = os.path.join("models")
    contents = os.listdir(path)
    return contents


def validate_csv(csv):
    #TODO: validate csv for correct formatting
    '''
    Takes in a csv file and verifies correct format:
         8760 rows of ints or floats
        Limit size of numbers to 64 bit
        Make sure only one column
        Make sure no other values are in rows
        Limit file size
        Return True if all conditions met else false
    '''
    if st.session_state['files'][csv].profile.shape() != (8760, 1):
        st.error("Wrong File Shape")
        df_load = None
        run = False
    elif st.session_state['files'][csv].profile.dtypes != (float or int):
        st.error("Wrong datatype")
        df_load = None
        run = False
    else:
        run = True
    return True
