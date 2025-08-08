import csv
import pandas as pd
import os
from pathlib import Path
import datetime

class DataLoader:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.df = None  # DataFrame to hold the loaded data
        if (self.data_dir / "processed_data.pkl").exists():
            self.df = pd.read_pickle(self.data_dir / "processed_data.pkl")
            return
        self.__files = {}

    def load(self, force_reload=False):
        if self.df is not None and not force_reload:
            return self.df  # Return the existing DataFrame if it has already been loaded
        
        # Initialize the files dictionary to store parsed CSV data
        self.__files = {}
        _data_dir = Path(self.data_dir / "La Vista 1H") 
        for file_name in os.listdir(_data_dir):
            print(file_name)
            content = self.__parse_csv(
                _data_dir / file_name
            )
            self.__files[file_name] = content
        self.__cleanup()
        self.__files['Sales Meter Flow Rate (MCF_Day).csv'] = self.__flow_rate_cycles()
        self.df = pd.DataFrame(self.__files['Sales Meter Flow Rate (MCF_Day).csv'], columns=['cycle_id', 'isotime', 'flow_rate'])
        self.__data_entries_manager()

        output_path = self.data_dir / "processed_data.pkl"
        self.df.to_pickle(output_path)
        
        return self.df
    
    def get_data(self):
        if not hasattr(self, 'df'):
            raise ValueError("Data has not been loaded yet. Call load() first.")
        return self.df


    def __iso_to_unix(self, iso_str):
        # Handles ISO 8601 with 'Z' (UTC)
        # 2025-06-29T08:08:20Z
        try:
            dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            return int(dt.timestamp())
        except ValueError as e:
            print(f"error: {iso_str}")
            raise e
        
    def __parse_csv(self, file_path):
        csv_file = []
        idx = 0
        with open(file_path, mode='r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if (idx == 0):
                    idx+=1
                    continue
                row[0] = self.__iso_to_unix(row[0])
                row[1] = float(row[1])
                csv_file.append(row)
        return csv_file

    def __cleanup(self):
        isotime_threshold = 0
        for i in range(len(self.__files["Sales Meter Flow Rate (MCF_Day).csv"])):
            if self.__files["Sales Meter Flow Rate (MCF_Day).csv"][i][1] == 0.0:
                if i==0: # if list is already clean (starting from zero)
                    break
                isotime_threshold = self.__files["Sales Meter Flow Rate (MCF_Day).csv"][i-1][0] + 60 # added one minute of threshold
                self.__files["Sales Meter Flow Rate (MCF_Day).csv"] = self.__files["Sales Meter Flow Rate (MCF_Day).csv"][i:]
                break
        # now cleaning all the data using this in all files
        for file in self.__files:
            if(file=="Sales Meter Flow Rate (MCF_Day).csv"):
                continue
            for i in range(len(self.__files[file])):
                if self.__files[file][i][0] >= isotime_threshold:
                    #print(f"threshold: ${isotime_threshold} and data: ${files[file][i][0]}\n")
                    self.__files[file] = self.__files[file][i:]  # remove the first element
                    break

    def __flow_rate_cycles(self):
        flow_rate = self.__files['Sales Meter Flow Rate (MCF_Day).csv']
        data = [[None, None, None] for i in range(len(flow_rate))]
        cycle_id = -1
        new_cycle_mil_gia = False
        for i in range(len(data)):
            if flow_rate[i][1] == 0.0 and not new_cycle_mil_gia:
                cycle_id+=1
                new_cycle_mil_gia = True
            elif flow_rate[i][1] > 0.0:
                new_cycle_mil_gia = False
            data[i][0] = cycle_id
            data[i][1] = flow_rate[i][0]
            data[i][2] = flow_rate[i][1]

        return data

    def __data_entries_manager(self):
        for file_name in self.__files:
            if file_name == "Sales Meter Flow Rate (MCF_Day).csv":
                continue
            self.df[file_name] = None  # Create a new column for each file
            # Iterate through the DataFrame and for each isotime of flow rate (threshold_isotime)
            # find the corresponding isotime in the range of the thereshold_isotime_range
            # and assign the value from the corresponding file to the DataFrame      
            for i in range(len(self.df)):
                threshold_isotime = self.df.iloc[i]["isotime"]
                threshold_isotime_range = [threshold_isotime-60, threshold_isotime+61] # last is not included soo +61 to get +60
                for j in range(len(self.__files[file_name])):
                    if self.__files[file_name][j][0] in range(threshold_isotime_range[0], threshold_isotime_range[1]):
                        # found the data entry
                        self.df.at[i,file_name] = self.__files[file_name][j][1]
                        break