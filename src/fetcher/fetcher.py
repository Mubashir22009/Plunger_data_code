from typing import Dict, Any,List
from dateutil import parser
import os
import csv
from typing import List, Tuple
from datetime import datetime, timedelta
from .onping_auth import AuthManager
from .onping_fetcher import fetch_data_range
import json

from ..metadata import cdt,WELLS_CONFIG_FILE,DATA_FOLDER
days_to_subtract=1    # used if no config avaliable  or  FETCHDATA_DAYS = True
FETCHDATA_DAYS =False


def save_to_csv(file_path: str, data: List[Dict[str, Any]]):
    # Ensure the folder path exists
    folder = os.path.dirname(file_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    # Check if file exists
    file_exists = os.path.exists(file_path)

    #  Open the file in append mode
    with open(file_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write header if file is new
        if not file_exists:
            writer.writerow(["timestamp", "val"])

        for item in data:
            # Extract "time" and "val" from each dict
            timestamp = item.get("time")
            val = item.get("val")

            # Print comma-separated
            print(f"{timestamp},{val}")

            # Write to CSV
            writer.writerow([timestamp, val])

    print(f"Saved {len(data)} rows to {file_path}")


def fetch_data(auth: AuthManager, config: Dict) -> None:
    stepSec=config["step_seconds"]
    print(f"Starting  data fetching ")
    
   
    try:
        if(config==None or FETCHDATA_DAYS):
            print("config is none")
            startTime = endTime = datetime.now()
            startTime = (startTime - timedelta(days=max(days_to_subtract,1))).astimezone(cdt)
        else:
            startTime = parser.parse(config["lastFetchTime"]).astimezone(cdt)
            endTime = datetime.now()
        endTime = endTime.astimezone(cdt)
        


        iso_str = endTime.isoformat()
        print(f"found config , start time is now {startTime} and {endTime}")
        if not os.path.exists(WELLS_CONFIG_FILE):
            with open(WELLS_CONFIG_FILE, "w") as f:
                json.dump({}, f)

            
        with open(WELLS_CONFIG_FILE, "r") as f:
            file = json.load(f)
            file["lastFetchTime"] = iso_str

        with open(WELLS_CONFIG_FILE, "w") as f:
            print(f"writing  to config file")
            json.dump(file, f, indent=3)
            

                
        print("--- Starting new live fetch data ---")
        for well_object in config["wells"]:
            well_name = well_object.get("name", "N/A")

            print(f"\nWell: {well_name}")
            for pid_object in well_object["pids"]:
                pid_name = pid_object.get("name", "N/A")
                pid_id = pid_object.get("pid", "N/A")
                data = fetch_data_range(auth,pid_id,startTime,endTime,stepSec)
                print("GOT DATA")
                if data != None:
                    save_to_csv(f"{DATA_FOLDER}/{well_name}/{pid_name}.csv",data)
                    print(f"✓ {well_name} - {pid_name} ({pid_id}): {len(data)} readings fetched")
                else:
                    print(f"✗ {well_name} - {pid_name} ({pid_id}): No data found for range {startTime} to {endTime}")
    except KeyboardInterrupt:
        print("Live data fetching stopped by user.")
          
        print(f"---  fetch data complete. ")


def fetcher_Main():
    print("start Athentication")
    auth=AuthManager()
    auth.authenticate()
    if  os.path.exists(WELLS_CONFIG_FILE):    
        print("file exists") 
        with open(WELLS_CONFIG_FILE, "r") as f:
         config = json.load(f)
         fetch_data(auth,config)
    else:
        print(" CANNOT FETCH DATA AS NO CONFIG PROVIDED")

    