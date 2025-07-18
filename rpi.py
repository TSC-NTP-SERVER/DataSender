import subprocess
import re
from datetime import datetime, timezone
import time

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

#Database connection
mongo_client = MongoClient("mongodb+srv://admin:1234@testrecieve.ekkcpkz.mongodb.net/db?retryWrites=true&w=majority&appName=s")
db = mongo_client.db

#Create collection
def create_collection(name):
    try:
        if name not in db.list_collection_names():
            db.create_collection(name, capped=True, size=1000000, max=10)
            print(f"Created capped collection {name}")
    except CollectionInvalid as e:
        print("Collection creation failed:", e)
    return db[name]

#get_raw
def get_tracking_data():
    result = subprocess.run(['chronyc', 'tracking'], capture_output=True, text=True)
    return result.stdout

def get_clients_data():
    result = subprocess.run(['sudo', 'chronyc', 'clients'], capture_output=True, text=True)
    return result.stdout

def get_time_data():
    result = subprocess.run(['timedatectl', 'status'], capture_output=True, text=True)
    return result.stdout

#parsing
def parse_tracking_output(output):
    data = {}
    for line in output.splitlines():
        if ':' in line:
            key, val = line.split(':', 1)
            key = key.strip()
            key = key.replace(" ", "_")
            val = val.strip()
            if key == "Ref_time_(UTC)":
                data[key] = parse_ref_time(val)
            elif key == "Reference_ID":
                data[key] = val  # Force it to remain a string 
            else:
                numeric = extract_numeric(val)
                data[key] = numeric if numeric is not None else val
    return data

def parse_clients_output(output):
    clients = []
    lines = output.strip().splitlines()

    if len(lines) < 3:
        return {"Clients": []}

    # The header line is assumed to be the first line (lines[0])
    raw_headers = lines[0].split()

    counts = {}
    for h in raw_headers:
        counts[h] = counts.get(h, 0) + 1

    seen = {}
    headers = []
    # for h in raw_headers:
    #     seen[h] = seen.get(h, 0) + 1
    #     if counts[h] > 1:
    #         # Add suffix only if duplicated header
    #         key = h.replace(" ","_")
    #         headers.append(f"{key}({seen[h]})")
    #     else:
    #         # Leave unique headers unchanged
    #         key = h.replace(" ","_")
    #         headers.append(key)

    for h in raw_headers:
        seen[h] = seen.get(h, 0) + 1
        base_key = h.replace(" ", "_")
        if counts[h] > 1:
            # Suffix by context
            if seen[h] == 1:
                headers.append(f"{base_key}_ntp")
            elif seen[h] == 2:
                headers.append(f"{base_key}_cmd")
            else:
                headers.append(f"{base_key}_{seen[h]}")
        else:
            headers.append(base_key)


    # Skip separator line (lines[1])
    for line in lines[2:]:
        fields = line.split()
        if len(fields) >= len(headers):
            data = dict(zip(headers, fields))
            clients.append(data)

    return {"Clients": clients}

def parse_time_output(output):
    data = {}
    for line in output.splitlines():
        if ':' in line:
            key, val = line.split(':', 1)
            key = key.strip()
            key = key.replace(" ","_")
            val = val.strip()
            if key in ["Local_time", "Universal_time", "RTC_time"]:

                data[key] = parse_ref_time(val)

            elif key == "Time_zone":
                data[key] = val

            else:
                numeric = extract_numeric(val)
                data[key] = numeric if numeric is not None else val
    return data

#Time control
time_formats = [
    "%a %b %d %H:%M:%S %Y",         # e.g., Wed Jul 09 06:23:55 2025
    "%a %Y-%m-%d %H:%M:%S %Z",      # e.g., Wed 2025-07-09 06:23:55 UTC
    "%a %Y-%m-%d %H:%M:%S %z",      # e.g., Wed 2025-07-09 13:23:55 +07
    "%a %Y-%m-%d %H:%M:%S",         # e.g., Wed 2025-07-09 06:23:55
]

def parse_ref_time(val):
    for fmt in time_formats:
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    print("⚠️ Failed to parse ref time:", val)
    return val  # or return None if you prefer to skip unparsed values

#get_numeric
def extract_numeric(s):
    match = re.search(r'[-+]?\d*\.\d+|\d+', s)
    if match:
        try:
            return float(match.group(0)) if '.' in match.group(0) else int(match.group(0))
        except ValueError:
            return None
    return None

#time_stamps
def add_timestamp(doc):
    doc['Timestamp'] = datetime.now(timezone.utc)
    return doc

#main
def main():
    tracking_collection = create_collection("tracking")
    client_collection = create_collection("client")
    time_collection = create_collection("time")
    while True:
        tracking_raw = get_tracking_data()
        clients_raw = get_clients_data()
        time_raw = get_time_data()

        tracking_data = parse_tracking_output(tracking_raw)
        client_data = parse_clients_output(clients_raw)
        time_data = parse_time_output(time_raw)

        tracking_data = add_timestamp(tracking_data)
        client_data = add_timestamp(client_data)
        time_data = add_timestamp(time_data)

        t_result = tracking_collection.insert_one(tracking_data)
        if t_result.inserted_id:
            print("✅ Tracking inserted to MongoDB:", t_result.inserted_id)

        c_result = client_collection.insert_one(client_data)
        if c_result.inserted_id:
            print("✅ Client inserted to MongoDB:", c_result.inserted_id)

        tc_result = time_collection.insert_one(time_data)
        if tc_result.inserted_id:
            print("✅ Time inserted to MongoDB:", tc_result.inserted_id)

if __name__ == "__main__":
    main()