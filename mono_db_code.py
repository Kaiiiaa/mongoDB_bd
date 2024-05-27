import pandas as pd
import zipfile
import os
import numpy as np
import threading
from pymongo import MongoClient, errors
import matplotlib.pyplot as plt
import logging
import asyncio
import nest_asyncio

# Apply nest_asyncio
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the path for the zip file
zip_file_path = r'C:\Users\stank\Dropbox\My PC (LAPTOP-50O52C5F)\Downloads\aisdk-2023-05-01.zip'
csv_file_name = 'aisdk-2023-05-01.csv'

# Extract the CSV file from the zip file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extract(csv_file_name, os.path.expanduser('~/Downloads'))

# Load the dataset
csv_file_path = os.path.expanduser(f'~/Downloads/{csv_file_name}')
df = pd.read_csv(csv_file_path)

# Function to insert data into MongoDB
async def insert_data(sub_df, lock):
    try:
        client = MongoClient("mongodb://localhost:27020", serverSelectionTimeoutMS=5000)
        db = client.vessel_data
        collection = db.raw_data
        async with lock:
            collection.insert_many(sub_df.to_dict('records'))
        client.close()
    except errors.ServerSelectionTimeoutError as err:
        logging.error(f"MongoDB connection error: {err}")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")

# Function to split DataFrame into chunks for parallel insertion
def split_dataframe(df, chunk_size=10000):
    chunks = [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    return chunks

# Function to filter data
async def filter_data(lock):
    try:
        client = MongoClient("mongodb://localhost:27020", serverSelectionTimeoutMS=5000)
        db = client.vessel_data
        raw_collection = db.raw_data
        filtered_collection = db.filtered_data
        
        pipeline = [
            {"$match": {"$and": [{"Navigational status": {"$exists": True}},
                                 {"MMSI": {"$exists": True}},
                                 {"Latitude": {"$exists": True}},
                                 {"Longitude": {"$exists": True}},
                                 {"ROT": {"$exists": True}},
                                 {"SOG": {"$exists": True}},
                                 {"COG": {"$exists": True}},
                                 {"Heading": {"$exists": True}}]}},
            {"$group": {"_id": "$MMSI", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 100}}}
        ]

        valid_vessels = raw_collection.aggregate(pipeline)
        valid_vessel_ids = [vessel['_id'] for vessel in valid_vessels]

        for vessel_id in valid_vessel_ids:
            vessel_data = raw_collection.find({"MMSI": vessel_id})
            async with lock:
                filtered_collection.insert_many(vessel_data)
        client.close()
    except errors.ServerSelectionTimeoutError as err:
        logging.error(f"MongoDB connection error: {err}")
    except Exception as e:
        logging.error(f"Error filtering data: {e}")

# Function to calculate delta t and generate histogram
async def calculate_delta_t_and_generate_histogram(lock):
    try:
        client = MongoClient("mongodb://localhost:27020", serverSelectionTimeoutMS=5000)
        db = client.vessel_data
        filtered_collection = db.filtered_data
        
        vessels = filtered_collection.distinct("MMSI")
        
        delta_ts = []
        for vessel in vessels:
            data_points = list(filtered_collection.find({"MMSI": vessel}).sort("Timestamp", 1))
            for i in range(1, len(data_points)):
                delta_t = (data_points[i]['Timestamp'] - data_points[i-1]['Timestamp']).total_seconds() * 1000
                delta_ts.append(delta_t)

        client.close()

        # Generate Histogram
        plt.hist(delta_ts, bins=50)
        plt.xlabel('Delta t (ms)')
        plt.ylabel('Frequency')
        plt.title('Histogram of Delta t between data points')
        plt.show()
    except errors.ServerSelectionTimeoutError as err:
        logging.error(f"MongoDB connection error: {err}")
    except Exception as e:
        logging.error(f"Error calculating delta t: {e}")

async def main():
    lock = asyncio.Lock()

    # Split the data into smaller chunks
    chunks = split_dataframe(df, chunk_size=10000)

    # Insert data in parallel
    insert_tasks = [insert_data(chunk, lock) for chunk in chunks]
    await asyncio.gather(*insert_tasks)

    # Filter data
    await filter_data(lock)

    # Calculate delta t and generate histogram
    await calculate_delta_t_and_generate_histogram(lock)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
