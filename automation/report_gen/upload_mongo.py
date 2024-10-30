from pymongo import MongoClient
import pandas as pd


def upload_df_to_mongodb(
        df: pd.DataFrame,
        db_name: str,
        collection_name: str,
        mongo_uri: str
):
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Convert DataFrame to dictionary records
    data = df.to_dict(orient="records")

    # Insert records into the collection
    if data:
        collection.insert_many(data)
        print(f"Inserted {len(data)} records into '{collection_name}' collection in '{db_name}' database.")
    else:
        print("No data found in the CSV file.")

    # Close the MongoDB connection
    client.close()
