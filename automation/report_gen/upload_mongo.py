from pymongo import MongoClient
import pandas as pd


def upload_df_to_mongodb(
        df: pd.DataFrame,
        db_name: str,
        collection_name: str,
        mongo_uri: str
):
    """
    Upload the resulting faculty report to a new collection in teamdb.

    :param df: resulting faculty report
    :param db_name: mongodb database to use
    :param collection_name: new mongodb collection to use
    :param mongo_uri: connection string with user/password auth
    :return:
    """
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
