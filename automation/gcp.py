import csv
import os
from google.cloud import storage
from pymongo import MongoClient
from io import StringIO
import functions_framework

MONGO_URI = os.getenv('MONGO_URI')

mongo_client = MongoClient(MONGO_URI)
db = client[]