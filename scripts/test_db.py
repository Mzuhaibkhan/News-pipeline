import os
import sys
import certifi
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Read MONGO_URI from env or use the one in .env
from dotenv import load_dotenv
load_dotenv()

mongo_uri = os.environ.get("MONGO_URI")
print("Connecting to:", mongo_uri)

try:
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000, tlsCAFile=certifi.where())
    # Ping
    client.admin.command("ping")
    print("Success: Connected to MongoDB cluster!")
except ConnectionFailure as exc:
    print("Failed to connect to MongoDB cluster:")
    print(exc)
    sys.exit(1)
except Exception as e:
    print("Unexpected error:")
    print(e)
    sys.exit(1)
