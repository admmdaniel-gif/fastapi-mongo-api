import json
from pymongo import MongoClient

# Connection string to your MongoDB Atlas
connection_string = "mongodb+srv://admmdaniel:ADo3Vy93OGr4BVyP@dataclassydb.xhyoae2.mongodb.net/?retryWrites=true&w=majority&appName=DataClassyDB"

# Data Base name your are looking to scan
database_name = "sample_mflix"

try:
    # Connect to your cluster
    client = MongoClient(connection_string)
    db = client[database_name]

    # List all collections automatically
    collections = db.list_collection_names()

    # Show collections
    print(f"Connecting to the database '{database_name}'. Collections found:")
    for col in collections:
        print(f" - {col}")

    # Settings Structure
    config = {
        "mongodb": {
            "uri": connection_string,
            "database": database_name,
            "collections": collections
        }
    }

    # Save settings at JSON file
    with open("config.json", "w") as f:
        json.dump(config, f, indent=4)

    print("\n File 'config.json'Successfully Generated.")

except Exception as e:
    print(" Error connecting or reading your database:")
    print(e)
