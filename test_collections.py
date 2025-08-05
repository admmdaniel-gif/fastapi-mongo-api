from pymongo import MongoClient

connection_string = "mongodb+srv://admmdaniel:ADo3Vy93OGr4BVyP@dataclassydb.xhyoae2.mongodb.net/?retryWrites=true&w=majority&appName=DataClassyDB"
client = MongoClient(connection_string)
db = client["sample_mflix"]

try:
    print(" Succesfull Connection.")
    collections = db.list_collection_names()
    print("Collections Found:")
    for name in collections:
        print(f" - {name}")
except Exception as e:
    print("Error Listing Collections:", e)
