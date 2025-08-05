from pymongo import MongoClient

MONGO_URI = "mongodb+srv://admmdaniel:ADo3Vy93OGr4BVyP@dataclassydb.xhyoae2.mongodb.net/?retryWrites=true&w=majority&appName=DataClassyDB"
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

try:
    info = client.server_info()
    print("Conexión exitosa:", info)
except Exception as e:
    print("Error conectando a MongoDB:", e)