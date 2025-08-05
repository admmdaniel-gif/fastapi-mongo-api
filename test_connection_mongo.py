from pymongo import MongoClient

# Reemplaza <db_password> por tu contraseña real de MongoDB Atlas.
# Asegúrate de que el connection string esté bien formado
connection_string = "mongodb+srv://admmdaniel:ADo3Vy93OGr4BVyP@dataclassydb.xhyoae2.mongodb.net/?retryWrites=true&w=majority&appName=DataClassyDB"

# Conectar a MongoDB
client = MongoClient(connection_string)

# Acceder a una base de datos específica
db = client["sample_mflix"]

# Probar la conexión (opcional)
try:
    print("Connected Successfully!")
    # Puedes intentar acceder a una colección para comprobar que la conexión funciona.
    collection = db["movies"]
    print("Collection:", collection.name)
except Exception as e:
    print("There was an error connecting:", e)
