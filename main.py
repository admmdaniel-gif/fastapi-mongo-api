from fastapi import FastAPI, HTTPException, Path, Query
from pymongo import MongoClient
import uvicorn
import os

app = FastAPI()

# Setup your connection string
MONGO_URI = os.environ.get("MONGO_URI")
DATABASE_NAME = "sample_mflix"

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DATABASE_NAME]
except Exception as e:
    raise RuntimeError(f"Error connecting with MongoDB: {e}")

def infer_schema(documents):
    schema = {}
    for doc in documents:
        for key, value in doc.items():
            if key == "_id":
                continue  # Ignoramos _id si quieres
            value_type = type(value).__name__
            if key not in schema:
                schema[key] = set()
            schema[key].add(value_type)

    # Convertimos los sets en listas
    return {field: list(types) for field, types in schema.items()}

def get_sample_schema(collection_name, sample_size=10):
    collection = db[collection_name]
    sample_docs = list(collection.find().limit(sample_size))
    field_types = {}

    for doc in sample_docs:
        for key, value in doc.items():
            if key not in field_types:
                field_types[key] = set()
            field_types[key].add(type(value).__name__)

    return {key: list(value_types) for key, value_types in field_types.items()}

@app.get("/")
def read_root():
    return {"message": "API working successfully"}

@app.get("/collections")
def get_collections():
    try:
        collections = db.list_collection_names()
        return {"collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections/{name}/count")
def count_documents(
    name: str = Path(..., description="Name of the collection")
):
    try:
        collection = db[name]
        count = collection.count_documents({})
        return {"collection": name, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections/{name}/sample")
def sample_documents(
    name: str = Path(..., description="Name of the collection"),
    limit: int = Query(5, description="Limit of documents to return")
):
    try:
        collection = db[name]
        docs = list(collection.find().limit(limit))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"collection": name, "sample": docs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema-infer/{collection_name}")
def schema_infer(
    collection_name: str = Path(..., description="Name of the collection"),
    limit: int = Query(10, description="Number of documents to sample")
):
    try:
        collection = db[collection_name]
        sample_docs = list(collection.find().limit(limit))

        # Convertimos ObjectId a string por si acaso
        for doc in sample_docs:
            doc["_id"] = str(doc["_id"])

        inferred_schema = infer_schema(sample_docs)
        return {
            "collection": collection_name,
            "sample_size": len(sample_docs),
            "schema": inferred_schema
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/relationship-infer")
def infer_relationships():
    try:
        collections = db.list_collection_names()
        schemas = {}

        # Recopilar esquemas
        for coll in collections:
            schemas[coll] = get_sample_schema(coll)

        relationships = []

        # Analizar relaciones potenciales
        for source_coll, source_fields in schemas.items():
            for field, types in source_fields.items():
                if field.endswith("_id") or field == "userId" or field == "movieId":
                    for target_coll, target_fields in schemas.items():
                        if target_coll == source_coll:
                            continue
                        # Buscamos si el campo aparece como _id en otra colección
                        if "_id" in target_fields:
                            relationships.append({
                                "from_collection": source_coll,
                                "field": field,
                                "possible_target": target_coll,
                                "via": "_id"
                            })

        return {
            "possible_relationships": relationships
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
