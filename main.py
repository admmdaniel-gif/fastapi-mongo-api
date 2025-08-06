from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel, Field
from typing import List, Dict, Any
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


# Pydantic models for responses

class RootResponse(BaseModel):
    message: str = Field(..., example="API working successfully")

class CollectionsResponse(BaseModel):
    collections: List[str] = Field(..., example=["movies", "users", "comments"])

class CountResponse(BaseModel):
    collection: str = Field(..., example="movies")
    count: int = Field(..., example=12345)

class SampleDocResponse(BaseModel):
    collection: str = Field(..., example="movies")
    sample: List[Dict[str, Any]] = Field(
        ...,
        example=[
            {
                "_id": "60c5baef7d0f0000e90005d1",
                "title": "The Shawshank Redemption",
                "year": 1994,
                "genres": ["Drama"]
            },
            {
                "_id": "60c5baef7d0f0000e90005d2",
                "title": "The Godfather",
                "year": 1972,
                "genres": ["Crime", "Drama"]
            }
        ]
    )

class SchemaInferResponse(BaseModel):
    collection: str = Field(..., example="movies")
    sample_size: int = Field(..., example=10)
    schema: Dict[str, List[str]] = Field(
        ...,
        example={
            "title": ["str"],
            "year": ["int"],
            "genres": ["list"]
        }
    )

class Relationship(BaseModel):
    from_collection: str = Field(..., example="comments")
    field: str = Field(..., example="movieId")
    possible_target: str = Field(..., example="movies")
    via: str = Field(..., example="_id")

class RelationshipInferResponse(BaseModel):
    possible_relationships: List[Relationship] = Field(
        ...,
        example=[
            {
                "from_collection": "comments",
                "field": "movieId",
                "possible_target": "movies",
                "via": "_id"
            }
        ]
    )


# Endpoints

@app.get("/", response_model=RootResponse)
def read_root():
    return {"message": "API working successfully"}


@app.get("/collections", response_model=CollectionsResponse)
def get_collections():
    try:
        collections = db.list_collection_names()
        return {"collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/collections/{name}/count", response_model=CountResponse)
def count_documents(name: str):
    try:
        collection = db[name]
        count = collection.count_documents({})
        return {"collection": name, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/collections/{name}/sample",
    response_model=SampleDocResponse,
    responses={
        200: {
            "description": "Sample documents from the collection",
            "content": {
                "application/json": {
                    "example": {
                        "collection": "movies",
                        "sample": [
                            {
                                "_id": "60c5baef7d0f0000e90005d1",
                                "title": "The Shawshank Redemption",
                                "year": 1994,
                                "genres": ["Drama"]
                            },
                            {
                                "_id": "60c5baef7d0f0000e90005d2",
                                "title": "The Godfather",
                                "year": 1972,
                                "genres": ["Crime", "Drama"]
                            }
                        ]
                    }
                }
            }
        }
    }
)
def sample_documents(name: str, limit: int = 5):
    try:
        collection = db[name]
        docs = list(collection.find().limit(limit))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"collection": name, "sample": docs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/schema-infer/{collection_name}", response_model=SchemaInferResponse)
def schema_infer(collection_name: str, limit: int = 10):
    try:
        collection = db[collection_name]
        sample_docs = list(collection.find().limit(limit))
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


@app.get("/relationship-infer", response_model=RelationshipInferResponse)
def infer_relationships():
    try:
        collections = db.list_collection_names()
        schemas = {}
        for coll in collections:
            schemas[coll] = get_sample_schema(coll)

        relationships = []
        for source_coll, source_fields in schemas.items():
            for field, types in source_fields.items():
                if field.endswith("_id") or field == "userId" or field == "movieId":
                    for target_coll, target_fields in schemas.items():
                        if target_coll == source_coll:
                            continue
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
