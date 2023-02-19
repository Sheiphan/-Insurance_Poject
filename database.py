import pymongo
import pandas as pd
import json
from pymongo import MongoClient, InsertOne

client = pymongo.MongoClient("mongodb+srv://Sheiphan_MongoDB:EBEnezeR123@cluster0.iewrgfp.mongodb.net/?retryWrites=true&w=majority")

DATA_FILE = "insurance.csv"
DATABASE_NAME = "INSURANCE_DATABASE"
COLLECTION_NAME = "INSURANCE_COLLECTION"

if __name__ == "__main__":
    df = pd.read_csv(DATA_FILE)
    print(f"Rows and Columns: {df.shape}")
    
    # df.reset_index(drop = True, inplace=True)
    
    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])
              
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)