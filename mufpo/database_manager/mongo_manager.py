from pymongo import MongoClient
from dotenv import load_dotenv
import os
import pandas as pd

class MongoDBManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBManager, cls).__new__(cls)
            load_dotenv()

            # MongoDB URI
            uri = f"mongodb://{os.getenv('MONGO_DB_USER')}:{os.getenv('MONGO_DB_PASS')}@{os.getenv('MONGO_DB_HOST')}:{os.getenv('MONGO_DB_PORT', '27017')}"

            try:
                # Create MongoDB client
                cls._instance.client = MongoClient(uri)
                print("Connected to MongoDB Server")
            except Exception as e:
                print(f"Error connecting to MongoDB: {e}")
                cls._instance = None
        return cls._instance

    def create_database(self, db_name):
        """Create a new database."""
        try:
            self.client[db_name].command('ping') 
            print(f"Database {db_name} created successfully")
        except Exception as e:
            print(f"Error: {e}")

    def delete_database(self, db_name):
        """Delete a database."""
        try:
            self.client.drop_database(db_name)
            print(f"Database {db_name} deleted successfully")
        except Exception as e:
            print(f"Error: {e}")

    def execute_query(self, db_name, collection_name, query):
        """Execute a MongoDB query."""
        try:
            collection = self.client[db_name][collection_name]
            result = collection.find(query)
            print("Query successful.")
            return list(result)
        except Exception as e:
            print(f"Error: {e}")

    def insert_data(self, db_name, collection_name, data):
        """Insert data directly into a MongoDB collection."""
        try:
            collection = self.client[db_name][collection_name]
            if isinstance(data, list):
                collection.insert_many(data)
            else:
                collection.insert_one(data)
            print(f"Data insertion into {collection_name} successful.")
        except Exception as e:
            print(f"Error: {e}")

    def bulk_insert_from_csv(self, db_name, collection_name, csv_file):
        """Bulk insert data from a CSV file to a specified collection."""
        try:
            df = pd.read_csv(csv_file)
            data = df.to_dict('records')
            collection = self.client[db_name][collection_name]
            collection.insert_many(data)
            print(f"Bulk insert into {collection_name} successful.")
        except Exception as e:
            print(f"Error: {e}")

    def list_databases(self):
        """List all databases."""
        try:
            databases = self.client.list_database_names()
            return databases
        except Exception as e:
            print(f"Error: {e}")
            return []

    def list_collections(self, db_name):
        """List all collections in a specific database."""
        try:
            collections = self.client[db_name].list_collection_names()
            return collections
        except Exception as e:
            print(f"Error: {e}")
            return []

    def close_connection(self):
        """Close the database connection."""
        self.client.close()
        print("MongoDB connection is closed")

