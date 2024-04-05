from sqlalchemy import create_engine, text, Table, Column, Integer, String, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

class MariaDBManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MariaDBManager, cls).__new__(cls)
            load_dotenv()

            # Database URL
            db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', '3306')}"

            try:
                # Create engine
                cls._instance.engine = create_engine(db_url, echo=True)
                print("Connected to MariaDB Server")
            except SQLAlchemyError as e:
                print(f"Error connecting to MariaDB: {e}")
                cls._instance = None
        return cls._instance

    def create_database(self, db_name):
        """Create a new database."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                conn.commit()
                print(f"Database {db_name} created successfully")
        except SQLAlchemyError as e:
            print(f"Error: {e}")

    def replace_dataframe_2_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists='replace', index=False)  # 'if_exists='replace'' is crucial

    def delete_database(self, db_name):
        """Delete a database."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"DROP DATABASE {db_name}"))
                conn.commit()
                print(f"Database {db_name} deleted successfully")
        except SQLAlchemyError as e:
            print(f"Error: {e}")

    def execute_query(self, db_name, query, **kwargs):
        """Execute a SQL query."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {db_name}"))
                result = conn.execute(text(query), **kwargs)
                conn.commit()
                print(f"{query} \n successful.")
                return result.fetchall()
        except SQLAlchemyError as e:
            print(f"Error: {e}")

    
    def execute_insert(self, db_name, query, **kwargs):
        """Execute a SQL query."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"USE {db_name}"))
                conn.execute(text(query), **kwargs)
                conn.commit()
                print(f"{query} \n successful.")
        except SQLAlchemyError as e:
            print(f"Error: {e}")


    def list_databases(self):
        """List all databases."""
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text('SHOW DATABASES')).fetchall()
                return [table[0] for table in res]
        except SQLAlchemyError as e:
            print(f"Error: {e}")
            return []

    def list_tables(self, db_name):
        """List all tables in a specific database."""
        try:
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}'"
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchall()
                # Each result is a tuple, so extract the first element to get the table name
                return [table[0] for table in result]
        except SQLAlchemyError as e:
            print(f"Error: {e}")
            return []

    def close_connection(self):
        """Close the database connection."""
        self.engine.dispose()
        print("MariaDB connection is closed")