from sqlalchemy import create_engine
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

                # Create a Session
                Session = sessionmaker(bind=cls._instance.engine)
                cls._instance.session = Session()
                print("Connected to MariaDB Server")
            except SQLAlchemyError as e:
                print(f"Error connecting to MariaDB: {e}")
                cls._instance = None
        return cls._instance

    def create_database(self, db_name):
        """Create a new database."""
        try:
            self.engine.execute(f"CREATE DATABASE {db_name}")
            print(f"Database {db_name} created successfully")
        except SQLAlchemyError as e:
            print(f"Error: {e}")

    def delete_database(self, db_name):
        """Delete a database."""
        try:
            self.engine.execute(f"DROP DATABASE {db_name}")
            print(f"Database {db_name} deleted successfully")
        except SQLAlchemyError as e:
            print(f"Error: {e}")

    def execute_query(self, query, **kwargs):
        """Execute a SQL query."""
        try:
            self.session.execute(query, **kwargs)
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            print(f"Error: {e}")

    def close_connection(self):
        """Close the database connection."""
        self.session.close()
        print("MariaDB connection is closed")
