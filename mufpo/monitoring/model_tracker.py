import datetime
from mufpo.database_manager import MariaDBManager
from .factory.watcher import Watcher
import logging
DB_NAME = "funds_db"

class WatchTrain(Watcher):
    @staticmethod
    def log(
            model_name: str, 
            version: str, date: datetime.datetime,
            status: str, update=False
    ):  
        db_manager = MariaDBManager()
        table_name = 'watcher_training'
        # Replace 'your_database_name' with your actual database name
        # Check if the table exists, and create it if it doesn't
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            model_name VARCHAR(100),
            version VARCHAR(100),
            date DATETIME,
            status VARCHAR(100),
            PRIMARY KEY (model_name, version)
        );
        """
        db_manager.execute_query(DB_NAME, query=create_table_query)
        
        # Insert or update data in the table
        query = f"""
        INSERT INTO {table_name} (model_name, version, date, status) 
        VALUES ('{model_name}', '{version}', '{date}', '{status}')
        ON DUPLICATE KEY UPDATE status = '{status}';
        """
            
        response = db_manager.execute_query(DB_NAME, query=query)
        db_manager.close_connection()
        return response




class WatchEval(Watcher):
    @staticmethod
    def log(
            model_name:str, 
            version: str, date: datetime.datetime,
            metric: str, accuracy: float
    ):
        db_manager = MariaDBManager()
        table_name = 'watcher_eval'
        query = f"""
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{DB_NAME}' AND table_name = '{table_name}') 
        THEN
            -- If the table does not exist, create it
            CREATE TABLE {table_name} (
                model_name VARCHAR(100),
                version VARCHAR(100),
                date DATETIME,
                metric VARCHAR(100),
                accuracy FLOAT
                PRIMARY KEY (model_name, version)
            );

        -- Insert data into the table
        INSERT INTO {table_name} (model_name, version, date, metric, accuracy, ) 
        VALUES ({model_name}, {version}, {date}, {metric}, {accuracy});
        END IF;"""
        response = db_manager.execute_query(DB_NAME, query=query)
        db_manager.close_connection()
        return response


class WatchPredict(Watcher):
    @staticmethod
    def log(dataframe):
        db_manager = MariaDBManager()
        table_name = 'watcher_prediction'
        
        # Check if the table exists, and create it if it doesn't
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            model_name VARCHAR(100),
            version VARCHAR(100),
            date DATETIME,
            predict FLOAT,
            target_name VARCHAR(100),
            flag VARCHAR(100),
            description VARCHAR(255),
            PRIMARY KEY (model_name, version, date)
        );
        """
        db_manager.execute_query(DB_NAME, query=create_table_query)
        
        # Insert data into the table
        for _, row in dataframe.iterrows():
            model_name = row['model_name']
            version = row['version']
            date = row['date']
            predict = row['predict']
            target_name = row['target_name']
            flag = row['flag']
            description = row['description']
            
            insert_query = f"""
            INSERT INTO {table_name} (model_name, version, date, predict, target_name, flag, description) 
            VALUES ('{model_name}', '{version}', '{date}', {predict}, '{target_name}', '{flag}', '{description}');
            """
            db_manager.execute_query(DB_NAME, query=insert_query)

        db_manager.close_connection()
        return "Data inserted successfully"


class WatchModel(Watcher):
    @staticmethod
    def log(
            model_name: str,
            version: str, date: datetime.datetime,
            parameters: str, description: str = ""
    ):
        db_manager = MariaDBManager()
        table_name = 'watcher_prediction'
        query = f"""
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{DB_NAME}' AND table_name = {table_name}) 
        THEN
            -- If the table does not exist, create it
            CREATE TABLE {table_name} (
                model_name VARCHAR(100),
                version VARCHAR(100),
                date DATETIME,
                parameters VARCHAR(255),
                description VARCHAR(255)
                PRIMARY KEY (model_name, version)
            );

        -- Insert data into the table
        INSERT INTO {table_name} (model_name, version, date, predict, target_name, flag, description) 
        VALUES ({model_name}, {version}, {date}, {parameters}, {description});
        END IF;"""
        response = db_manager.execute_query(DB_NAME, query=query)
        db_manager.close_connection()
        return response
