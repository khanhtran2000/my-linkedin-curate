import os
import sys

import common_utils as cu


class DataIngester():
    def __init__(self):
        self.logger = cu.create_log()
        self.rt = cu.POST_RT
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.database = os.getenv("DATABASE")

    def get_user(self):
        '''Return database user.
        '''
        try:
            return self.user
        except:
            self.logger.error("Error while reading the database user : " + " Error: " + str(sys.exc_info()[0]))
    
    def get_password(self):
        '''Return database password.
        '''
        try:
            return self.password
        except:
            self.logger.error("Error while reading the database password : " + " Error: " + str(sys.exc_info()[0]))

    def get_host(self):
        '''Return database host.
        '''
        try:
            return self.host
        except:
            self.logger.error("Error while reading the database host : " + " Error: " + str(sys.exc_info()[0]))

    def get_port(self):
        '''Return database port.
        '''
        try:
            return self.port
        except:
            self.logger.error("Error while reading the database port : " + " Error: " + str(sys.exc_info()[0]))

    def get_database(self):
        '''Return database name.
        '''
        try:
            return self.database
        except:
            self.logger.error("Error while reading the database name : " + " Error: " + str(sys.exc_info()[0]))

    def connect_to_postgres(self):
        '''Create a connection to the given database.
        '''
        connection = cu.create_postgres_connection(user=self.get_user(),
                                                   password=self.get_password(),
                                                   host=self.get_host(),
                                                   port=self.get_port(),
                                                   database=self.get_database())

        return connection


class LinkedinIngester(DataIngester):
    def ingest_posts(self, clean_records: list):
        '''Update new scraped Linkedin data into the table.
        '''
        table_name = cu.get_table_name(run_type=self.rt)
        # Connect
        connection = self.connect_to_postgres()
        cursor = connection.cursor()
        # Ingest
        try:
            for record in clean_records:
                query = f"""INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(query, record)
                connection.commit()
                self.logger.info(f"> Successfully ingested record {record} into table {table_name}.\n")
        except:
            self.logger.error(f"Error while trying to insert records into table : {table_name} " + " Error: " + str(sys.exc_info()[0]))
            
        connection.close()

        self.logger.info("> PostgreSQL connection is closed.")