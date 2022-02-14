import os
import sys

import common_utils as cu
import data_scraper as ds


class DataCleaner():
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

    def remove_emoji(self, list_of_string: list):
        clean_list = cu.remove_emoji(list_of_string=list_of_string)
        return clean_list

    def escape_single_quote(self, list_of_string: list):
        clean_list = cu.escape_single_quote(list_of_string=list_of_string)
        return clean_list


class LinkedinCleaner(DataCleaner):
    def create_ids_and_dates(self, nrow: int):
        '''Create IDs and dates for the clean records.
        :param nrow:
        '''
        try:
            # Get date of the scrape
            dates = [cu.get_current_date() for i in range(nrow)]
            # Create a list of IDs for the records
            max_id = cu.run_select_query(connection=self.connect_to_postgres(), run_type=self.rt, fields="MAX(id)")[0][0]
            if max_id is None:
                ids = [i for i in range(1, nrow+1)]
            else:
                ids = [i for i in range(max_id+1, max_id+(nrow+1))]
            self.logger.info("> IDs and dates are created for clean records.\n")
        except:
            self.logger.error("Error while creating IDs and dates for clean records : " + " Error: " + str(sys.exc_info()[0]))

        return ids, dates

    def get_author_id_and_field_id(self, nrow: int, author: str):
        '''
        :param nrow:
        :param author:
        '''
        author_id = cu.get_author_id(connection=self.connect_to_postgres(), author=author)
        field_id = cu.get_author_field_id(connection=self.connect_to_postgres(), author=author)
        author_ids = [author_id for i in range(nrow)]
        field_ids = [field_id for i in range(nrow)]

        return author_ids, field_ids

    def get_clean_records(self, raw_record: list, author: str) -> list:
        '''
        :param raw_record:
        '''
        texts, media_links, media_types = raw_record
        texts = self.remove_emoji(texts) # Remove emoji from texts
        texts = self.escape_single_quote(texts) # Escape the single quotes in texts
        ids, dates = self.create_ids_and_dates(nrow=len(texts))
        author_ids, field_ids = self.get_author_id_and_field_id(nrow=len(texts), author=author)
        try:
            clean_records = list(zip(ids, author_ids, field_ids, texts, dates, media_links, media_types))
            self.logger.info("> Records are ready for data ingestion.\n")
        except Exception as e:
            self.logger.error("Error while finalizing records for data ingestion : " + " Error: " + str(sys.exc_info()[0]))

        return clean_records