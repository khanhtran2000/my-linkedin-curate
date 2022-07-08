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
    
    #----------------------- Texts Preprocessing -----------------------#

    def remove_emoji(self, list_of_string: list):
        clean_list = cu.remove_emoji(list_of_string=list_of_string)
        return clean_list

    def escape_single_quote(self, list_of_string: list):
        clean_list = cu.escape_single_quote(list_of_string=list_of_string)
        return clean_list

    def remove_hashtags(self, list_of_string: list):
        clean_list = cu.remove_hashtags(list_of_string=list_of_string)
        return clean_list
    
    def extract_hashtags(self, list_of_string: list):
        hashtags = cu.extract_hashtags(list_of_string=list_of_string)
        return hashtags

    def remove_mentions(self, list_of_string: list):
        clean_list = cu.remove_mentions(list_of_string=list_of_string)
        return clean_list

    def extract_mentions(self, list_of_string: list):
        mentions = cu.extract_mentions(list_of_string=list_of_string)
        return mentions

    def remove_links(self, list_of_string: list):
        clean_list = cu.remove_links(list_of_string=list_of_string)
        return clean_list
    
    # def extract_links(self, list_of_string: list):
    #     links = cu.extract_links(list_of_string=list_of_string)
    #     return links


class LinkedinCleaner(DataCleaner):
    def get_date_keys(self, nrow: int) -> list:
        '''Get the date keys.
        :param nrow: number of rows
        '''
        date_key = cu.get_date_key(connection=self.connect_to_postgres())
        date_keys = [date_key for i in range(nrow)]
        return date_keys

    def get_author_keys(self, nrow: int, author: str):
        '''Get the author keys from the author_dimension table.
        :param nrow: number of rows
        :param author: author name
        '''
        author_key = cu.get_author_key(connection=self.connect_to_postgres(), author=author)
        author_keys = [author_key for i in range(nrow)]

        return author_keys

    def get_clean_records(self, raw_record: list, author: str) -> list:
        '''Clean and prepare clean values for insertions into posts_fact table.
        :param raw_record:
        '''
        texts, reactions_counts, comments_counts, shares_counts, media_links, media_types = raw_record
        texts_without_emoji = self.remove_emoji(texts) # Remove emoji from texts
        clean_texts = self.escape_single_quote(texts_without_emoji) # Escape the single quotes in texts
        clean_texts = self.remove_hashtags(clean_texts) # Remove hashtags
        clean_texts = self.remove_mentions(clean_texts) # Remove mentions
        clean_texts = self.remove_links(clean_texts) # Remove links
        date_keys = self.get_date_keys(nrow=len(texts))
        author_keys = self.get_author_keys(nrow=len(texts), author=author)

        # Preprocess hashtags, and mentions
        raw_hashtags = self.extract_hashtags(texts_without_emoji)
        raw_mentions = self.extract_mentions(texts_without_emoji)

        hashtags = []
        for raw_hashtag in raw_hashtags:
            if len(raw_hashtag) > 0:
                hashtags.append("{" + ','.join(raw_hashtag) + "}")
            else:
                hashtags.append('{' + 'None' + '}')

        mentions = []
        for raw_mention in raw_mentions:
            if len(raw_mention) > 0:
                mentions.append("{" + ','.join(raw_mention) + "}")
            else:
                mentions.append('{' + 'None' + '}')

        try:
            clean_records = list(zip(date_keys, author_keys, clean_texts, reactions_counts, comments_counts, shares_counts, media_links, media_types, hashtags, mentions))
            self.logger.info("> Records are ready for data ingestion into posts_fact table.\n")
        except:
            self.logger.error("Error while finalizing records for data ingestion into posts_fact table: " + " Error: " + str(sys.exc_info()[0]))

        return clean_records