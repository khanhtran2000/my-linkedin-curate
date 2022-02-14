from dotenv import load_dotenv
import unittest
import os
import sys
sys.path.append("..")

import src.common_utils as cu


class Test_Utils(unittest.TestCase):
    def test_create_connection(self):
        load_dotenv()
        connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                                   password=os.getenv("PASSWORD"),
                                                   host=os.getenv("HOST"),
                                                   port=os.getenv("PORT"),
                                                   database=os.getenv("DATABASE"))
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        actual = cursor.fetchone()[0]
        expected = 1
        self.assertEqual(expected, actual)
    
    def test_run_insert_query(self):
        load_dotenv()
        connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                                   password=os.getenv("PASSWORD"),
                                                   host=os.getenv("HOST"),
                                                   port=os.getenv("PORT"),
                                                   database=os.getenv("DATABASE"))
        actual = cu.run_select_query(connection=connection, run_type=cu.COVID_RT, fields="MAX(id)")
        expected = 63
        self.assertEquals(expected, actual)

    def test_login(self):
        cu.login_linkedin()

    def test_create_soup(self):
        load_dotenv()
        soup = cu.create_soup(post_url=os.getenv("TEST_URL"))
        return soup

    def test_get_author_id(self):
        load_dotenv()
        connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                                   password=os.getenv("PASSWORD"),
                                                   host=os.getenv("HOST"),
                                                   port=os.getenv("PORT"),
                                                   database=os.getenv("DATABASE"))
        print(cu.get_author_id(connection=connection, author="Lex Fridman"))   

    def test_get_author_field_id(self):
        load_dotenv()
        connection = cu.create_postgres_connection(user=os.getenv("DB_USER"),
                                                   password=os.getenv("PASSWORD"),
                                                   host=os.getenv("HOST"),
                                                   port=os.getenv("PORT"),
                                                   database=os.getenv("DATABASE"))
        print(cu.get_author_field_id(connection=connection, author="Lex Fridman"))


if __name__ == "__main__":
    unittest.main()