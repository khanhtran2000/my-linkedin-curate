from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from dotenv import load_dotenv
import psycopg2
import sys
import os
import logging
from logging.handlers import WatchedFileHandler
from datetime import datetime
import time


load_dotenv()
# Login credentials
login_url = os.getenv("LOGIN_URL")
chrome_path = os.getenv("CHROMEPATH")
username = os.getenv("LINKEDIN_USER")
password = os.getenv("LINKEDIN_PASSWORD")
# Logger
logger = None
# Run types
AUTHOR_RT = "author"
FIELD_RT = "field"
POST_RT = "post"
INGEST_RT = "ingest"
# Tables
AUTHOR_TABLE = "all_authors"
FIELD_TABLE = "all_fields"
POST_TABLE = "author_posts"

#--------------- Logging utilities ---------------#

def create_log(path=None):
    '''Create a log file with default level at INFO.
    '''
    global logger

    if logger is not None:
        return logger

    if path is None:
        path = get_log_file_path()

    handler = WatchedFileHandler(os.environ.get("LOGFILE", path))
    formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler.setFormatter(formatter)
    logger = logging.getLogger("PipelineLog")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    return logger


def get_abs_root_path():
    '''Get absoulate root path.
    '''
    return os.path.abspath(os.path.dirname(__file__))


def get_log_file_path():
    '''Get log file path. Create one if not exist.
    '''
    logs_folder_path = os.path.join(get_abs_root_path(), r"../logs/")
    if not os.path.isdir(logs_folder_path):
        os.mkdir(logs_folder_path)  # create path if not found

    log_file_path = os.path.join(logs_folder_path, r"pipeline.log")
    if not os.path.isfile(log_file_path):
        open(log_file_path, 'w').close()  # create file if not found

    return log_file_path


def get_logger():
    '''Create logger.
    '''
    global logger
    if logger is None:
        logger = create_log()
    return logger

#--------------- End of Logging utilities ---------------#

#--------------- Scraping utilities ---------------#

def login_linkedin():
    '''Login
    '''
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        driver = webdriver.Chrome(executable_path=chrome_path, options=options)
        driver.get(login_url)
        elementID = driver.find_element(By.ID, "username")
        elementID.send_keys(username)
        elementID = driver.find_element(By.ID, "password")
        elementID.send_keys(password)
        elementID.submit()
        get_logger().info("Successfully log in Linkedin account with username: " + username)
        return driver
    except:
        get_logger().error(f"Error while logging in Linkedin account {username}: "  + " Error: " + str(sys.exc_info()[0]))


def infinite_scroll(driver):
    '''TODO: this is not working right now.
    '''
    #Simulate scrolling to capture all posts
    SCROLL_PAUSE_TIME = 3
    # Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def create_soup(driver, url: str) -> BeautifulSoup:
    '''Return a BeautifulSoup object.
    :param driver:
    :param url: the url that we want to scrape
    '''
    try:
        # infinite_scroll(driver=driver) # Not working
        driver.get(url)
        # Wait to load page
        time.sleep(5)
        html = driver.page_source
        soup = BeautifulSoup(html,'html.parser')
        soup.prettify()
    except:
        # get_logger().error(e)
        get_logger().error(f"Error while creating BeautifulSoup object for url {url}: "  + " Error: " + str(sys.exc_info()[0]))

    return soup

#--------------- End of Scraping utilities ---------------#


def get_current_date() -> str:
    '''Return current date as a string.
    '''
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return now


def create_postgres_connection(user: str, password: str, host: str, port: str, database: str) -> psycopg2.extensions.connection:
    '''Create a connection to PostgresSQL database
    :param user:
    :param password:
    :param host:
    :param port:
    :param database:
    '''
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        get_logger().info("> PostgreSQL connection is created.\n")
    except:
        get_logger().error("Error while connecting to PostgreSQL database : " + " Error: " + str(sys.exc_info()[0]))

    return connection


def get_table_name(run_type: str):
    '''Return table name for a given run_type.
    :param type:
    '''
    try:
        if run_type == AUTHOR_RT:
            table_name = AUTHOR_TABLE
        elif run_type == FIELD_RT:
            table_name = FIELD_TABLE
        elif run_type == POST_RT:
            table_name = POST_TABLE
    except:
        get_logger().error(f"Error while getting table name for runtype {run_type} : " + " Error: " + str(sys.exc_info()[0]))

    return table_name


def get_attribute_values(connection: psycopg2.extensions.connection, run_type: str, fields: str):
    '''Get all values of a given column and table.
    :param connection:
    :param run_type:
    :param fields:
    '''
    try:
        values = run_select_query(connection=connection, run_type=run_type, fields=fields)
        values = [val[0] for val in values]
        return values
    except:
        get_logger().error(f"Error while getting values of {fields} in {run_type} run type: " + " Error: " + str(sys.exc_info()[0]))


def get_author_id(connection: psycopg2.extensions.connection, author: str):
    '''Get id of a given author.
    '''
    constraint = f"WHERE author='{author}'"
    try:
        author_id = run_select_query(connection=connection, 
                                     run_type=AUTHOR_RT, 
                                     fields="id", 
                                     constraint=constraint)
        author_id = author_id[0][0]
        return author_id
    except:
        get_logger().error(f"Error while getting author id of {author} : " + " Error: " + str(sys.exc_info()[0]))


def get_author_field_id(connection: psycopg2.extensions.connection, author: str):
    '''Get field id of a given author.
    '''
    constraint = f"WHERE author='{author}'"
    try:
        field_id = run_select_query(connection=connection,
                                     run_type=AUTHOR_RT,
                                     fields="field_id",
                                     constraint=constraint)
        field_id = field_id[0][0]
        return field_id
    except:
        get_logger().error(f"Error while getting field id of {author} : " + " Error: " + str(sys.exc_info()[0]))
    

#--------------- Build queries ---------------#

def build_insert_query(table_name: str, records_to_insert: tuple):
    '''Build query for INSERT statement
    :param table_name: name of the table
    :param records_to_insert: records to insert into the given table
    '''
    try:
        insert_query = f"""INSERT INTO {table_name} VALUES {records_to_insert}"""
    except:
        get_logger().error(f"Error while building insert query for table {table_name} : " + " Error: " + str(sys.exc_info()[0]))

    return insert_query


def build_delete_query(table_name: str):
    pass


def build_select_query(table_name: str, fields: str):
    '''Build query for SELECT statement
    :param table_name: name of the table
    :param fields: fields want to be selected from the given table
    '''
    try:
        select_query = f"SELECT {fields} FROM {table_name} "
    except:
        get_logger().error(f"Error while building select query for table {table_name} : " + " Error: " + str(sys.exc_info()[0]))
    
    return select_query


#--------------- End of Build queries ---------------#


#--------------- Run queries ---------------#

def run_insert_query(connection: psycopg2.extensions.connection, run_type: str, records_to_insert: tuple):
    '''Execute the insert records query into PostgreSQL table.
    :param connection:
    :param run_type:
    :param records_to_insert:
    '''
    cursor = connection.cursor()
    table_name = get_table_name(run_type=run_type)
    insert_query = build_insert_query(table_name=table_name, records_to_insert=records_to_insert)

    try:
        cursor.execute(insert_query)
        connection.commit()
        get_logger().info(f"> Successfully ingested record {records_to_insert} into table {table_name}.\n")
    except:
        get_logger().error(f"Error while trying to insert records into table : {table_name} " + " Error: " + str(sys.exc_info()[0]))
    # finally:
    #     cursor.close()
    #     connection.close()


def run_delete_query(connection: psycopg2.extensions.connection, run_type: str):
    pass


def run_select_query(connection: psycopg2.extensions.connection, run_type: str, fields: str, constraint: str = None):
    '''Execute the select query into PostgreSQL table.
    :param connection:
    :param run_type:
    :param fields: field to be selected
    '''
    cursor = connection.cursor()
    table_name = get_table_name(run_type=run_type)
    select_query = build_select_query(table_name=table_name, fields=fields)

    if constraint is not None:
        select_query += constraint

    try:
        results = cursor.execute(select_query)
        results = cursor.fetchall()
        get_logger().info(f"> Successfully select {fields} from {table_name}.\n")
        return results
    except:
        get_logger().error(f"Error while trying to select {fields} from {table_name} : " + " Error: " + str(sys.exc_info()[0]))
    # finally:
    #     cursor.close()
    #     connection.close()

#--------------- End of Run queries ---------------#

#--------------- Cleaning data ---------------#

def remove_emoji(list_of_string: list):
    '''Remove emoji from text.
    :param list_of_string: 
    '''
    clean_list = []
    for string in list_of_string:
        clean_string = string.encode('ascii', 'ignore').decode('ascii')
        clean_list.append(clean_string)
    
    return clean_list


def escape_single_quote(list_of_string: list):
    '''Remove unwanted symbols.
    :param list_of_string:
    '''
    clean_list = []
    for string in list_of_string:
        clean_string = string.replace("\'", "''")
        clean_string = string.replace("\\", "")
        clean_list.append(clean_string)
    
    return clean_list


#--------------- End of Cleaning data ---------------#