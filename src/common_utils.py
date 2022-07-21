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
import re


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
DATE_RT = "date"
POST_RT = "post"
INGEST_RT = "ingest"
# Tables
AUTHOR_TABLE = "author_dimension"
DATE_TABLE = "date_dimension"
POST_TABLE = "posts_fact"

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
        get_logger().info("Successfully log in LinkedIn account with username: " + username)
        return driver
    except Exception as e:
        get_logger().error(e)
        # get_logger().error(f"Error while logging in Linkedin account {username}: "  + " Error: " + str(sys.exc_info()[0]))


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
        # soup.prettify()
    except:
        # get_logger().error(e)
        get_logger().error(f"Error while creating BeautifulSoup object for url {url}: "  + " Error: " + str(sys.exc_info()[0]))

    return soup

#--------------- End of Scraping utilities ---------------#


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
        elif run_type == DATE_RT:
            table_name = DATE_TABLE
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


def build_date_dimension_values():
    '''Build values for an insertion into date_dimension table.
    '''
    current_day = datetime.now().strftime("%d")
    current_month = datetime.now().strftime("%m")
    calendar_year = datetime.now().strftime("%Y")
    current_date = f"{calendar_year}{current_month}{current_day}" # also date_key
    calendar_month = datetime.now().strftime("%B")
    day_of_week = datetime.now().strftime("%A")
    full_date_des = f"{calendar_month} {current_day}, {calendar_year}"
    calendar_quarter = f"Q{int(current_month)//3 + 1}"
    
    if day_of_week in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        weekday_indicator = "Weekday"
    else:
        weekday_indicator = "Weekend"

    return (current_date, current_date, full_date_des, day_of_week, calendar_month, calendar_quarter, calendar_year, weekday_indicator)


def get_date_key(connection: psycopg2.extensions.connection):
    '''Get date_key for the current date.
    '''
    current_date = datetime.now().strftime("%Y%m%d")
    try:
        queried_date_keys = run_select_query(connection=connection,
                                             run_type=DATE_RT,
                                             fields="date_key")
        available_date_keys = []
        for date_keys in queried_date_keys:
            available_date_keys.append(date_keys[0])
        
        if current_date not in available_date_keys:
            new_date_dimension_row = build_date_dimension_values()
            run_insert_query(connection=connection, run_type=DATE_RT, records_to_insert=new_date_dimension_row)
    except:
        get_logger().error(f"Error while getting date key for date: {current_date} : " + " Error: " + str(sys.exc_info()[0]))
    
    return current_date


def get_author_key(connection: psycopg2.extensions.connection, author: str):
    '''Get author_key of a given author.
    :param connection: an established connection to the server to run query against the database
    :param 
    '''
    constraint = f"WHERE author_name='{author}'"
    try:
        author_key = run_select_query(connection=connection, 
                                     run_type=AUTHOR_RT, 
                                     fields="author_key", 
                                     constraint=constraint)
        author_key = author_key[0][0]
        return author_key
    except:
        get_logger().error(f"Error while getting author key of {author} : " + " Error: " + str(sys.exc_info()[0]))


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
    :param connection: an established connection to the server
    :param run_type: any of [AUTHOR_RT, DATE_RT, POST_RT, LINK_RT]
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
        get_logger().error(f"Error while trying to insert: {records_to_insert}.")
        # get_logger().error(f"Error while trying to insert records into table : {table_name} " + " Error: " + str(sys.exc_info()[0]))
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


def escape_single_quote(list_of_string: list) -> list:
    '''Remove unwanted symbols.
    :param list_of_string:
    '''
    clean_list = []
    for string in list_of_string:
        clean_string = string.replace("\'", "''")
        clean_string = string.replace("\\", "")
        clean_list.append(clean_string)
    
    return clean_list


def remove_hashtags(list_of_string: list) -> list:
    '''Remove hashtags from a list of strings.
    :param list_of_string:
    '''
    clean_list = [re.sub("(\#[a-zA-Z0-9]+\\b)", "", string) for string in list_of_string]
    return clean_list


def extract_hashtags(list_of_string: list) -> list:
    '''Extract hashtags from a list of strings.
    :param list_of_string:
    '''
    hashtags = [re.findall("(\#[a-zA-Z0-9]+\\b)", string) for string in list_of_string]
    return hashtags


def remove_mentions(list_of_string: list) -> list:
    '''Remove mentions from a list of strings.
    :param list_of_string:
    '''
    clean_list = [re.sub("(\@[a-zA-Z0-9]+\\b)", "", string) for string in list_of_string]
    return clean_list


def extract_mentions(list_of_string: list) -> list:
    '''Extract mentions from a list of strings.
    :param list_of_string:
    '''
    mentions = [re.findall("(\@[a-zA-Z0-9]+\\b)", string) for string in list_of_string]
    return mentions


def remove_links(list_of_string: list) -> list:
    '''Remove links from a list of strings.
    :param list_of_string:
    '''
    clean_list = [re.sub("((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)", "", string) for string in list_of_string]
    return clean_list


# def extract_links(list_of_string: list) -> list:
#     '''Extract links from a list of strings.
#     :param list_of_string:
#     '''
#     links = [re.findall("((https?):((//)|(\\\\))+([\w\d:#@%/;$()~_?\+-=\\\.&](#!)?)*)", string) for string in list_of_string]
#     return links


#--------------- End of Cleaning data ---------------#