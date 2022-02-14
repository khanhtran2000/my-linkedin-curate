from dotenv import load_dotenv
import os
import sys

import common_utils as cu


load_dotenv()
class DataScraper():
    def __init__(self):
        self.logger = cu.create_log()
        self.driver = cu.login_linkedin()
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
    
    def scrape_containers(self, author_url: str):
        soup = cu.create_soup(driver=self.driver, url=author_url)
        containers = soup.find_all("div", {"class":"occludable-update ember-view"})
        return containers


class LinkedinScraper(DataScraper):
    def scrape_content_text(self, author_url: str) -> list:
        '''Return content text to ingest into PostgreSQL database.
        :param author_url: Linkedin profile link.
        '''
        post_texts = []
        
        for container in self.scrape_containers(author_url=author_url):
            try:
                text_box = container.find("div", {"class":"feed-shared-text relative feed-shared-update-v2__commentary"})
                text = text_box.find("span", {"dir":"ltr"})

                #Appending text to lists
                post_texts.append(text.text)
            except:
                self.logger.error(f"Error while scraping post text from URL {author_url} : " + " Error: " + str(sys.exc_info()[0]))
        
        return post_texts
    
    def scrape_media(self, author_url: str) -> list:
        '''Return media links and corresponding types.
        :param author_url: Linkedin profile link.
        '''
        media_links = []
        media_types = []

        for container in self.scrape_containers(author_url=author_url):
            try:
                image_box = container.find_all("div",{"class": "feed-shared-image__container"})
                image_link = image_box[0].find("img", {"class":"ivm-view-attr__img--centered feed-shared-image__image lazy-image ember-view"})
                media_links.append(image_link['src'])
                media_types.append("Image")
            except:
                try:
                    article_box = container.find_all("div",{"class": "feed-shared-article__description-container"})
                    article_link = article_box[0].find('a', href=True)
                    media_links.append(article_link['href'])
                    media_types.append("Article")
                except:
                    try:
                        video_box = container.find_all("div",{"class": "feed-shared-external-video__meta"})          
                        video_link = video_box[0].find('a', href=True)
                        media_links.append(video_link['href'])
                        media_types.append("Youtube Video")   
                    except:
                        try:
                            poll_box = container.find_all("div",{"class": "feed-shared-update-v2__content overflow-hidden feed-shared-poll ember-view"})
                            media_links.append("None")
                            media_types.append("Other: Poll, Shared Post, etc")
                        except:
                            media_links.append("None")
                            media_types.append("Unknown")
        
        return media_links, media_types

    def scrape_data(self) -> dict:
        '''Return text, links, and media types.
        '''
        author_posts = {}
        author_urls = cu.get_attribute_values(connection=self.connect_to_postgres(), run_type=cu.AUTHOR_RT, fields="url")
        author_names = cu.get_attribute_values(connection=self.connect_to_postgres(), run_type=cu.AUTHOR_RT, fields="author")
        for n, author_url in enumerate(author_urls):
            texts = self.scrape_content_text(author_url=author_url)
            media_links = self.scrape_media(author_url=author_url)[0]
            media_types = self.scrape_media(author_url=author_url)[1]

            author_posts.update({author_names[n]:[texts, media_links, media_types]})
            
            self.logger.info(f"> Successfully scraped new posts of author {author_names[n]}.")

        return author_posts


class OtherScraper(DataScraper):
    pass