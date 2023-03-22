import asyncio
from multiprocessing import Pool
import logging
import os
from datetime import datetime
import lxml.html as html
import pandas as pd
import time
import warnings
import json
from htmlLib import SeleniumScraper
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
from productList import productList
from dbconnector import AmazonDatabaseConnector



SeleniumScraper = SeleniumScraper()

warnings.filterwarnings("ignore")

class Scraper:
    def __init__(self):
        self.stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.storagePath = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../"
        )

        logging.basicConfig(
            filename=self.storagePath + "logs/Scraper_{}.log".format(self.stamp),
            level=logging.INFO,
            filemode="w",
        )
        self.website = "https://medium.com/search?q="
        self.websiteName = "https://medium.com"
        self.blogLinksXpath = '//*[@aria-label="Post Preview Title"]//@href'
        self.blogTitleXpath = '//h1//text()'
        self.blogContentXpath = '//section//p//text()'
        self.dfList = []
    
    def getBlogLinks(self, keyword):
        # Get blog links
        driver = SeleniumScraper.get_selenium_driver()

        self.website = self.website + keyword
        try:
            response = driver.get(self.website)
            response = driver.page_source
            doc = html.fromstring(response)
            print("Request fetched successfully for url: {}".format(self.website))

        except Exception as e:
            logging.error("Error in fetching request for url: {} and error: {}".format(self.website, e))
            return None
        
        try:
    
            bloglinks = SeleniumScraper.get_xpath_link(doc, self.blogLinksXpath, self.websiteName)
            logging.info("Blog links fetched successfully for url: {}".format(self.website))

            if bloglinks is None:
                logging.error("Error in fetching blog links for url: {}".format(self.website))
                return None

        except Exception as e:
            if bloglinks is None:
                logging.error("Error in fetching blog links for url: {} and error: {}".format(self.website, e))
                return None
            
        return bloglinks

    def getBlogContent(self, blogLink):
        # Get blog content
        blogDetails = {}

        driver = SeleniumScraper.get_selenium_driver()
        time.sleep(2)
        try:
            response = driver.get(blogLink)
            response = driver.page_source
            doc = html.fromstring(response)
            print("Request fetched successfully for url: {}".format(blogLink))

        except Exception as e:
            logging.error("Error in fetching request for url: {} and error: {}".format(blogLink, e))
            return None
        
        try:
            blogTitle = SeleniumScraper.get_xpath_data(doc, self.blogTitleXpath)[0]
            blogDetails["Blog_title"] = blogTitle
            blogSubheading = blogTitle[1:-1]

            blogDetails["Blog_subheading"] = blogSubheading
            
        except Exception as e:
            logging.error("Error in fetching blog title for url: {} and error: {}".format(blogLink, e))
            blogDetails["Blog_title"] = ''
            blogDetails["Blog_subheading"] = ''

        blogContent = SeleniumScraper.get_xpath_data(doc, self.blogContentXpath)
        blogContent = " ".join(blogContent)
        blogDetails["Blog_content"] = blogContent            



        return blogDetails

    def main(self, keyword):
        blogLinks = self.getBlogLinks(keyword)
        if blogLinks is None:
            logging.error("Error in fetching blog links for keyword: {}".format(keyword))
            return None

        print(blogLinks)
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(self.getBlogContent, blogLinks)
            for result in results:
                self.db.insertProduct(result)



if __name__ == "__main__":
    number_of_threads=5
    scraper = Scraper()   

    # make db amazon.db if it doesn't exist
    if not os.path.exists(scraper.storagePath + "medium.db"):
        print(f'Creating amazon.db at {scraper.storagePath+"medium.db"}')
        db = AmazonDatabaseConnector(scraper.stamp)
        db.schemaMaker()
    
    scraper.db = AmazonDatabaseConnector(scraper.stamp)

    for keyword in productList:
        scraper.main(keyword)
        scraper.website = "https://medium.com/search?q="    
