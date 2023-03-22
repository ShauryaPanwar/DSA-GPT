import sqlite3
import os

class AmazonDatabaseConnector:
    def __init__(self, stamp):
        self.dbPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../medium.db")
        self.conn = sqlite3.connect(self.dbPath)
        self.cur = self.conn.cursor()
        self.welcomeMessage = "Welcome to Amazon Scraper. This is the database for the Amazon Scraper. This database was created on {}.".format(stamp)

    def schemaMaker(self):
        # creating tables
        self.cur.execute("""CREATE TABLE blogs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Blog_title TEXT NOT NULL,
            Blog_content TEXT NOT NULL,
            Blog_subheading TEXT NOT NULL
        );""")
        self.conn.commit()
    
    def insertProduct(self, productDetails):
        self.cur.execute("INSERT INTO blogs (Blog_title, Blog_content, Blog_subheading) VALUES (?, ?, ?)", (productDetails["Blog_title"], productDetails["Blog_content"], productDetails["Blog_subheading"]))
        self.conn.commit()

    def fetchAllProducts(self):
        self.cur.execute("SELECT * FROM blogs")
        return self.cur.fetchall()

    def clearDatabase(self):
        self.cur.execute("DELETE FROM blogs")
        self.conn.commit()
        
    
    def removeDuplicates(self):
        self.cur.execute("DELETE FROM products WHERE rowid NOT IN (SELECT MIN(rowid) FROM products GROUP BY sku)")
        self.conn.commit()