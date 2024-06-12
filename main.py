import schedule
import time
from crawlWeb import crawlThriftBooks
from crawlWeb.crawlGoodReads import crawlGoodReads
import preprocessData
import pandas as pd
from save import saveThriftBooks, saveBookCrossing, saveGoodReads
from datetime import date

def getThriftBooks():
    rawFilePath = crawlThriftBooks.execute()
    df = preprocessData.executeByAttribute(rawFilePath=rawFilePath, attribute='description', sourceId = 1)
    print(df)
    inserted_books = saveThriftBooks.execute(df)
    return inserted_books

def getBookCrossingBooks():
    rawFilePath = r'dataset/book-crossing/raw/1st_book_crossing_data.csv'
    df = preprocessData.executeByAttribute(rawFilePath=rawFilePath, attribute='description', sourceId = 4)
    print(df)

    inserted_books = saveBookCrossing.execute(df)
    return inserted_books

def getGoodReads() :
    rawFilePath = crawlGoodReads.execute()
    # rawFilePath = r'D:\Jasmine Data\SuplementarySpaceForOneDrive\PBL7-SRC\crawl-data\goodreads-2024-06-11.csv'
    df = preprocessData.executeByAttribute(rawFilePath=rawFilePath, attribute='description', sourceId = 3)
    print(df)
    saveGoodReads.execute(df)

def getData():
    if date.today().day != 1:
        return

    getThriftBooks()
    # getBookCrossingBooks() # dont crawl this periodically
    getGoodReads()
    print("Task executed!")


def main():
    schedule.every().day.at("00:00").do(getData)

    while True:
        schedule.run_pending()
        time.sleep(60)  # sleep for 60s before checking again

if __name__ == "__main__":
    main()