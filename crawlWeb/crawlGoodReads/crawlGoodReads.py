import time
import requests
from bs4 import BeautifulSoup
import json
from crawlWeb.crawlGoodReads import crawlGoodReadsURL
import pandas as pd
import datetime

def get_books_data(books_url):
    # Tạo danh sách books
    books_info = []
    max_attempt = 10
    sleep_time = 10

    # Duyệt qua từng URL
    for book_info in books_url:
        url = book_info["book_link"]
        attempts = 0
        while attempts < max_attempt:  # Thử lại tối đa 5 lần
            try:
                # Mở trang web
                response = requests.get(url)

                soup = BeautifulSoup(response.text, "html.parser")

                details_string = soup.find("script", id="__NEXT_DATA__").text
                # Use json.loads for safe parsing
                details_dict = json.loads(details_string)

                # Get the 5th key in the dictionary
                keys = details_dict["props"]["pageProps"]["apolloState"].keys()
                # key startwith 'Book:' is the key we need
                keys = [k for k in keys if k.startswith("Book:")]
                key = keys[-1]
                book = details_dict["props"]["pageProps"]["apolloState"][key]

                isbn = book["details"]["isbn"]
                description = book['description({"stripped":true})']
                image_url = book["imageUrl"]
                publisher = book["details"]["publisher"]
                publish_date = book["details"]["publicationTime"]  # Timestamp
                language = book["details"]["language"]["name"]
                pages = book["details"]["numPages"]

                # Thêm thông tin của cuốn sách vào danh sách
                books_info.append(
                    {
                        "isbn": isbn,
                        "title": book_info["title"],
                        "description": description,
                        "image_url": image_url,
                        "author": book_info["author_name"],
                        "publisher": publisher,
                        "publish_date": publish_date,
                        "language": language,
                        "pages": pages,
                    }
                )

                break  # Nếu không có exception thì thoát khỏi vòng lặp

            except Exception as e:
                print(f"Error occurred for URL: {url}. Exception: {e}")
                attempts += 1
                time.sleep(sleep_time)  # Chờ trước khi thử lại
                if attempts == max_attempt - 1:
                    print(
                        f"Reached maximum number of attempts ({max_attempt}) for URL: {url}. Skipping to next URL."
                    )
    return books_info

def getDateTime():
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    return current_date

def execute():
    current_date = getDateTime()
    books_url = crawlGoodReadsURL.get_new_book_url()
    books_info = get_books_data(books_url)

    return books_info