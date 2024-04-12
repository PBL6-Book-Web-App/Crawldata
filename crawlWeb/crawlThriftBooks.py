import pandas as pd
import numpy as np
import requests
import json
from bs4 import BeautifulSoup
import datetime

def getNumberReviews(url_by_id):
    request_by_id = requests.get(url_by_id)
    content_by_id = request_by_id.content

    soup_by_id = BeautifulSoup(content_by_id, 'html.parser')
    scripts = soup_by_id.find_all('script')

    for script in scripts:
        script = script.get_text()
        if script.startswith('ReactDOM.hydrate'):
            start_index = script.find('{')
            last_index = script.rfind('}') + 1
            script_data = json.loads(script[start_index:last_index])
            number_of_ratings = script_data['work']['TotalRatingsCount']
            number_of_reviews = script_data['work']['TotalReviewsCount']
            return number_of_ratings, number_of_reviews


def getListBooksByPage(page):
    books = []

    url = f'https://www.thriftbooks.com/api/browse/Search'

    request_body = {
        "searchTerms": [
            ""
        ],
        "sortBy": "mostPopular",
        "sortDirection": "desc",
        "page": page,
        "itemsPerPage": "50",
        "displayType": 0,
        "languages": [
            40 # English
        ],
        "currentPage": page,
        "resultsPerPage": "50"
    }

    headers = {
        "Content-Type": "application/json"
    }

    r = requests.post(url=url, json=request_body, headers=headers)
    items = r.json()['works']

    for item in items:
        url_base = 'https://www.thriftbooks.com'
        url_api = 'https://www.thriftbooks.com/stateless/editions/workinfo'

        id = item['idWork']

        workUrl = item['workUrl']

        payload = {
            'workId': id
        }

        request_api = requests.post(url=url_api, data=payload)
        data = request_api.json()['Work']

        title = data.get('Title', '')
        description = data.get('Synopsis', '')
        image_url = data.get('ImageUrl', None)
        release_date = data['ActiveEdition'].get('ReleaseDate', None)
        publisher = data['ActiveEdition'].get('Publisher', None)
        number_of_pages = data['ActiveEdition'].get('NumberOfPages', None)
        price = data['ActiveEdition'].get('BuyNowPrice', None)
        book_cover = data['ActiveEdition'].get('Media', None)
        authors = ','.join(list(map(lambda x: x['AuthorName'], data['Authors'])))
        rating = data.get('WorkRating', None)

        url_page = url_base + '/w/' + workUrl + '/' + str(id)
        print('url_page: ', url_page)
        number_of_ratings, number_of_reviews = getNumberReviews(url_page)

        book = {
            'id': id,
            'title': title,
            'description': description,
            'book_cover': book_cover,
            'image_url': image_url,
            'release_date': release_date,
            'publisher': publisher,
            'number_of_pages': number_of_pages,
            'price': price,
            'authors': authors,
            'rating': rating,
            'number_of_ratings': number_of_ratings,
            'number_of_reviews': number_of_reviews
        }

        print('book: ', json.dumps(book, indent=4))
        books.append(book)

    return books


def save_to_csv(books):
    current_date = datetime.datetime.now()
    columns = ['id', 'title', 'description', 'book_cover', 'image_url', 'release_date', 'publisher', 'number_of_pages', 'price', 'authors', 'rating', 'number_of_ratings', 'number_of_reviews']
    df = pd.DataFrame(data = books, columns = columns)
    df.to_csv(f'../dataset/thrift-books/raw/thrift-books-{current_date}.csv', mode = 'a', header=False, index=False)

def execute():
    for i in range(200):
        page = i + 1
        print(f'==================Crawling Page {page}==================')
        books = getListBooksByPage(page)
        save_to_csv(books)

    return f'../dataset/thrift-books/raw/thrift-books-{datetime.datetime.now()}.csv'