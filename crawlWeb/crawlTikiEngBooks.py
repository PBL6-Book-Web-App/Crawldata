import pandas as pd
import requests
import re
from bs4 import BeautifulSoup

def is_english(text):
    non_english_regex = re.compile(r'[^\x00-\x7F]+')
    match = non_english_regex.search(text)
    return not match


def get_book_by_id(id):
    url_get_by_id = f'https://tiki.vn/api/v2/products/{id}'

    book = requests.get(url_get_by_id, headers=headers).json()

    soup = BeautifulSoup(book['description'], 'html.parser')
    description = soup.get_text()

    filterd_authors = filter(lambda x: 'name' in x, book.get('authors', []))
    authors = ','.join(map(lambda x: x['name'], filterd_authors))

    publisher = ''
    publication_date = ''
    manufacturer = ''
    book_cover = ''
    number_of_page = 0

    specifications = book['specifications']
    for spec in specifications:
        if spec.get('name') == 'Thông tin chung':
            attributes = spec.get('attributes')

            for attr in attributes:
                if (attr['code'] == 'publisher_vn'):
                    publisher = attr['value']

                if (attr['code'] == 'publication_date'):
                    publication_date = attr['value']

                if (attr['code'] == 'book_cover'):
                    book_cover = attr['value']

                if (attr['code'] == 'number_of_page'):
                    number_of_page = attr['value']

                if (attr['code'] == 'manufacturer'):
                    manufacturer = attr['value']

    return description, authors, publisher, publication_date, manufacturer, book_cover, number_of_page



def get_list_books_by_page(data):
    books = []

    for item in data:
        name = item['name']

        if is_english(name) == True:
            id = item['id']
            price = item['price']
            original_price = item['original_price']
            discount = item['discount']
            discount_rate = item['discount_rate']
            rating_average = item['rating_average']
            review_count = item['review_count']
            thumbnail_url = item['thumbnail_url']
            try:
                quantity_sold = item['quantity_sold']['value']
            except (KeyError, TypeError):
                quantity_sold = 0

    #         category_l1_name = item['visible_impression_info']['amplitude'].get('category_l1_name', '') # this is always NhaSachTiki
            category_l2_name = item['visible_impression_info']['amplitude'].get('category_l2_name', '')
            category_l3_name = item['visible_impression_info']['amplitude'].get('category_l3_name', '')
            primary_category_name = item['visible_impression_info']['amplitude'].get('primary_category_name', '')
            categories = [category_l2_name, category_l3_name, primary_category_name]
            categories = list(filter(lambda x: x != '', categories))

            description, authors, publisher, publication_date, manufacturer, book_cover, number_of_page = get_book_by_id(str(id))

            book = {
                'id': id,
                'name': name,
                'price': price,
                'original_price': original_price,
                'discount': discount,
                'discount_rate': discount_rate,
                'rating_average': rating_average,
                'review_count': review_count,
                'thumbnail_url': thumbnail_url,
                'quantity_sold': quantity_sold,
                'categories': categories,
                'description': description,
                'authors': authors,
                'publication_date': publication_date,
                'manufacturer': manufacturer,
                'number_of_page': number_of_page
            }
            books.append(book)
    return books

def execute():
    url_list = 'https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category=320&urlKey=sach-tieng-anh'

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Content-Type': 'application/json'
    }

    i = 1
    books = []

    while True:
        print(f'==================Crawling Page {i}==================')
        url_list_page = url_list + f'&page={i}'
        i = i+1

        r = requests.get(url_list_page, headers=headers)
        data = r.json()['data']

        if data == []:
            print('End')
            break

        list_books_by_page = get_list_books_by_page(data)

        books.extend(list_books_by_page)
        print(len(books))


    columns = ['id', 'name', 'price', 'original_price', 'discount', 'discount_rate', 'rating_average', 'review_count', 'thumbnail_url', 'quantity_sold', 'categories', 'description', 'authors', 'publisher', 'publication_date', 'manufacturer', 'book_cover', 'number_of_page']
    df_en_tiki = pd.DataFrame(data = books, columns = columns)
    df_en_tiki
