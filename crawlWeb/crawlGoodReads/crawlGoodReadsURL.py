import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
import re
import requests

def get_remaining_books_url_by_request(next_page_token, current_year, current_month):
    # remove 0 before month
    if current_month[0] == "0":
        current_month = current_month[1]
    url = (
        "https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql"
    )
    payload = {
        "operationName": "getTopList",
        "variables": {
            "name": f"books-by-release-date-{current_year}-{current_month}",
            "period": "A",
            "location": "ALL",
            "limit": 185,
            "nextPageToken": f"{next_page_token}",
        },
        "query": "query getTopList($name: String!, $period: String!, $location: String!, $nextPageToken: String, $limit: Int) {\n  getTopList(\n    getTopListInput: {name: $name, period: $period, location: $location}\n    pagination: {after: $nextPageToken, limit: $limit}\n  ) {\n    name\n    period\n    location\n    pageInfo {\n      hasNextPage\n      nextPageToken\n      __typename\n    }\n    edges {\n      ... on TopListBookEdge {\n        rank\n        count\n        node {\n          ...BasicBookFragment\n          viewerShelving {\n            ...ViewerShelvingFragmentWTR\n            __typename\n          }\n          primaryContributorEdge {\n            ...BasicContributorFragment\n            __typename\n          }\n          secondaryContributorEdges {\n            ...BasicContributorFragment\n            __typename\n          }\n          work {\n            stats {\n              ratingsCount\n              textReviewsCount\n              averageRating\n              __typename\n            }\n            id\n            viewerShelvings {\n              ...ViewerShelvingFragmentWTR\n              __typename\n            }\n            __typename\n          }\n          ...ShelvedMultipleExclusiveFragment\n          __typename\n        }\n        __typename\n      }\n      ... on TopListUserEdge {\n        rank\n        count\n        node {\n          name\n          imageUrl\n          __typename\n        }\n        __typename\n      }\n      ... on TopListWorkEdge {\n        rank\n        count\n        node {\n          id\n          details {\n            bestBook {\n              titleComplete\n              ...BasicBookFragment\n              viewerShelving {\n                ...ViewerShelvingFragmentWTR\n                __typename\n              }\n              primaryContributorEdge {\n                ...BasicContributorFragment\n                __typename\n              }\n              secondaryContributorEdges {\n                ...BasicContributorFragment\n                __typename\n              }\n              work {\n                stats {\n                  ratingsCount\n                  textReviewsCount\n                  averageRating\n                  __typename\n                }\n                viewerShelvings {\n                  ...ViewerShelvingFragmentWTR\n                  __typename\n                }\n                __typename\n              }\n              ...ShelvedMultipleExclusiveFragment\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment BasicBookFragment on Book {\n  id\n  legacyId\n  imageUrl\n  description\n  title\n  titleComplete\n  webUrl\n  __typename\n}\n\nfragment BasicContributorFragment on BookContributorEdge {\n  node {\n    id\n    name\n    webUrl\n    isGrAuthor\n    __typename\n  }\n  role\n  __typename\n}\n\nfragment ViewerShelvingFragmentWTR on Shelving {\n  id\n  book {\n    id\n    __typename\n  }\n  shelf {\n    name\n    webUrl\n    __typename\n  }\n  taggings {\n    tag {\n      name\n      webUrl\n      __typename\n    }\n    __typename\n  }\n  webUrl\n  __typename\n}\n\nfragment ShelvedMultipleExclusiveFragment on Book {\n  reviewEditUrl\n  work {\n    id\n    editions {\n      webUrl\n      __typename\n    }\n    viewerShelvingsUrl\n    __typename\n  }\n  __typename\n}\n",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "X-Api-Key": "da2-xpgsdydkbregjhpr6ejzqdhuwy",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", url, headers=headers, json=payload, timeout=10)

    books_info = []
    if response.status_code == 200:
        data = response.json()
        books = data["data"]["getTopList"]["edges"]

        for book in books:
            book_link = book["node"]["webUrl"]
            title = book["node"]["title"]
            author_link = book["node"]["primaryContributorEdge"]["node"]["webUrl"]
            author_name = book["node"]["primaryContributorEdge"]["node"]["name"]

            books_info.append(
                {
                    "book_link": book_link,
                    "title": title,
                    "author_link": author_link,
                    "author_name": author_name,
                }
            )

        return books_info
    else:
        print(f"Failed to get book URLs, status code: {response.status_code}")

def get_new_book_url():
    current_month = datetime.datetime.now().strftime("%m")
    current_year = datetime.datetime.now().strftime("%Y")

    response = requests.get(
        f"https://www.goodreads.com/book/popular_by_date/{current_year}/{current_month}"
    )
    soup = BeautifulSoup(response.text, "html.parser")

    # Tạo một danh sách để lưu thông tin sách
    books_url = []

    # Lặp qua từng phần tử <article class="BookListItem">
    for book_item in soup.find_all("article", class_="BookListItem"):
        # Lấy href của quyển sách
        book_link = book_item.find("a", class_="BookCard__clickCardTarget")["href"]

        # Lấy tiêu đề của quyển sách
        title = book_item.find("h3", class_="Text__title3").text.strip()

        # Lấy contributor link
        author_link = book_item.find("a", class_="ContributorLink")["href"]

        # Lấy tên của contributor và loại bỏ khoảng trắng thừa
        author_name = book_item.find(
            "span", class_="ContributorLink__name"
        ).text.strip()
        author_name = " ".join(author_name.split())

        # Thêm thông tin sách vào danh sách books_info
        books_url.append(
            {
                "book_link": book_link,
                "title": title,
                "author_link": author_link,
                "author_name": author_name,
            }
        )

    next_page_token_script = soup.find("script", id="__NEXT_DATA__").text
    # Tìm thông tin nextPageToken
    match = re.search(r'"nextPageToken":"([^"]+)"', next_page_token_script)
    next_page_token = match.group(1)
    
    books_url.extend(
        get_remaining_books_url_by_request(next_page_token, current_year, current_month)
    )
    return books_url
