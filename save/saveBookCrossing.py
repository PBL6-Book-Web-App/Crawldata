import re
from services import saveService
from datetime import date, datetime

source_id = 4  # id for source from Book-Crossing Books


def is_valid_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")  # Assuming YYYY-MM-DD format
        return True
    except ValueError:
        return False


def get_number(number_str):
    try:
        num = int(number_str)
    except ValueError:
        num = None
    return num


def insert_book(row, conn, cur):
    print(
        f"book = { row.title } ### check_book_existed = {saveService.check_book_existed(row['isbn'], cur)}"
    )

    if not saveService.check_book_existed(row["isbn"], cur):
        query = """
            INSERT INTO book (
                id, title, description, book_cover, image_url, release_date,
                publisher, number_of_pages, average_rating, number_of_ratings,
                source_id, preprocessed_description, categories
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # release_date = row.get("release_date", None)
        # if is_valid_date(str(release_date)) == False or release_date is None:
        #     release_date = None
        release_day = row.get("publication_day", "1")
        release_month = row.get("publication_month", "1")
        release_year = row.get("publication_year", "1")
        release_date = None
        if release_year != "1" and is_valid_date(
            f"{release_year}-{release_month}-{release_day}"
        ):
            release_date = f"{release_year}-{release_month}-{release_day}"

        number_of_pages = row.get("num_pages", None)
        number_of_pages = get_number(number_of_pages)

        average_rating = row.get("average_rating", None)
        average_rating = get_number(average_rating)

        number_of_ratings = row.get("ratings_count", None)
        number_of_ratings = get_number(number_of_ratings)

        book_id = row.get("isbn", None)

        categories = row.get("categories", None)
        # ['History, Biology']
        if categories is not None:
            categories = re.sub(r"[^\w\s,]", "", categories)

        if book_id:
            book = (
                str(row["isbn"]),  # id
                row["title"],
                row["description"],
                row["format"],
                row["image_url_backup"],
                release_date,
                row.get("publisher"),
                number_of_pages,
                average_rating,
                number_of_ratings,
                # row['text_reviews_count'],
                str(source_id),
                row["preprocessed_description"],
                categories,
            )

            print(book)

            cur.execute(query, book)

            print(f"Book '{row['title']}' inserted successfully!")

            return row
        print(f"Book '{row['title']}' has no id!")

    else:
        print(f"Book '{row['title']}' already existed!")


def execute(df):
    all_authors = set()
    for authors_str in df["authors"].dropna():
        if isinstance(authors_str, str):
            authors_list = authors_str.split(",")
            all_authors.update(
                [author.strip() for author in authors_list if author.strip() != ""]
            )

    # # Save authors list
    conn, cur = saveService.connect_db(timeout=30000)
    try:
        saveService.insert_authors(all_authors, conn, cur)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error: ", e)
    finally:
        cur.close()
        conn.close()

    # Save books list
    conn, cur = saveService.connect_db(timeout=30000)
    try:
        for _, row in df.iterrows():
            insert_book(row, conn, cur)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error book: ", e)
    finally:
        cur.close()
        conn.close()

    # Save author book relationship
    conn, cur = saveService.connect_db(timeout=3000000)

    try:
        saveService.insert_author_to_book_from_dataframe(df, conn, cur)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error: ", e)
    finally:
        cur.close()
        conn.close()
