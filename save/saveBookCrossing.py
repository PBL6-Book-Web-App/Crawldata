import psycopg2
import datetime
from dotenv import dotenv_values

def check_author_existed(author_name, cur):
    query = "SELECT COUNT(*) FROM author WHERE name = %s"
    cur.execute(query, (author_name,))
    count = cur.fetchone()[0]
    return count > 0

def check_book_existed(book_id, cur):
    query = "SELECT COUNT(*) FROM book WHERE id = %s"
    cur.execute(query, (book_id,))
    count = cur.fetchone()[0]
    return count > 0

def insert_authors(authors, conn, cur):
    query = "INSERT INTO author (name) VALUES (%s)"

    for author in authors:
        print(f'author = {author} ### check_author_existed = {check_author_existed(author, cur)}' )
        if not check_author_existed(author, cur):
            cur.execute(query, (author,))
#                 print(f"Author '{author}' inserted successfully!")
        else:
            print(f"Author '{author}' already exists in the database, skipping insertion.")

def insert_book(row, conn, cur):
    source_id = 4 # id for source from Book-Crossing Books 

    print(f'book = { row.title } ### check_book_existed = {check_book_existed(row.id, cur)}' )

    if not check_book_existed(row['id'], cur):
        query = f'''INSERT INTO "book" ("id", "title", "description", "book_cover", "image_url", "release_date", "publisher", "number_of_pages", "price", "average_rating", "number_of_ratings", "number_of_reviews", "source_id", "preprocessed_description", "categories")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        day = row.get('publication_day', 1)
        month = row.get('publication_month', 1)
        year = row.get('publication_year')

        release_date = None

        if year:
            release_date = datetime.datetime(year, month, day)

        cur.execute(query, (
            row['isbn'],
            row['title'],
            row['description'],
            row['format'],
            row['image_url'],
            release_date,
            row['publisher'],
            row['num_pages'],
            row['average_rating'],
            row['ratings_count'],
            row['text_reviews_count'],
            source_id,
            row['preprocessed_description'],
            row['categories']
        ))

        return row


def insert_author_to_book(author_name, book_id, conn, cur):
    cur.execute("SELECT id FROM author WHERE name = %s", (author_name,))
    author_record = cur.fetchone()

    if author_record:
        author_id = author_record[0]
    else:
        cur.execute("INSERT INTO author (name) VALUES (%s) RETURNING id", (author_name,))
        author_id = cur.fetchone()[0]

    cur.execute("INSERT INTO author_to_book (author_id, book_id) VALUES (%s, %s)", (author_id, book_id))
    print(f"Record inserted into author_to_book for book_id {book_id} and author {author_name} successfully!")

def insert_author_to_book_from_dataframe(df, conn, cur):
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    cur.execute(f"SELECT id FROM book WHERE DATE(created_at) = '{current_date}' ORDER BY created_at")
    book_ids = [row[0] for row in cur.fetchall()]

    for index, row in df.iterrows():
        book_id = book_ids[index]
        authors = str(row['authors'])
        authors = authors.split(',')

        for author_name in authors:
            insert_author_to_book(author_name.strip(), book_id, conn, cur)

    print("Records inserted into author_to_book successfully!")

def connect_db (timeout=30000):
    env_vars = dotenv_values(".env")

    db_host = env_vars["DB_HOST"]
    db_port = env_vars["DB_PORT"]
    db_user = env_vars["DB_USER"]
    db_pass = env_vars["DB_PASS"]
    db_name = env_vars["DB_NAME"]

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port,
        connect_timeout = timeout
    )

    cur = conn.cursor()

    return conn, cur

def execute(df):
    conn, cur = connect_db(timeout=30000)

    books = []
    all_authors = set()
    for authors_str in df['authors'].dropna():
        if isinstance(authors_str, str):
            authors_list = authors_str.split(',')
            all_authors.update([author.strip() for author in authors_list if author.strip() != ''])

    try:
        insert_authors(all_authors, conn, cur)

        for _, row in df.iterrows():
            inserted_book = insert_book(row, conn, cur)
            books.append(inserted_book)

        insert_author_to_book_from_dataframe(df, conn, cur)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error: ", e)
    finally:
        cur.close()
        conn.close()
        return books
