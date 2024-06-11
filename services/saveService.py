import psycopg2
import datetime
from dotenv import dotenv_values

def connect_db (timeout=300000):
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

def check_author_existed(author_name, cur):
    query = "SELECT COUNT(*) FROM author WHERE name = %s"
    cur.execute(query, (author_name,))
    count = cur.fetchone()[0]
    return count > 0

def check_book_existed(book_id, cur):
    query = "SELECT COUNT(*) FROM book WHERE id = %s"
    cur.execute(query, (str(book_id),))
    count = cur.fetchone()[0]
    return count > 0

def check_author_to_book_existed(author_id, book_id, cur):
    query = "SELECT COUNT(*) FROM author_to_book WHERE author_id = %s AND book_id = %s"
    cur.execute(query, (str(author_id), str(book_id),))
    count = cur.fetchone()[0]
    return count > 0

def insert_authors(authors, conn, cur):
    query = "INSERT INTO author (name) VALUES (%s)"

    for author in authors:
        if not check_author_existed(author, cur):
            cur.execute(query, (author,))
            print(f"Author '{author}' have just been inserted into database.")
        else:
            print(f"Author '{author}' already exists in the database, skipping insertion.")

def insert_author_to_book(author_name, book_id, conn, cur):
    cur.execute("SELECT id FROM author WHERE name = %s", (author_name,))
    author_record = cur.fetchone()
    book_id = str(book_id)

    if author_record:
        author_id = author_record[0]
    else:
        cur.execute("INSERT INTO author (name) VALUES (%s) RETURNING id", (author_name,))
        author_id = cur.fetchone()[0]
    
    if not check_author_to_book_existed(author_id, book_id, cur):
        cur.execute("INSERT INTO author_to_book (author_id, book_id) VALUES (%s, %s)", (author_id, book_id))
        print(f"Record inserted into author_to_book for book_id {book_id} and author {author_name} successfully!")
    else:
        print(f"Record author_to_book for book_id {book_id} and author {author_name} has existed!")

def insert_author_to_book_from_dataframe(df, conn, cur):
    current_date = datetime.date.today().strftime("%Y-%m-%d")

    for index, row in df.iterrows():
        book_id = str(row.get('isbn', row.get('id')))
        authors = str(row.get('authors', row.get('author')))
        authors = authors.split(',')

        for author_name in authors:
            insert_author_to_book(author_name.strip(), book_id, conn, cur)
            print({'author_name': author_name, 'book_id': book_id})