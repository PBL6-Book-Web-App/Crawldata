import psycopg2
import datetime

from dotenv import dotenv_values

def check_author_existed(author_name, cur):
    query = "SELECT COUNT(*) FROM author WHERE name = '%s'"
    cur.execute(query, (author_name,))
    count = cur.fetchone()[0]
    return count > 0

def insert_authors_into_postgresql(authors, db_info):
    db_host, db_port, db_user, db_pass, db_name = db_info

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )

    cur = conn.cursor()

    try:
        query = "INSERT INTO author (name) VALUES (%s)"

        for author in authors:
            print('check_author_existed = ', check_author_existed(author, cur))
            if not check_author_existed(author, cur):
                cur.execute(query, (author,))
#                 print(f"Author '{author}' inserted successfully!")
            else:
                print(f"Author '{author}' already exists in the database, skipping insertion.")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error inserting authors:", e)
    finally:
        cur.close()
        conn.close()


def insert_book(row, db_info):
    source_id = 'd42f914c-6fb9-43df-b938-b257516f41d7' # id for source from Thrift Books

    db_host, db_port, db_user, db_pass, db_name = db_info

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )

    cur = conn.cursor()

    try:
        query = f'''INSERT INTO "book" ("title", "description", "book_cover", "image_url", "release_date", "publisher", "number_of_pages", "price", "average_rating", "number_of_ratings", "number_of_reviews", "source_id")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        cur.execute(query, (
            row['title'],
            row['description'],
            row['book_cover'],
            row['image_url'],
            row['release_date'],
            row['publisher'],
            row['number_of_pages'],
            row['price'],
            row['rating'],
            row['number_of_ratings'],
            row['number_of_reviews'],
            source_id
        ))

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error inserting row:", e)
    finally:
        cur.close()
        conn.close()


def insert_author_to_book(conn, cur, author_name, book_id):
    try:
        cur.execute("SELECT id FROM author WHERE name = %s", (author_name,))
        author_record = cur.fetchone()

        if author_record:
            author_id = author_record[0]
        else:
            cur.execute("INSERT INTO author (name) VALUES (%s) RETURNING id", (author_name,))
            author_id = cur.fetchone()[0]

        cur.execute("INSERT INTO author_to_book (author_id, book_id) VALUES (%s, %s)", (author_id, book_id))
        conn.commit()
        print(f"Record inserted into author_to_book for book_id {book_id} and author {author_name} successfully!")
    except Exception as e:
        conn.rollback()
        print("Error inserting record into author_to_book:", e)

def insert_author_to_book_from_dataframe(df, conn, cur):
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    try:
        cur.execute(f"SELECT id FROM book WHERE DATE(created_at) = '{current_date}' ORDER BY created_at")
        book_ids = [row[0] for row in cur.fetchall()]

        for index, row in df.iterrows():
            book_id = book_ids[index]
            authors = str(row['authors'])
            authors = authors.split(',')

            for author_name in authors:
                insert_author_to_book(conn, cur, author_name.strip(), book_id)

        print("Records inserted into author_to_book successfully!")
    except Exception as e:
        conn.rollback()
        print("Error:", e)

def execute(df):
    env_vars = dotenv_values(".env")

    db_host = env_vars["DB_HOST"]
    db_port = env_vars["DB_PORT"]
    db_user = env_vars["DB_USER"]
    db_pass = env_vars["DB_PASS"]
    db_name = env_vars["DB_NAME"]

    db_info = (db_host, db_port, db_user, db_pass, db_name)

    all_authors = set()
    df['authors'].fillna('')
    for authors_str in df['authors']:
        if isinstance(authors_str, str):
            authors_list = authors_str.split(',')
            all_authors.update([author.strip() for author in authors_list if author.strip() != ''])

    insert_authors_into_postgresql(all_authors, db_info)

    for _, row in df.iterrows():
        insert_book(row, db_info)

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host,
        port=db_port
    )

    cur = conn.cursor()

    insert_author_to_book_from_dataframe(df, conn, cur)

    cur.close()
    conn.close()