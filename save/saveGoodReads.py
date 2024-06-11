from services import saveService
from datetime import datetime

source_id = 3 # id for source from GoodReads Books 

def convert_timestamp_to_date(timestamp_ms):
    if timestamp_ms is None:
        return None
        
    timestamp_s = timestamp_ms / 1000 # Convert milliseconds to seconds

    date_time = datetime.fromtimestamp(timestamp_s)

    formatted_date = date_time.strftime("%Y-%m-%d")

    return formatted_date

def is_valid_date(date_str):
  try:
    datetime.strptime(date_str, "%Y-%m-%d") 
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
    print(f'book = { row.title } ### check_book_existed = {saveService.check_book_existed(row.isbn, cur)}' )

    if not saveService.check_book_existed(row['isbn'], cur):
        release_date = convert_timestamp_to_date(row.get('publish_date', None)) 
        if is_valid_date(str(release_date)) == False or release_date is None:
            release_date = None

        number_of_pages = row.get('pages', None)
        number_of_pages = get_number(number_of_pages)

        book_id = row.get('isbn', None)

        if book_id:
            query = """
                INSERT INTO book (
                    id, title, description, image_url, release_date, 
                    publisher, number_of_pages,
                    source_id, preprocessed_description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            book = (
                str(row['isbn']),
                row['title'],
                row['description'],
                row['image_url'],
                release_date,
                row.get('publisher'),
                number_of_pages,
                str(source_id),
                row['preprocessed_description']
            )
            print(book )
            cur.execute(query, book)
            print(f"Book '{row['title']}' inserted successfully!")
            return row
        else: 
            query = """
                INSERT INTO book (
                    title, description, image_url, release_date, 
                    publisher, number_of_pages,
                    source_id, preprocessed_description
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            book = (
                row['title'],
                row['description'],
                row['image_url'],
                release_date,
                row.get('publisher'),
                number_of_pages,
                str(source_id),
                row['preprocessed_description']
            )
            print(book )
            cur.execute(query, book)
            print(f"Book '{row['title']}' inserted successfully!")
            return row
    else:
        print(f"Book '{row['title']}' already existed!")


def execute(df):
    all_authors = set()
    for authors_str in df['author'].dropna():
        if isinstance(authors_str, str):
            authors_list = authors_str.split(',')
            all_authors.update([author.strip() for author in authors_list if author.strip() != ''])

    # Save authors list
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
        print("Error: ", e)
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