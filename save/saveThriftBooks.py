from services import saveService

source_id = 1 

def insert_book(row, conn, cur):
    print(f'book = { row.title } ### check_book_existed = {saveService.check_book_existed(row.id, cur)}' )

    if not saveService.check_book_existed(row['id'], cur):
        query = f'''INSERT INTO "book" ("id", "title", "description", "book_cover", "image_url", "release_date", "publisher", "number_of_pages", "price", "average_rating", "number_of_ratings", "number_of_reviews", "source_id", "preprocessed_description")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        cur.execute(query, (
            str(row['id']),
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
            source_id,
            row['preprocessed_description'],
        ))
        print(f"Book '{row['title']}' inserted successfully!")
        return row

def execute(df):
    all_authors = set()
    for authors_str in df['authors'].dropna():
        if isinstance(authors_str, str):
            authors_list = authors_str.split(',')
            all_authors.update([author.strip() for author in authors_list if author.strip() != ''])

    # Save authors
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

    # Save books
    conn, cur = saveService.connect_db(timeout=300000)
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

    # Save book author relationship
    conn, cur = saveService.connect_db(timeout=300000)
    try:
        saveService.insert_author_to_book_from_dataframe(df, conn, cur)

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Error: ", e)
    finally:
        cur.close()
        conn.close()