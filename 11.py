import psycopg2
import csv

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Post9992k"
)

cur = conn.cursor()

# Drop and recreate phonebook
cur.execute("DROP TABLE IF EXISTS phonebook CASCADE;")
cur.execute("""
CREATE TABLE phonebook (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL UNIQUE,
    phone VARCHAR(20) NOT NULL UNIQUE
)
""")

# Other tables
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS user_score (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    score INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1
)
""")
cur.execute("""
CREATE TABLE IF NOT EXISTS leaderboard (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    score INTEGER,
    level INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
)
""")

# Drop old objects
cur.execute("DROP FUNCTION IF EXISTS search_phonebook(TEXT);")
cur.execute("DROP FUNCTION IF EXISTS get_phonebook_paged(INTEGER, INTEGER);")
cur.execute("DROP FUNCTION IF EXISTS get_all_phonebook_ordered();")
cur.execute("DROP PROCEDURE IF EXISTS insert_many_users(TEXT[], TEXT[]);")
cur.execute("DROP PROCEDURE IF EXISTS upsert_phonebook_user(VARCHAR, VARCHAR);")
cur.execute("DROP PROCEDURE IF EXISTS delete_user(TEXT);")

# Functions
cur.execute("""
CREATE OR REPLACE FUNCTION search_phonebook(pattern TEXT)
RETURNS TABLE(row_number BIGINT, first_name VARCHAR, phone VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ROW_NUMBER() OVER (ORDER BY phonebook.id),
        phonebook.first_name,
        phonebook.phone
    FROM phonebook
    WHERE phonebook.first_name ILIKE '%' || pattern || '%'
       OR phonebook.phone ILIKE '%' || pattern || '%';
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE FUNCTION get_phonebook_paged(p_limit INT, p_offset INT)
RETURNS TABLE(row_number BIGINT, first_name VARCHAR, phone VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        sub.row_number,
        sub.first_name,
        sub.phone
    FROM (
        SELECT
            ROW_NUMBER() OVER (ORDER BY phonebook.id) AS row_number,
            phonebook.first_name,
            phonebook.phone
        FROM phonebook
    ) AS sub
    ORDER BY sub.row_number
    LIMIT p_limit
    OFFSET p_offset;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE FUNCTION get_all_phonebook_ordered()
RETURNS TABLE(row_number BIGINT, first_name VARCHAR, phone VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        ROW_NUMBER() OVER (ORDER BY phonebook.id),
        phonebook.first_name,
        phonebook.phone
    FROM phonebook;
END;
$$;
""")

# Procedures
cur.execute("""
CREATE OR REPLACE PROCEDURE upsert_phonebook_user(p_name VARCHAR, p_phone VARCHAR)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO phonebook (first_name, phone)
    VALUES (p_name, p_phone)
    ON CONFLICT (first_name)
    DO UPDATE SET phone = EXCLUDED.phone;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE insert_many_users(
    IN names TEXT[],
    IN phones TEXT[]
)
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    phone_pattern CONSTANT TEXT := '^[0-9\\-\\+]+$';
BEGIN
    FOR i IN array_lower(names, 1)..array_upper(names, 1) LOOP
        IF phones[i] ~ phone_pattern THEN
            BEGIN
                INSERT INTO phonebook(first_name, phone)
                VALUES(names[i], phones[i])
                ON CONFLICT (first_name)
                DO UPDATE SET phone = EXCLUDED.phone;
            EXCEPTION WHEN others THEN
                RAISE NOTICE 'Invalid entry: % - %', names[i], phones[i];
            END;
        ELSE
            RAISE NOTICE 'Invalid phone format: % - %', names[i], phones[i];
        END IF;
    END LOOP;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE delete_user(p_identifier TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM phonebook
    WHERE first_name = p_identifier
       OR phone = p_identifier;
END;
$$;
""")

conn.commit()

# Interaction functions
def insert_from_csv():
    path = input("Enter CSV file path: ")
    with open(path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) != 2:
                print("Invalid row:", row)
                continue
            first_name, phone = row
            try:
                cur.execute(
                    "INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (first_name) DO UPDATE SET phone=EXCLUDED.phone",
                    (first_name, phone)
                )
            except Exception as e:
                conn.rollback()
                print("Error inserting:", e)
    conn.commit()
    print("CSV data inserted.")

def insert_from_console():
    first_name = input("Name: ").strip()
    phone = input("Phone: ").strip()
    try:
        cur.execute("CALL upsert_phonebook_user(%s, %s)", (first_name, phone))
        conn.commit()
        print("Inserted or updated successfully.")
    except Exception as e:
        conn.rollback()
        print("Error inserting:", e)

def update_user():
    old_phone_or_name = input("Enter username or phone to update: ")
    new_name = input("Enter new name (or press Enter to skip): ").strip()
    new_phone = input("Enter new phone (or press Enter to skip): ").strip()
    set_clauses = []
    values = []
    if new_name:
        set_clauses.append("first_name=%s")
        values.append(new_name)
    if new_phone:
        set_clauses.append("phone=%s")
        values.append(new_phone)
    if not set_clauses:
        print("Nothing to update.")
        return
    values.extend([old_phone_or_name, old_phone_or_name])
    sql = f"""
    UPDATE phonebook
    SET {', '.join(set_clauses)}
    WHERE phone=%s OR first_name=%s
    """
    try:
        cur.execute(sql, values)
        conn.commit()
        if cur.rowcount > 0:
            print("Data updated.")
        else:
            print("No records matched.")
    except Exception as e:
        conn.rollback()
        print("Error updating:", e)

def query_users():
    try:
        cur.execute("SELECT * FROM get_all_phonebook_ordered()")
        rows = cur.fetchall()
        if not rows:
            print("Phonebook is empty.")
        else:
            for row in rows:
                print(f"{row[0]}. {row[1]} - {row[2]}")
    except Exception as e:
        conn.rollback()
        print("Error querying:", e)

def delete_user_python():
    identifier = input("Enter name or phone to delete: ").strip()
    try:
        cur.execute("DELETE FROM phonebook WHERE first_name=%s OR phone=%s", (identifier, identifier))
        conn.commit()
        print("User(s) deleted (Python).")
    except Exception as e:
        conn.rollback()
        print("Error deleting:", e)

def search_users_by_pattern():
    pattern = input("Enter pattern to search: ").strip()
    try:
        cur.execute("SELECT * FROM search_phonebook(%s)", (pattern,))
        rows = cur.fetchall()
        if not rows:
            print("No matches found.")
        else:
            for row in rows:
                print(f"{row[0]}. {row[1]} - {row[2]}")
    except Exception as e:
        conn.rollback()
        print("Error searching:", e)

def upsert_user():
    name = input("Enter name: ").strip()
    phone = input("Enter phone: ").strip()
    try:
        cur.execute("CALL upsert_phonebook_user(%s, %s)", (name, phone))
        conn.commit()
        print("User inserted or updated.")
    except Exception as e:
        conn.rollback()
        print("Error upserting:", e)

def insert_many_users():
    names = []
    phones = []
    print("Enter users (empty name to finish):")
    while True:
        name = input("Name: ").strip()
        if not name:
            break
        phone = input("Phone: ").strip()
        names.append(name)
        phones.append(phone)
    if not names:
        print("No users to insert.")
        return
    try:
        cur.execute("CALL insert_many_users(%s::text[], %s::text[])", (names, phones))
        conn.commit()
        print("Users inserted successfully.")
    except Exception as e:
        conn.rollback()
        print("Error inserting many users:", e)

def query_with_pagination():
    limit = int(input("Enter limit: "))
    offset = int(input("Enter offset: "))
    try:
        cur.execute("SELECT * FROM get_phonebook_paged(%s, %s)", (limit, offset))
        rows = cur.fetchall()
        if not rows:
            print("No data to show.")
        else:
            for row in rows:
                print(f"{row[0]}. {row[1]} - {row[2]}")
    except Exception as e:
        conn.rollback()
        print("Error querying pagination:", e)

def delete_user_procedure():
    identifier = input("Enter name or phone to delete: ").strip()
    try:
        cur.execute("CALL delete_user(%s)", (identifier,))
        conn.commit()
        print("User(s) deleted (Procedure).")
    except Exception as e:
        conn.rollback()
        print("Error deleting:", e)

# Main Menu
if __name__ == "__main__":
    while True:
        print("\nMain Menu:")
        print("1. Insert from CSV")
        print("2. Insert from Console")
        print("3. Update User")
        print("4. Query Users")
        print("6. Search Users by Pattern")
        print("7. Upsert User")
        print("8. Insert Many Users")
        print("9. Query with Pagination")
        print("10. Delete User")
        print("11. Exit")
        choice = input("Choose option: ")

        if choice == "1":
            insert_from_csv()
        elif choice == "2":
            insert_from_console()
        elif choice == "3":
            update_user()
        elif choice == "4":
            query_users()
        elif choice == "5":
            delete_user_python()
        elif choice == "6":
            search_users_by_pattern()
        elif choice == "7":
            upsert_user()
        elif choice == "8":
            insert_many_users()
        elif choice == "9":
            query_with_pagination()
        elif choice == "10":
            delete_user_procedure()
        elif choice == "11":
            break
        else:
            print("Invalid choice. Try again.")

    cur.close()
    conn.close()
