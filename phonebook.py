import psycopg2
import csv
import pygame
import random
import sys
import os
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Post9992k"
)

cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE IF NOT EXISTS phonebook (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    phone VARCHAR(20) NOT NULL UNIQUE
)
""")
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
conn.commit()

# Phonebook functions (unchanged)
def insert_from_csv(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) != 2:
                print("Invalid row:", row)
                continue
            first_name, phone = row
            try:
                cur.execute(
                    "INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (phone) DO NOTHING",
                    (first_name, phone)
                )
            except Exception as e:
                print("Error inserting:", e)
    conn.commit()
    print("CSV data inserted.")

def insert_from_console():
    first_name = input("Name: ").strip()
    phone = input("Phone: ").strip()
    try:
        cur.execute(
            "INSERT INTO phonebook (first_name, phone) VALUES (%s, %s) ON CONFLICT (phone) DO NOTHING",
            (first_name, phone)
        )
        conn.commit()
        print("Data inserted)")
    except Exception as e:
        print("Error inserting:", e)

def update_user(old_phone_or_name, new_name=None, new_phone=None):
    if new_name:
        cur.execute(
            "UPDATE phonebook SET first_name=%s WHERE phone=%s OR first_name=%s",
            (new_name, old_phone_or_name, old_phone_or_name)
        )
    if new_phone:
        cur.execute(
            "UPDATE phonebook SET phone=%s WHERE phone=%s OR first_name=%s",
            (new_phone, old_phone_or_name, old_phone_or_name)
        )
    conn.commit()
    print("Data updated.")

def query_users(filter_text=None):
    if filter_text:
        cur.execute(
            "SELECT * FROM phonebook WHERE first_name ILIKE %s OR phone ILIKE %s",
            (f"%{filter_text}%", f"%{filter_text}%")
        )
    else:
        cur.execute("SELECT * FROM phonebook")
    rows = cur.fetchall()
    for row in rows:
        print(row)

def delete_user(identifier):
    cur.execute(
        "DELETE FROM phonebook WHERE first_name=%s OR phone=%s",
        (identifier, identifier)
    )
    conn.commit()
    print("udaleno")

# Snake game DB functions
def get_or_create_user(username):
    cur.execute("SELECT id FROM users WHERE username=%s", (username,))
    row = cur.fetchone()
    if row:
        user_id = row[0]
        print(f"Welcome back, {username}!")
    else:
        cur.execute("INSERT INTO users (username) VALUES (%s) RETURNING id", (username,))
        user_id = cur.fetchone()[0]
        conn.commit()
        print(f"Created user: {username}")
    cur.execute("SELECT score, level FROM user_score WHERE user_id=%s ORDER BY id DESC LIMIT 1", (user_id,))
    score_row = cur.fetchone()
    if score_row:
        score, level = score_row
        print(f"Your last score: {score}, Level: {level}")
    else:
        score = 0
        level = 1
        print("U never played so starting from 0")
    return user_id, score, level

def save_progress(user_id, username, score, level):
    cur.execute(
        "INSERT INTO user_score (user_id, score, level) VALUES (%s, %s, %s)",
        (user_id, score, level)
    )
    cur.execute("SELECT id, score FROM leaderboard WHERE username=%s", (username,))
    row = cur.fetchone()
    if row:
        existing_id, existing_score = row
        if score > existing_score:
            cur.execute("""
                UPDATE leaderboard
                SET score=%s, level=%s, created_at=NOW()
                WHERE id=%s
            """, (score, level, existing_id))
            print("Leaderboard updated with new high score!")
        else:
            print("Score saved, but not higher than your best.")
    else:
        cur.execute("""
            INSERT INTO leaderboard (username, score, level)
            VALUES (%s, %s, %s)
        """, (username, score, level))
        print("New leaderboard entry created.")
    conn.commit()
    print("Progress saved.")

def show_leaderboard():
    cur.execute("""
    SELECT username, score, level, created_at
    FROM leaderboard
    ORDER BY score DESC
    LIMIT 10
    """)
    rows = cur.fetchall()
    print("\n--- Top 10 Best Runs ---")
    for i, row in enumerate(rows, 1):
        username, score, level, created_at = row
        print(f"{i}. {username}: {score} points, Level {level}, at {created_at}")

def close_connection():
    cur.close()
    conn.close()

# Snake Game Function (level reset + better visuals)
def snake_game(user_id, username, last_level):
    pygame.init()

    WIDTH, HEIGHT = 800, 600
    BLOCK_SIZE = 20
    GRID_WIDTH = WIDTH // BLOCK_SIZE
    GRID_HEIGHT = HEIGHT // BLOCK_SIZE

    BACKGROUND_COLOR = (30, 30, 30)
    TEXT_COLOR = (255, 255, 255)

    screen = pygame.display.set_mode((WIDTH, HEIGHT))
    pygame.display.set_caption("Snake Game")
    font = pygame.font.SysFont("Arial", 24)
    clock = pygame.time.Clock()

    def draw_text(text, x, y, color=TEXT_COLOR):
        label = font.render(text, True, color)
        screen.blit(label, (x, y))

    def generate_food(snake):
        while True:
            x = random.randint(0, GRID_WIDTH - 1)
            y = random.randint(0, GRID_HEIGHT - 1)
            if (x, y) not in snake:
                weight = random.randint(1, 3)
                timer = random.randint(30, 60)
                # Random bright color for food
                color = (
                    random.randint(128, 255),
                    random.randint(128, 255),
                    random.randint(128, 255)
                )
                return {"pos": (x, y), "weight": weight, "timer": timer, "color": color}

    snake = [(5, 5), (4, 5), (3, 5)]
    direction = (1, 0)
    food = generate_food(snake)
    score = 0
    level = 0
    speed = 10

    hue = 0  # for snake color cycling

    running = True
    while running:
        screen.fill(BACKGROUND_COLOR)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_UP and direction != (0, 1):
                    direction = (0, -1)
                elif event.key == pygame.K_DOWN and direction != (0, -1):
                    direction = (0, 1)
                elif event.key == pygame.K_LEFT and direction != (1, 0):
                    direction = (-1, 0)
                elif event.key == pygame.K_RIGHT and direction != (-1, 0):
                    direction = (1, 0)
                elif event.key == pygame.K_p:
                    save_progress(user_id, username, score, level)
                    print("Paused and saved progress.")

        head_x, head_y = snake[0]
        dx, dy = direction
        new_head = (head_x + dx, head_y + dy)

        if (new_head[0] < 0 or new_head[0] >= GRID_WIDTH or
            new_head[1] < 0 or new_head[1] >= GRID_HEIGHT or
            new_head in snake[:-1]):
            print("Game Over!")
            save_progress(user_id, username, score, level)
            running = False
            continue

        snake.insert(0, new_head)

        if new_head == food["pos"]:
            score += food["weight"]
            if score % 5 == 0:
                level += 1
                speed += 1
            food = generate_food(snake)
        else:
            snake.pop()
            food["timer"] -= 1
            if food["timer"] <= 0:
                food = generate_food(snake)

        # Update hue for snake rainbow effect
        hue = (hue + 2) % 360

        # Draw snake segments
        for i, segment in enumerate(snake):
            x, y = segment
            # Each segment gets a slightly different hue
            segment_hue = (hue + i * 10) % 360
            # Convert HSV to RGB
            color = pygame.Color(0)
            color.hsva = (segment_hue, 100, 100, 100)
            pygame.draw.rect(screen, color, (x * BLOCK_SIZE, y * BLOCK_SIZE, BLOCK_SIZE, BLOCK_SIZE), border_radius=4)

        # Draw colorful food
        fx, fy = food["pos"]
        center = (fx * BLOCK_SIZE + BLOCK_SIZE // 2, fy * BLOCK_SIZE + BLOCK_SIZE // 2)
        radius = BLOCK_SIZE // 2 - 2
        pygame.draw.circle(screen, food["color"], center, radius)

        draw_text(f"Score: {score}", 10, 10)
        draw_text(f"Level: {level}", 10, 40)
        draw_text(f"Speed: {speed}", 10, 70)
        draw_text(f"Press 'P' to Save", 10, HEIGHT - 30)

        pygame.display.flip()
        clock.tick(speed)

    pygame.quit()

# Main Menu
if __name__ == "__main__":
    while True:
        print("\nMain Menu:")
        print("1. Insert from CSV")
        print("2. Insert from Console")
        print("3. Update User")
        print("4. Query Users")
        print("5. Delete User")
        print("6. Snake Game")
        print("7. 10 Best Runs")
        print("8. Exit")
        choice = input("Choose option ")

        if choice == "1":
            path = input("Enter CSV file path: ")
            insert_from_csv(path)
        elif choice == "2":
            insert_from_console()
        elif choice == "3":
            idf = input("Enter username or phone to update: ")
            new_n = input("Enter new name (or press Enter to skip): ").strip()
            new_p = input("Enter new phone (or press Enter to skip): ").strip()
            update_user(idf, new_name=new_n if new_n else None, new_phone=new_p if new_p else None)
        elif choice == "4":
            f = input("Enter filter text (or press Enter for all): ")
            query_users(f if f else None)
        elif choice == "5":
            idf = input("Enter username or phone to delete: ")
            delete_user(idf)
        elif choice == "6":
            username = input("Enter your Snake username: ").strip()
            user_id, last_score, last_level = get_or_create_user(username)
            snake_game(user_id, username, last_level)
        elif choice == "7":
            show_leaderboard()
        elif choice == "8":
            break
        else:
            print("Invalid choice. Try again.")

    close_connection()
