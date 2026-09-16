import psycopg2


# =========================
# DATABASE CONNECTION
# =========================

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="youtube_db",
    user="postgres",
    password="postgres"
)

cursor = connection.cursor()


# =========================
# STAGING TABLE
# =========================

cursor.execute("""
CREATE TABLE IF NOT EXISTS staging (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100),
    video_id VARCHAR(100) UNIQUE NOT NULL,
    description VARCHAR(300),
    like_count INTEGER DEFAULT 0,
    published_at TIMESTAMP,
    view_count BIGINT,
    comment_count BIGINT,
    favorite_count BIGINT
);
""")


# =========================
# CORE TABLE
# =========================

cursor.execute("""
CREATE TABLE IF NOT EXISTS core (
    id SERIAL PRIMARY KEY,
    title VARCHAR(100),
    video_id VARCHAR(100) UNIQUE NOT NULL,
    description VARCHAR(300),
    like_count INTEGER DEFAULT 0,
    published_at TIMESTAMP,
    view_count BIGINT,
    comment_count BIGINT,
    favorite_count BIGINT
);
""")


# =========================
# SAVE CHANGES
# =========================

connection.commit()

print("✅ Staging and Core tables created successfully!")


# =========================
# CLOSE CONNECTION
# =========================

cursor.close()
connection.close()
