CREATE TABLE principals(
    id SERIAL PRIMARY KEY,
    created_on TIMESTAMP NOT NULL,
    role_id VARCHAR(24) NOT NULL
);

CREATE TABLE articles(
    id SERIAL PRIMARY KEY,
    title VARCHAR(256) NOT NULL,
    source VARCHAR(64),
    link TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    preview_text TEXT NOT NULL,
    preview_pic TEXT NOT NULL
);

CREATE TABLE habr_posts(
    id SERIAL PRIMARY KEY,
    article_id BIGINT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    tags VARCHAR(256)[] NOT NULL,
    hubs VARCHAR(256)[] NOT NULL,
    author_name VARCHAR(32) NOT NULL,
    author_link TEXT NOT NULL,
    author_karma INT NOT NULL,
    author_rating INT NOT NULL
);
