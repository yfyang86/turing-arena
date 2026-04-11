CREATE TABLE IF NOT EXISTS sessions (
    id          VARCHAR PRIMARY KEY,
    name        VARCHAR NOT NULL,
    created_at  TIMESTAMP DEFAULT current_timestamp,
    updated_at  TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS session_laureates (
    session_id    VARCHAR NOT NULL,
    laureate_slug VARCHAR NOT NULL,
    joined_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (session_id, laureate_slug)
);

CREATE TABLE IF NOT EXISTS messages (
    id             VARCHAR PRIMARY KEY,
    session_id     VARCHAR NOT NULL,
    role           VARCHAR NOT NULL,
    laureate_slug  VARCHAR,
    content        TEXT NOT NULL,
    metadata       JSON,
    created_at     TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS topics (
    id            VARCHAR PRIMARY KEY,
    name          VARCHAR NOT NULL UNIQUE,
    first_seen    TIMESTAMP DEFAULT current_timestamp,
    mention_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS topic_mentions (
    id          VARCHAR PRIMARY KEY,
    topic_id    VARCHAR NOT NULL,
    message_id  VARCHAR NOT NULL,
    session_id  VARCHAR NOT NULL,
    created_at  TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS user_collection (
    laureate_slug     VARCHAR PRIMARY KEY,
    unlocked_at       TIMESTAMP DEFAULT current_timestamp,
    interaction_count INTEGER DEFAULT 0
);

-- Wiki pages (persistent knowledge base)
CREATE TABLE IF NOT EXISTS wiki_pages (
    id           VARCHAR PRIMARY KEY,
    slug         VARCHAR UNIQUE NOT NULL,
    title        VARCHAR NOT NULL,
    page_type    VARCHAR NOT NULL,
    content      TEXT NOT NULL,
    frontmatter  JSON,
    content_hash VARCHAR NOT NULL,
    parent_hash  VARCHAR,
    created_at   TIMESTAMP DEFAULT current_timestamp,
    updated_at   TIMESTAMP DEFAULT current_timestamp,
    version      INTEGER DEFAULT 1
);

-- Wiki cross-references
CREATE TABLE IF NOT EXISTS wiki_links (
    from_slug   VARCHAR NOT NULL,
    to_slug     VARCHAR NOT NULL,
    link_type   VARCHAR NOT NULL,
    created_at  TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (from_slug, to_slug, link_type)
);

-- Wiki activity log
CREATE TABLE IF NOT EXISTS wiki_log (
    id          VARCHAR PRIMARY KEY,
    action      VARCHAR NOT NULL,
    detail      TEXT,
    session_id  VARCHAR,
    created_at  TIMESTAMP DEFAULT current_timestamp
);

-- Concept timeline: tracks how a concept evolves across sessions
CREATE TABLE IF NOT EXISTS wiki_concept_timeline (
    id              VARCHAR PRIMARY KEY,
    concept_slug    VARCHAR NOT NULL,
    session_id      VARCHAR NOT NULL,
    session_name    VARCHAR,
    snapshot        TEXT NOT NULL,
    content_hash    VARCHAR NOT NULL,
    phase           VARCHAR DEFAULT 'mentioned',
    created_at      TIMESTAMP DEFAULT current_timestamp
);

-- Session ingest tracking: hash of session content at last ingest
CREATE TABLE IF NOT EXISTS wiki_ingest_log (
    session_id      VARCHAR PRIMARY KEY,
    content_hash    VARCHAR NOT NULL,
    ingested_at     TIMESTAMP DEFAULT current_timestamp
);

-- Wiki page version history (saved before each overwrite)
CREATE TABLE IF NOT EXISTS wiki_page_history (
    id              VARCHAR PRIMARY KEY,
    slug            VARCHAR NOT NULL,
    version         INTEGER NOT NULL,
    title           VARCHAR NOT NULL,
    content         TEXT NOT NULL,
    frontmatter     JSON,
    content_hash    VARCHAR NOT NULL,
    saved_at        TIMESTAMP DEFAULT current_timestamp
);
