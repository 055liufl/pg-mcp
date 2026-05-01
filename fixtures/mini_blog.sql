-- =============================================================================
-- Database: mini_blog (SMALL)
-- Purpose:  A minimal blog application — small-scale schema
-- Scale:    ~6 tables, 1 view, 1 enum, ~5 indexes, ~150 rows
-- Use case: Tests pg-mcp full-context schema injection (≤ 50 tables)
-- =============================================================================

-- Drop existing objects in dependency order
DROP VIEW IF EXISTS published_posts CASCADE;
DROP TABLE IF EXISTS audit_log CASCADE;
DROP TABLE IF EXISTS post_tags CASCADE;
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TYPE IF EXISTS post_status CASCADE;

-- =============================================================================
-- TYPES
-- =============================================================================

CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');
COMMENT ON TYPE post_status IS '博客文章发布状态';

-- =============================================================================
-- TABLES
-- =============================================================================

CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    username    VARCHAR(50)  NOT NULL UNIQUE,
    email       VARCHAR(255) NOT NULL UNIQUE,
    full_name   VARCHAR(120),
    bio         TEXT,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_login  TIMESTAMPTZ,
    CONSTRAINT users_email_format_chk CHECK (email LIKE '%@%.%')
);

COMMENT ON TABLE  users               IS '博客用户：作者与读者';
COMMENT ON COLUMN users.username      IS '登录用户名，全局唯一';
COMMENT ON COLUMN users.email         IS '邮箱地址，全局唯一';
COMMENT ON COLUMN users.full_name     IS '展示用全名';
COMMENT ON COLUMN users.bio           IS '用户简介';
COMMENT ON COLUMN users.is_active     IS '账号是否激活';
COMMENT ON COLUMN users.last_login    IS '最近登录时间，从未登录为 NULL';


CREATE TABLE tags (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(50) NOT NULL UNIQUE,
    slug        VARCHAR(50) NOT NULL UNIQUE,
    description TEXT
);

COMMENT ON TABLE  tags             IS '文章标签';
COMMENT ON COLUMN tags.name        IS '标签显示名';
COMMENT ON COLUMN tags.slug        IS 'URL 友好标识';


CREATE TABLE posts (
    id           SERIAL PRIMARY KEY,
    author_id    INTEGER     NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title        VARCHAR(255) NOT NULL,
    slug         VARCHAR(255) NOT NULL UNIQUE,
    content      TEXT         NOT NULL,
    status       post_status  NOT NULL DEFAULT 'draft',
    view_count   INTEGER      NOT NULL DEFAULT 0,
    word_count   INTEGER GENERATED ALWAYS AS (length(content) / 5) STORED,
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT posts_published_state_chk
        CHECK ((status = 'published' AND published_at IS NOT NULL)
            OR (status <> 'published')),
    CONSTRAINT posts_view_count_nonneg_chk CHECK (view_count >= 0)
);

COMMENT ON TABLE  posts              IS '博客文章';
COMMENT ON COLUMN posts.author_id    IS '作者，FK 至 users.id';
COMMENT ON COLUMN posts.slug         IS 'URL 友好标识，全局唯一';
COMMENT ON COLUMN posts.status       IS '发布状态，draft/published/archived';
COMMENT ON COLUMN posts.view_count   IS '浏览次数，非负整数';
COMMENT ON COLUMN posts.word_count   IS '估算字数，由 content 自动派生';
COMMENT ON COLUMN posts.published_at IS '发布时间；status=published 时必填';


CREATE TABLE comments (
    id          BIGSERIAL PRIMARY KEY,
    post_id     INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author_id   INTEGER          REFERENCES users(id) ON DELETE SET NULL,
    parent_id   BIGINT           REFERENCES comments(id) ON DELETE CASCADE,
    body        TEXT    NOT NULL,
    is_approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  comments             IS '文章评论，支持楼中楼回复';
COMMENT ON COLUMN comments.parent_id   IS '父评论，回复时填写，否则为 NULL';
COMMENT ON COLUMN comments.is_approved IS '是否通过审核，未通过的评论不展示';


CREATE TABLE post_tags (
    post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES tags(id)  ON DELETE CASCADE,
    PRIMARY KEY (post_id, tag_id)
);

COMMENT ON TABLE post_tags IS '文章 ↔ 标签 多对多关联';


CREATE TABLE audit_log (
    id          BIGSERIAL PRIMARY KEY,
    actor_id    INTEGER REFERENCES users(id),
    entity      VARCHAR(40) NOT NULL,
    entity_id   INTEGER     NOT NULL,
    action      VARCHAR(20) NOT NULL,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  audit_log         IS '管理操作审计日志';
COMMENT ON COLUMN audit_log.entity  IS '被操作实体类型，例如 post / comment';
COMMENT ON COLUMN audit_log.action  IS '操作类型，例如 create / publish / archive';
COMMENT ON COLUMN audit_log.metadata IS '操作上下文，例如 IP/UA/diff';

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX posts_author_idx          ON posts (author_id);
CREATE INDEX posts_status_idx          ON posts (status);
CREATE INDEX posts_published_at_idx    ON posts (published_at DESC) WHERE status = 'published';
CREATE INDEX comments_post_idx         ON comments (post_id);
CREATE INDEX comments_pending_idx      ON comments (post_id) WHERE is_approved = FALSE;
CREATE INDEX audit_metadata_gin_idx    ON audit_log USING GIN (metadata);
CREATE INDEX users_active_email_idx    ON users (email) WHERE is_active = TRUE;

-- =============================================================================
-- VIEWS
-- =============================================================================

CREATE VIEW published_posts AS
SELECT  p.id,
        p.title,
        p.slug,
        p.published_at,
        p.view_count,
        u.username     AS author_username,
        u.full_name    AS author_full_name
FROM    posts p
JOIN    users u ON u.id = p.author_id
WHERE   p.status = 'published';

COMMENT ON VIEW published_posts IS '所有已发布文章及作者信息（不含正文）';

-- =============================================================================
-- DATA: USERS
-- =============================================================================

INSERT INTO users (username, email, full_name, bio, is_active, last_login) VALUES
  ('alice',    'alice@example.com',    'Alice Wong',     '资深技术作者，专注 PostgreSQL 和分布式系统', TRUE,  NOW() - INTERVAL '2 hours'),
  ('bob',      'bob@example.com',      'Bob Schmidt',    'Rust 爱好者，业余写博客', TRUE,  NOW() - INTERVAL '1 day'),
  ('carol',    'carol@example.com',    'Carol Lin',      '后端工程师，AI/ML 方向', TRUE,  NOW() - INTERVAL '3 days'),
  ('dave',     'dave@example.com',     'Dave Oliveira',  'DevOps engineer 与开源贡献者', FALSE, NULL),
  ('eve',      'eve@example.com',      'Eve Marchetti',  '产品经理，正在学习 SQL', TRUE,  NOW() - INTERVAL '7 days'),
  ('frank',    'frank@example.com',    'Frank Tanaka',   '安全研究员，专注数据库渗透', TRUE,  NOW() - INTERVAL '14 days');

-- =============================================================================
-- DATA: TAGS
-- =============================================================================

INSERT INTO tags (name, slug, description) VALUES
  ('PostgreSQL', 'postgresql', '与 PostgreSQL 相关的内容'),
  ('Python',     'python',     'Python 编程语言'),
  ('Rust',       'rust',       'Rust 编程语言'),
  ('AI',         'ai',         '人工智能与机器学习'),
  ('DevOps',     'devops',     '部署、运维、可观测性'),
  ('Security',   'security',   '安全与合规'),
  ('Tutorial',   'tutorial',   '教程类文章'),
  ('Architecture','architecture','系统架构与设计');

-- =============================================================================
-- DATA: POSTS (24 posts, mix of statuses)
-- =============================================================================

INSERT INTO posts (author_id, title, slug, content, status, view_count, published_at, created_at, updated_at) VALUES
  (1, '深入理解 PostgreSQL MVCC',     'pg-mvcc-deep-dive',
      'MVCC 是 PostgreSQL 并发控制的核心机制。' || repeat('Lorem ipsum dolor sit amet. ', 60),
      'published', 1842, NOW() - INTERVAL '60 days', NOW() - INTERVAL '62 days', NOW() - INTERVAL '60 days'),
  (1, '使用 logical replication 实现跨实例同步', 'logical-replication-howto',
      '本文演示如何配置 logical replication。' || repeat('Section content. ', 80),
      'published',  912, NOW() - INTERVAL '45 days', NOW() - INTERVAL '47 days', NOW() - INTERVAL '45 days'),
  (1, 'pg_stat_statements 实战', 'pg-stat-statements-tips',
      'pg_stat_statements 是性能调优的利器。' || repeat('Tip: ', 100),
      'published',  650, NOW() - INTERVAL '30 days', NOW() - INTERVAL '32 days', NOW() - INTERVAL '30 days'),
  (1, '关于 VACUUM 的一些常见误区',  'vacuum-myths',
      '草稿：列出三个最常见的 VACUUM 误区。' || repeat('Word ', 30),
      'draft', 0, NULL, NOW() - INTERVAL '5 days', NOW() - INTERVAL '5 days'),

  (2, 'Rust 异步运行时对比', 'rust-async-runtimes',
      'tokio、async-std、smol 各有什么区别？' || repeat('Comparison. ', 70),
      'published',  430, NOW() - INTERVAL '20 days', NOW() - INTERVAL '22 days', NOW() - INTERVAL '20 days'),
  (2, '在 Rust 里写 Postgres 客户端', 'rust-postgres-client',
      '从零实现一个 Rust 的 Postgres wire-protocol 客户端。' || repeat('Code. ', 100),
      'published',  220, NOW() - INTERVAL '10 days', NOW() - INTERVAL '12 days', NOW() - INTERVAL '10 days'),
  (2, '我在生产用了一年 sqlx 之后', 'sqlx-after-one-year',
      '体验、坑、最佳实践。' || repeat('Lessons learned. ', 50),
      'archived', 1100, NOW() - INTERVAL '180 days', NOW() - INTERVAL '182 days', NOW() - INTERVAL '180 days'),

  (3, 'LangChain 为什么不适合生产？', 'langchain-for-production',
      'LangChain 的几个常见问题与替代方案。' || repeat('Reason. ', 70),
      'published', 2210, NOW() - INTERVAL '15 days', NOW() - INTERVAL '17 days', NOW() - INTERVAL '15 days'),
  (3, '用 pgvector 构建语义搜索',     'pgvector-semantic-search',
      'pgvector 让你不必引入 dedicated vector database。' || repeat('Demo. ', 80),
      'published', 1430, NOW() - INTERVAL '8 days', NOW() - INTERVAL '10 days', NOW() - INTERVAL '8 days'),
  (3, 'RAG pipeline 的常见反模式',     'rag-anti-patterns',
      'Retrieval-Augmented Generation 在工程化时的坑。' || repeat('Pattern. ', 60),
      'published',  680, NOW() - INTERVAL '3 days', NOW() - INTERVAL '5 days', NOW() - INTERVAL '3 days'),
  (3, '关于 LLM 评测的笔记',           'llm-eval-notes',
      '草稿：列出 5 种常见评测方式。' || repeat('Note. ', 40),
      'draft', 0, NULL, NOW() - INTERVAL '2 days', NOW() - INTERVAL '2 days'),

  (5, 'PM 学 SQL 第一课：SELECT',     'pm-sql-1-select',
      '面向产品经理的 SQL 入门。' || repeat('Step. ', 50),
      'published', 320, NOW() - INTERVAL '50 days', NOW() - INTERVAL '52 days', NOW() - INTERVAL '50 days'),
  (5, 'PM 学 SQL 第二课：JOIN',       'pm-sql-2-join',
      'JOIN 是 PM 最常用的关键字。' || repeat('Example. ', 60),
      'published', 280, NOW() - INTERVAL '40 days', NOW() - INTERVAL '42 days', NOW() - INTERVAL '40 days'),
  (5, 'PM 学 SQL 第三课：聚合', 'pm-sql-3-aggregate',
      'GROUP BY、HAVING、聚合函数。' || repeat('Example. ', 70),
      'published', 250, NOW() - INTERVAL '25 days', NOW() - INTERVAL '27 days', NOW() - INTERVAL '25 days'),

  (6, '审计日志设计模式',            'audit-log-patterns',
      '从 trigger 到 logical replication 的取舍。' || repeat('Pattern. ', 100),
      'published',  540, NOW() - INTERVAL '90 days', NOW() - INTERVAL '92 days', NOW() - INTERVAL '90 days'),
  (6, 'PostgreSQL 注入面试题精选',   'pg-injection-interview',
      '常见 PG 注入题与防御要点。' || repeat('Q&A. ', 80),
      'published',  720, NOW() - INTERVAL '70 days', NOW() - INTERVAL '72 days', NOW() - INTERVAL '70 days'),
  (6, 'pglast vs SQLGlot：谁更适合做 SQL 安全校验？', 'pglast-vs-sqlglot',
      '从准确性、性能、可维护性三个角度对比。' || repeat('Compare. ', 70),
      'published',  840, NOW() - INTERVAL '5 days', NOW() - INTERVAL '7 days', NOW() - INTERVAL '5 days'),
  (6, '草稿：MCP 协议最佳实践', 'mcp-best-practices-draft',
      '简短大纲。' || repeat('TODO. ', 30),
      'draft', 0, NULL, NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 day');

-- =============================================================================
-- DATA: POST_TAGS
-- =============================================================================

INSERT INTO post_tags (post_id, tag_id) VALUES
  -- Alice (PostgreSQL focused)
  (1, 1), (1, 7),                    -- pg-mvcc-deep-dive
  (2, 1), (2, 7),                    -- logical-replication
  (3, 1),                            -- pg_stat_statements
  (4, 1),                            -- vacuum draft
  -- Bob (Rust focused)
  (5, 3), (5, 8),                    -- async runtimes
  (6, 3), (6, 1),                    -- rust postgres
  (7, 3), (7, 1),                    -- sqlx
  -- Carol (AI focused)
  (8, 4), (8, 8),                    -- LangChain
  (9, 4), (9, 1), (9, 7),            -- pgvector
  (10, 4), (10, 8),                  -- RAG anti-patterns
  (11, 4),                           -- LLM eval notes
  -- Eve (PM tutorials)
  (12, 1), (12, 7), (12, 2),         -- SQL basics
  (13, 1), (13, 7),                  -- JOINs
  (14, 1), (14, 7),                  -- aggregates
  -- Frank (security)
  (15, 1), (15, 6), (15, 8),         -- audit logs
  (16, 1), (16, 6),                  -- pg injection
  (17, 1), (17, 6),                  -- pglast vs sqlglot
  (18, 6);                           -- MCP

-- =============================================================================
-- DATA: COMMENTS
-- =============================================================================

INSERT INTO comments (post_id, author_id, parent_id, body, is_approved, created_at) VALUES
  (1, 2, NULL, '这篇讲得太清楚了，比官方文档还容易懂。', TRUE,  NOW() - INTERVAL '59 days'),
  (1, 3, NULL, 'MVCC 一直是我的噩梦，感谢分享！', TRUE,  NOW() - INTERVAL '58 days'),
  (1, NULL, 2, '我也有同感，等我有空再读一遍。', TRUE,  NOW() - INTERVAL '57 days'),
  (1, 5, NULL, '请问 vacuum freeze 和 vacuum full 的区别？', FALSE, NOW() - INTERVAL '40 days'),

  (2, 3, NULL, '请问 publication 删除后 slot 不释放怎么处理？', TRUE,  NOW() - INTERVAL '44 days'),
  (2, 1, 5, '需要手动 pg_drop_replication_slot()。', TRUE, NOW() - INTERVAL '43 days'),

  (3, 6, NULL, '内置的 pg_stat_statements 是否需要单独装？', TRUE,  NOW() - INTERVAL '29 days'),
  (3, 1, 7, '是的，contrib 包里，需要 CREATE EXTENSION。', TRUE,  NOW() - INTERVAL '29 days'),

  (5, 1, NULL, '现在 tokio 仍然是默认选项吗？', TRUE,  NOW() - INTERVAL '19 days'),

  (8, 5, NULL, '太真实了！我们项目正打算抛弃 LangChain。', TRUE,  NOW() - INTERVAL '14 days'),
  (8, 6, NULL, '但是抛弃后用什么替代？', FALSE, NOW() - INTERVAL '13 days'),

  (9, 1, NULL, 'pgvector 的 HNSW 性能怎么样？', TRUE,  NOW() - INTERVAL '7 days'),
  (9, 3, 12, '相比 IVFFlat 大约快 3-5 倍，但内存占用更高。', TRUE, NOW() - INTERVAL '7 days'),

  (12, 6, NULL, '收藏了，正好给团队分享。', TRUE,  NOW() - INTERVAL '49 days'),
  (16, 3, NULL, '能再补充一下 SQLi 防御 checklist 吗？', TRUE,  NOW() - INTERVAL '69 days'),
  (17, 1, NULL, '我们生产用 SQLGlot，准确率确实有损失。', TRUE,  NOW() - INTERVAL '4 days');

-- =============================================================================
-- DATA: AUDIT_LOG
-- =============================================================================

INSERT INTO audit_log (actor_id, entity, entity_id, action, metadata, created_at) VALUES
  (1, 'post', 1,  'publish', '{"ip":"203.0.113.7","user_agent":"Chrome/120"}'::jsonb,  NOW() - INTERVAL '60 days'),
  (1, 'post', 2,  'publish', '{"ip":"203.0.113.7","user_agent":"Chrome/120"}'::jsonb,  NOW() - INTERVAL '45 days'),
  (1, 'post', 3,  'publish', '{"ip":"203.0.113.7"}'::jsonb,                              NOW() - INTERVAL '30 days'),
  (2, 'post', 5,  'publish', '{"ip":"198.51.100.4","user_agent":"Firefox/121"}'::jsonb, NOW() - INTERVAL '20 days'),
  (2, 'post', 6,  'publish', '{"ip":"198.51.100.4"}'::jsonb,                              NOW() - INTERVAL '10 days'),
  (2, 'post', 7,  'archive', '{"reason":"outdated content"}'::jsonb,                      NOW() - INTERVAL '5 days'),
  (3, 'post', 8,  'publish', '{"ip":"192.0.2.55"}'::jsonb,                                NOW() - INTERVAL '15 days'),
  (3, 'post', 9,  'publish', '{"ip":"192.0.2.55"}'::jsonb,                                NOW() - INTERVAL '8 days'),
  (3, 'post', 10, 'publish', '{"ip":"192.0.2.55"}'::jsonb,                                NOW() - INTERVAL '3 days'),
  (6, 'comment', 11, 'reject', '{"reason":"spam"}'::jsonb,                                NOW() - INTERVAL '13 days'),
  (6, 'comment', 4,  'reject', '{"reason":"off-topic"}'::jsonb,                           NOW() - INTERVAL '40 days'),
  (NULL, 'user',  4, 'deactivate', '{"reason":"inactivity"}'::jsonb,                      NOW() - INTERVAL '90 days');

-- =============================================================================
-- POST-LOAD: stats
-- =============================================================================
ANALYZE;
