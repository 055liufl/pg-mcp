# mini_blog reference

> 小型博客系统,单 schema 演示库,共 6 表 + 1 视图 + 1 枚举,约 95 行业务数据。

## 1. 路由关键词

当用户的自然语言查询出现以下任一词汇,**应路由到 mini_blog**:

- `blog` / `博客` / `文章` / `post` / `posts`
- `comment` / `评论` / `回复`
- `tag` / `标签`
- `author` / `作者` / `用户` (在博客语境下,如 alice / bob 等用户名)
- `published` / `draft` / `archived` / `发布` / `草稿` / `归档`
- `audit_log` / `审计` (在博客语境下,小规模)
- 出现具体用户名 `alice` / `bob` / `carol` / `dave` / `eve` / `frank`
- 出现 slug 类标识如 `pg-mvcc-deep-dive`、`langchain-for-production`

歧义判定:如果同时提到 "客户/订单/支付" 等电商词汇,路由到 `shop_oms`;如果提到 "事实表/维度/数据仓库" 等 DW 词汇,路由到 `analytics_dw`。

## 2. 连接

```bash
docker exec -e PGPASSWORD=test pg-mcp-fixtures \
  psql -h localhost -U test -d mini_blog -c '<SQL>'
```

主机直连(若本地有 psql):

```bash
PGPASSWORD=test psql -h localhost -p 5433 -U test -d mini_blog -c '<SQL>'
```

## 3. Schema 总览

唯一 schema:`public`(默认 search_path 即可,不必显式限定)。

| 类型 | 名称 | 行数 | 描述 |
|---|---|---|---|
| 表 | `users` | 6 | 博客用户(作者+读者) |
| 表 | `tags` | 8 | 文章标签 |
| 表 | `posts` | 18 | 博客文章 |
| 表 | `comments` | 16 | 文章评论(支持楼中楼) |
| 表 | `post_tags` | 35 | 文章 ↔ 标签 多对多 |
| 表 | `audit_log` | 12 | 管理操作审计 |
| 视图 | `published_posts` | 14 | 已发布文章 + 作者信息(无正文) |

## 4. 枚举类型

```sql
CREATE TYPE post_status AS ENUM ('draft', 'published', 'archived');
```

`posts.status` 仅取这三个值。`published_posts` 视图过滤 `status = 'published'`。

## 5. 表详细 schema

### `users`
| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | SERIAL PK | | |
| `username` | VARCHAR(50) | NOT NULL UNIQUE | 登录用户名 |
| `email` | VARCHAR(255) | NOT NULL UNIQUE,CHECK `email LIKE '%@%.%'` | |
| `full_name` | VARCHAR(120) | | 展示用全名 |
| `bio` | TEXT | | |
| `is_active` | BOOLEAN | NOT NULL DEFAULT TRUE | |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | |
| `last_login` | TIMESTAMPTZ | NULL = 从未登录 | |

### `tags`
| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | SERIAL PK | | |
| `name` | VARCHAR(50) | NOT NULL UNIQUE | 显示名,例如 `PostgreSQL` |
| `slug` | VARCHAR(50) | NOT NULL UNIQUE | URL 友好,例如 `postgresql` |
| `description` | TEXT | | |

### `posts`
| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | SERIAL PK | | |
| `author_id` | INTEGER | NOT NULL FK → `users(id)` ON DELETE CASCADE | |
| `title` | VARCHAR(255) | NOT NULL | |
| `slug` | VARCHAR(255) | NOT NULL UNIQUE | |
| `content` | TEXT | NOT NULL | |
| `status` | `post_status` | NOT NULL DEFAULT `'draft'` | enum |
| `view_count` | INTEGER | NOT NULL DEFAULT 0,CHECK ≥ 0 | |
| `word_count` | INTEGER | **GENERATED** AS `length(content) / 5` STORED | 估算字数,自动派生 |
| `published_at` | TIMESTAMPTZ | CHECK:`status='published'` 时必填 | |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | |
| `updated_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | |

业务约束 `posts_published_state_chk`:`status = 'published' AND published_at IS NOT NULL` 或 `status <> 'published'`。

### `comments`
| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | BIGSERIAL PK | | |
| `post_id` | INTEGER | NOT NULL FK → `posts(id)` ON DELETE CASCADE | |
| `author_id` | INTEGER | FK → `users(id)` ON DELETE SET NULL | NULL = 匿名/已注销 |
| `parent_id` | BIGINT | FK → `comments(id)` ON DELETE CASCADE | NULL = 顶层评论 |
| `body` | TEXT | NOT NULL | |
| `is_approved` | BOOLEAN | NOT NULL DEFAULT FALSE | 未审核不展示 |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | |

### `post_tags`(关联表)
| 列 | 类型 | 约束 |
|---|---|---|
| `post_id` | INTEGER | NOT NULL FK → `posts(id)` ON DELETE CASCADE |
| `tag_id` | INTEGER | NOT NULL FK → `tags(id)` ON DELETE CASCADE |
| PRIMARY KEY | | `(post_id, tag_id)` |

### `audit_log`
| 列 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | BIGSERIAL PK | | |
| `actor_id` | INTEGER | FK → `users(id)`(无 ON DELETE) | NULL = 系统操作 |
| `entity` | VARCHAR(40) | NOT NULL | 例如 `post` / `comment` / `user` |
| `entity_id` | INTEGER | NOT NULL | |
| `action` | VARCHAR(20) | NOT NULL | 例如 `publish` / `archive` / `reject` / `deactivate` |
| `metadata` | JSONB | | 含 `ip` / `user_agent` / `reason` 等键 |
| `created_at` | TIMESTAMPTZ | NOT NULL DEFAULT NOW() | |

## 6. 索引

| 索引 | 表 | 类型 | 备注 |
|---|---|---|---|
| `posts_author_idx` | `posts(author_id)` | btree | |
| `posts_status_idx` | `posts(status)` | btree | |
| `posts_published_at_idx` | `posts(published_at DESC)` | btree(部分) | `WHERE status='published'` |
| `comments_post_idx` | `comments(post_id)` | btree | |
| `comments_pending_idx` | `comments(post_id)` | btree(部分) | `WHERE is_approved=FALSE` |
| `audit_metadata_gin_idx` | `audit_log(metadata)` | GIN | JSONB 检索 |
| `users_active_email_idx` | `users(email)` | btree(部分) | `WHERE is_active=TRUE` |

## 7. 视图

### `published_posts`
```sql
SELECT p.id, p.title, p.slug, p.published_at, p.view_count,
       u.username AS author_username, u.full_name AS author_full_name
FROM posts p JOIN users u ON u.id = p.author_id
WHERE p.status = 'published';
```
**不含 `content` 字段**。需要正文请查 `posts`。

## 8. 字段语义与业务约定

- 时间字段全部 `TIMESTAMPTZ`,统计 "最近 N 天" 用 `created_at >= NOW() - INTERVAL 'N days'` 或 `published_at`(后者仅对 published 文章)。
- `posts.view_count` 仅是计数器,没有逐次浏览明细;不要构造 `JOIN view_events` 之类的表。
- `comments.is_approved = FALSE` 的评论代表未审核/已被拒;统计 "公开评论数" 时记得过滤 `is_approved = TRUE`。
- `posts.word_count` 是生成列,可直接用于排序/聚合,不必在 SQL 里再计算 `length(content)`。
- 当用户问 "总浏览量"、"最热文章" 时,优先用 `view_count`(已发布文章用 `published_posts` 视图更安全)。
- `audit_log.metadata` 的常见键:`ip`(text)、`user_agent`(text)、`reason`(text)、`diff`(object)。检索 IP 用 `metadata->>'ip' = '...'`;判断键存在用 `metadata ? 'reason'`。

## 9. 典型查询模板

### Q1:Alice 发表了多少条评论?
```sql
SELECT COUNT(*) AS comment_count
FROM   comments c
JOIN   users u ON u.id = c.author_id
WHERE  u.username = 'alice'
  AND  c.is_approved = TRUE;
```

### Q2:过去 30 天浏览量最高的 5 篇已发布文章
```sql
SELECT p.title, p.slug, p.view_count, p.published_at, u.username AS author
FROM   posts p
JOIN   users u ON u.id = p.author_id
WHERE  p.status = 'published'
  AND  p.published_at >= NOW() - INTERVAL '30 days'
ORDER  BY p.view_count DESC
LIMIT  5;
```

### Q3:每个标签下的已发布文章数
```sql
SELECT t.name AS tag, COUNT(*) AS post_count
FROM   tags t
JOIN   post_tags pt ON pt.tag_id = t.id
JOIN   posts p ON p.id = pt.post_id AND p.status = 'published'
GROUP  BY t.name
ORDER  BY post_count DESC;
```

### Q4:文章正文里搜索 "MVCC" 的已发布文章
```sql
SELECT id, title, slug
FROM   posts
WHERE  status = 'published'
  AND  content ILIKE '%MVCC%';
```

### Q5:从 audit_log 中找出 IP 为 192.0.2.55 的操作
```sql
SELECT id, actor_id, entity, entity_id, action, created_at
FROM   audit_log
WHERE  metadata->>'ip' = '192.0.2.55'
ORDER  BY created_at DESC;
```

### Q6:每位作者的 published / draft / archived 文章计数
```sql
SELECT u.username,
       COUNT(*) FILTER (WHERE p.status = 'published') AS published,
       COUNT(*) FILTER (WHERE p.status = 'draft')     AS draft,
       COUNT(*) FILTER (WHERE p.status = 'archived')  AS archived
FROM   users u
LEFT   JOIN posts p ON p.author_id = u.id
GROUP  BY u.username
ORDER  BY published DESC;
```
