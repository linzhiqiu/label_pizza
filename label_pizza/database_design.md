# Video-Labeling Platform · Database Schema v 1.1
> **PostgreSQL + SQLAlchemy** — soft links (no hard FKs); integrity enforced in the service layer.

---

## 0 · Type & Convention Cheat-Sheet

| Doc type       | SQLAlchemy field              | Postgres column              | Why we chose it                                |
| -------------- | ----------------------------- | ---------------------------- | ---------------------------------------------- |
| `INT`          | `Integer`                     | `integer`                    | Compact PK/FK                                  |
| `VARCHAR(n)`   | `String(n)`                   | `character varying(n)`       | Bounded text (emails, IDs)                     |
| `TEXT`         | `Text`                        | `text`                       | Unbounded UTF-8                                |
| `TIMESTAMPTZ`  | `DateTime(timezone=True)`     | `timestamp with time zone`   | Always UTC, DST-safe                           |
| `JSONB`        | `postgresql.JSONB`            | `jsonb`                      | GIN-indexable blobs                            |
| Soft delete    | `is_archived BOOLEAN`         | —                            | Hide rows without losing history               |

`QueryHelper.active(query)` (see *models.py*) auto-filters archived rows.

---

## 1 · `users`

| column | type | description |
| ------ | ---- | ----------- |
| `id`                | INT PK |
| `user_id_str`       | VARCHAR(128) UNIQUE | login / SSO handle |
| `email`             | VARCHAR(255) UNIQUE | Required for human/admin users, NULL for model users |
| `password_hash`     | TEXT |
| `user_type`         | ENUM (human / model / admin) |
| `created_at`        | TIMESTAMPTZ |
| `updated_at`        | TIMESTAMPTZ |
| `is_archived`       | BOOL |

**Rationale** – Global identity; `user_type='admin'` bypasses project ACLs.  
Soft-disable via `is_archived` keeps audit history.
Email is only required for human and admin users; model users don't have emails.

---

## 2 · `videos`

| column | type    | notes |
| ------ | ------- | ----- |
| `id`             | INT PK |
| `video_uid`      | VARCHAR(255) UNIQUE | file-name or UUID |
| `url`            | TEXT |
| `video_metadata` | JSONB |
| `created_at`     | TIMESTAMPTZ |
| `updated_at`     | TIMESTAMPTZ |
| `is_archived`    | BOOL |

**Rationale** – `video_uid` lets UI find assets without joins; metadata stays searchable.  
Archiving supports takedowns.

---

## 3 · `video_tags`

| column | type | notes |
| ------ | ---- | ----- |
| `video_id` / `tag` | INT, VARCHAR(64) — **composite PK** |
| `tag_source`       | ENUM (model / reviewer) |
| `created_at`       | TIMESTAMPTZ |

*Index*: GIN on `tag`.

**Rationale** – Global flags (NSFW, epilepsy). Only trusted actors write rows.

---

## 4 · `question_groups`

| column | type | default | description |
| ------ | ---- | ------- | ----------- |
| `id`           | INT PK |
| `title`        | VARCHAR(255) UNIQUE |
| `display_title`        | VARCHAR(255) |
| `description`  | TEXT |
| `is_reusable`  | BOOL | **FALSE** | `TRUE` ⇒ can be imported by many schemas (e.g., Shot Transition) |
| `is_auto_submit` | BOOL | **FALSE** | `TRUE` ⇒ answers are automatically submitted for annotation mode |
| `is_archived`  | BOOL | FALSE |

**Rationale** – Most groups one-time; reusable flag allows read-only sharing.

---

## 5 · `questions`

| column | type | notes |
| ------ | ---- | ----- |
| `id`                 | INT PK |
| `text`               | TEXT UNIQUE, immutable after creation |
| `display_text`       | TEXT, editable, for UI |
| `type`               | ENUM (single / description) |
| `options`            | JSONB array nullable (single-choice) |
| `display_values`     | JSONB array nullable (single-choice) |
| `option_weights`     | JSONB array nullable (single-choice) |
| `default_option`     | VARCHAR(120) nullable |
| `is_archived`        | BOOL |
| `created_at`         | TIMESTAMPTZ |

**Rationale** – Supports both radio and free-text; single-choice values indexed, descriptions not.  
Question text must be unique and is immutable after creation (use display_text for UI changes).  
For single-choice questions:
- `options` stores the actual values used in answers
- `display_values` stores the UI-friendly text for each option
- `option_weights` stores the weight for each option (defaults to 1.0)
- Both arrays must have matching lengths
- For description-type questions, all fields are NULL

---

## 5.1 · `question_group_questions`

| column | type | notes |
| ------ | ---- | ----- |
| `question_group_id` | INT |
| `question_id`      | INT |
| `display_order`    | INT |

**Link** – PK `(question_group_id, question_id)`.

**Rationale** – Many-to-many relationship between questions and groups with ordering.

---

## 6 · `schemas`

| column | type | notes |
| ------ | ---- | ----- |
| `id`          | INT PK |
| `name`        | TEXT UNIQUE NOT NULL |
| `instructions_url` | TEXT |
| `created_at`  | TIMESTAMPTZ |
| `updated_at`  | TIMESTAMPTZ |
| `has_custom_display` | BOOL |
| `is_archived` | BOOL |

**Link** `schema_question_groups` – PK `(schema_id, question_group_id)`, plus `display_order`.

**Rationale** – Reusable question sets; no FK allows forks without cascade.

---

## 6.1 · `schema_question_groups`

| column | type | notes |
| ------ | ---- | ----- |
| `schema_id`        | INT |
| `question_group_id`| INT |
| `display_order`    | INT |

**Link** – PK `(schema_id, question_group_id)`.

**Rationale** – Many-to-many relationship between schemas and question groups with ordering.

---

## 7 · `projects`

| column | type | notes |
| ------ | ---- | ----- |
| `id`          | INT PK |
| `name`        | TEXT |
| `schema_id`   | INT |
| `description` | TEXT |
| `created_at`  | TIMESTAMPTZ |
| `updated_at`  | TIMESTAMPTZ |
| `is_archived` | BOOL |

### 7.1 Links

| table | composite PK | notes |
| ----- | ------------ | ----- |
| `project_videos` | `(project_id, video_id)` | plus `added_at` |
| `project_user_roles` | `(project_id, user_id)` | `role`, `assigned_at`, `completed_at`, `user_weight`; index on `user_id` |

**Rationale** – A project = schema + video subset + roles; archiving hides it while retaining history.

---

## 8 · `project_groups`

| column | type | notes |
| ------ | ---- | ----- |
| `id`           | INT PK |
| `name`         | TEXT UNIQUE |
| `description`  | TEXT |
| `created_at`   | TIMESTAMPTZ |
| `is_archived`  | BOOL |

### 8.1 Link

| table | composite PK |
| ----- | ------------ |
| `project_group_projects` | `(project_group_id, project_id)` |

**Rationale** – Bundles arbitrary projects for export/report dashboards.

---

## 9 · `annotator_answers`

| column | type |
| ------ | ---- |
| `id` | INT PK |
| `video_id`, `question_id`, `user_id`, `project_id` | INT |
| `answer_type`      | ENUM (single / description) |
| `answer_value`     | TEXT |
| `confidence_score` | FLOAT nullable |
| `created_at`       | TIMESTAMPTZ |
| `modified_at`      | TIMESTAMPTZ |
| `notes`            | TEXT |

### 9.1 Constraints & Indexes

| kind | columns / condition |
| ---- | ------------------- |
| Unique | `(video_id, question_id, user_id, project_id)` |
| Index | `(question_id)` |
| Index | `(project_id, question_id)` |
| Index | `(project_id, question_id, answer_value)` **WHERE answer_type='single'`** |
| Index | `(video_id, question_id)` |
| Index | `(user_id, project_id)` |
| Index | `(user_id, project_id, question_id)` |

**Rationale** – Stores annotator submissions with confidence scores and modification history.

---

## 10 · `reviewer_ground_truth`

| column | type |
| ------ | ---- |
| `video_id`, `question_id`, `project_id` | INT (composite PK) |
| `reviewer_id`  | INT |
| `answer_type`  | ENUM (single / description) |
| `answer_value` | TEXT |
| `original_answer_value` | TEXT |
| `modified_at`  | TIMESTAMPTZ |
| `modified_by_admin_id` | INT nullable |
| `modified_by_admin_at` | TIMESTAMPTZ |
| `confidence_score` | FLOAT nullable |
| `created_at`  | TIMESTAMPTZ |
| `notes` | TEXT |

### 10.1 Constraints & Indexes

| kind | columns / condition |
| ---- | ------------------- |
| Index | `(question_id)` |
| Index | `(project_id, question_id)` |
| Index | `(project_id, question_id, answer_value)` **WHERE answer_type='single'`** |
| Index | `(video_id, question_id)` |
| Index | `(project_id, reviewer_id)` |
| Index | `(project_id, modified_by_admin_id)` |

**Rationale** – Stores ground truth answers with modification tracking for accuracy metrics.

---

## 11 · `answer_reviews`

| column | type | notes |
| ------ | ---- | ----- |
| `id` | INT PK |
| `answer_id` | INT UNIQUE | Reference to annotator_answers.id |
| `reviewer_id` | INT | Reviewer who performed the review |
| `status` | ENUM (pending / approved / rejected) |
| `comment` | TEXT |
| `reviewed_at` | TIMESTAMPTZ |

**Rationale** – Tracks review status and comments for annotator answers. One review per answer. Manual moderation of free-text; single-choice rows auto-grade.

---

## 12 · `project_video_question_displays`

| column | type | notes |
| ------ | ---- | ----- |
| `project_id`, `video_id`, `question_id` | INT — **composite PK** |
| `custom_display_text` | TEXT nullable | Override question display_text |
| `custom_display_values` | JSONB nullable | Override option display_values |
| `created_at` | TIMESTAMPTZ |
| `updated_at` | TIMESTAMPTZ |

### 12.1 Constraints & Indexes

| kind | columns / condition |
| ---- | ------------------- |
| Index | `(project_id)` |
| Index | `(project_id, video_id)` |

**Rationale** – Custom display overrides for specific project-video-question combinations. Only stores overrides when customization is needed. Performance optimized with schema-level `has_custom_display` flag to skip entire lookup for non-custom projects.

---

## Soft-Delete Strategy

* Tables with `is_archived` default to hidden.  
* Use `QueryHelper.active(query)` to auto-filter.  
* Cascade archival (e.g., project ⇒ its answers) inside service code so rules evolve freely.

---

*End of spec — `models.py` mirrors this schema exactly.*