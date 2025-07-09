---

Service-Layer API Spec

> **Scope** – pure-Python helpers that wrap SQLAlchemy sessions.
> **Goal** – allow UI (Streamlit, REST, CLI) and tests to call business logic without writing SQL.

---

## Index of Namespaces

| Namespace                                                  | Purpose                                     |
| ---------------------------------------------------------- | ------------------------------------------- |
| [`db`](#db)                                                | Engine & `SessionLocal` factory             |
| [`VideoService`](#videoservice)                            | CRUD for `videos` & progress per video      |
| [`ProjectService`](#projectservice)                        | CRUD for `projects`, assignments & progress |
| [`SchemaService`](#schemaservice)                          | CRUD for `schemas` and their questions      |
| [`QuestionService`](#questionservice)                      | CRUD for individual `questions`             |
| [`QuestionGroupService`](#questiongroupservice)            | CRUD for question groups & reuse rule       |
| [`AuthService`](#authservice)                              | Users, roles, login, project assignments    |
| [`AnnotatorService`](#annotatorservice)                    | Core answer submission & retrieval          |
| [`GroundTruthService`](#groundtruthservice)                | Ground truth management & accuracy metrics  |
| [`ProjectGroupService`](#projectgroupservice)              | Group projects & enforce uniqueness         |
| [`MetricsService`](#metricsservice--🚧)                    | Accuracy, consensus, trends                 |
| [`NotificationService`](#notificationservice--optional-🚧) | Feedback to annotators                      |

**Legend**

| Mark                                                          | Meaning |
| ------------------------------------------------------------- | ------- |
| ✔︎ Implemented in `services.py`                               |         |
| 🚧 Planned / not yet coded                                    |         |
| 🛡️ Enforces critical business rule (should be in test-suite) |         |

---

### db

| Function         | Returns                  | Notes                                                            |
| ---------------- | ------------------------ | ---------------------------------------------------------------- |
| `SessionLocal()` | `sqlalchemy.orm.Session` | Connects with `DBURL` from `.env`; `expire_on_commit=False`. 🛡️ |

**Rules:**
- Session must be closed after use
- All database operations must use this session factory


---

## VideoService

| Function                           | Status | Parameters                               | Returns            | Business rules enforced                              |
| ---------------------------------- | ------ | ---------------------------------------- | ------------------ | ---------------------------------------------------- |
| `get_all_videos(session)`          | ✔︎     | —                                        | `pandas.DataFrame` | Uses left joins to list each video + projects + GT ✓ |
| `add_video(video_uid, url, session)`          | ✔︎     | `video_uid:str`, `url:str`, `metadata:dict` (optional)    | `None`             | • `video_uid` unique 🛡️<br>• URL validation 🛡️<br>• Metadata validation 🛡️ |
| `get_video_by_uid(video_uid, session)` | ✔︎ | — | `Optional[Video]` | — |
| `get_video_by_url(url, session)` | ✔︎ | — | `Optional[Video]` | — |
| `get_video_url(video_id, session)` | ✔︎ | — | `str` | Raises if not found |
| `get_video_metadata(video_id, session)` | ✔︎ | — | `dict` | Raises if not found |
| `archive_video(video_id, session)` | ✔︎ | — | `None` | Raises if not found |
| `get_videos_with_project_status(session)` | ✔︎ | — | `DataFrame` | Shows project assignments and GT status |

**Rules:**
- Video UIDs must be unique
- URLs must be valid
- Metadata must be valid JSON
- Archived videos are hidden from normal operations
- Video status includes project assignments and ground truth completion

---

## ProjectService

| Function                                              | Status | Parameters | Returns     | Rules enforced                                         |
| ----------------------------------------------------- | ------ | ---------- | ----------- | ------------------------------------------------------ |
| `get_all_projects(session)`                           | ✔︎     | —          | `DataFrame` | shows vids, schema, % GT                               |
| `create_project(name, description, schema_id, video_ids, session)` | ✔︎     | —          | `Project`   | • schema not archived 🛡️<br>• videos not archived 🛡️ |
| `get_video_ids_by_uids(uids, session)`                | ✔︎     | —          | `list[int]` | —                                                      |
| `archive_project(project_id, session)`                | ✔︎     | —          | `None`      | • Sets `is_archived=True`<br>• blocks new answers 🛡️  |
| `progress(project_id, session)`                       | ✔︎     | —          | `dict`      | returns videos × questions × answers                   |
| `get_project_by_id(project_id, session)`              | ✔︎     | —          | `Project`   | —                                                      |
| `get_project_by_name(name, session)`                  | ✔︎     | —          | `Optional[Project]` | —                                           |
| `add_user_to_project(project_id, user_id, role, session, user_weight=None)` | ✔︎ | — | `None` | Role validation 🛡️ |
| `remove_user_from_project(user_id, project_id, session)`        | ✔︎     | —          | `None` | — |
| `remove_user_from_project(user_id, project_id, session)`       | ✔︎ | — | `None` | — |

**Rules:**
- Projects are immutable after creation (unless some videos or questions are archived)
- Archived projects block new answers
- Project names must be unique
- Users must have valid roles (annotator/reviewer/model/admin)
- Progress tracking includes all videos and questions

---

## SchemaService

| Function                                                  | Status | Parameters | Returns | Rules enforced |
| --------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `get_all_schemas(session)`                                | ✔︎     | —          | `DataFrame` | — |
| `get_schema_questions(schema_id, session)`                | ✔︎     | —          | `DataFrame` | — |
| `get_schema_id_by_name(name, session)`                    | ✔︎     | —          | `int` | Raises if not found |
| `create_schema(name, question_group_ids, session, instructions_url=None, has_custom_display=False)` | ✔︎ | — | `Schema` | • Unique name 🛡️<br>• Groups exist 🛡️<br>• URL validation 🛡️ |
| `edit_schema(schema_id, name=None, instructions_url=None, has_custom_display=None, session)` | ✔︎ | — | `None` | • Unique name 🛡️<br>• URL validation 🛡️ |
| `get_schema_details(schema_id, session)`                  | ✔︎     | —          | `Dict` | — |
| `archive_schema(schema_id, session)`                      | ✔︎     | —          | `None` | — |
| `unarchive_schema(schema_id, session)`                    | ✔︎     | —          | `None` | — |
| `get_question_group_order(schema_id, session)`            | ✔︎     | —          | `list[int]` | — |
| `update_question_group_order(schema_id, group_ids, session)` | ✔︎ | — | `None` | • Groups exist 🛡️ |
| `get_schema_by_name(name, session)`                       | ✔︎     | —          | `Schema` | — |
| `get_schema_by_id(schema_id, session)`                    | ✔︎     | —          | `Schema` | — |
| `get_schema_question_groups(schema_id, session)`          | ✔︎     | —          | `DataFrame` | — |

**Rules:**
- Schema names must be unique
- Schemas are immutable after creation
- Question group order can be modified
- All question groups must exist
- Archived schemas are hidden from normal operations

---

## QuestionGroupService

| Function                                                  | Status | Parameters | Returns | Rules enforced |
| --------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `get_all_groups(session)`                                 | ✔︎     | —          | `DataFrame` | Includes stats |
| `get_group_questions(group_id, session)`                  | ✔︎     | —          | `DataFrame` | — |
| `get_group_details(group_id, session)`                    | ✔︎     | —          | `dict` | — |
| `create_group(title, display_title, desc, is_reusable, question_ids, verification_function, , is_auto_submit, session)` | ✔︎ | — | `QuestionGroup` | • Unique title 🛡️<br>• Questions exist 🛡️<br>• is_auto_submit controls auto-submission in annotation mode |
| `get_group_by_name(name, session)`                        | ✔︎     | —          | `Optional[QuestionGroup]` | — |
| `get_group_by_id(group_id, session)`                      | ✔︎     | —          | `Optional[QuestionGroup]` | — |
| `edit_group(group_id, new_title, new_description, is_reusable, verification_function, is_auto_submit, session)` | ✔︎ | — | `None` | • Unique title 🛡️<br>• Reuse rule 🛡️<br>• Can update is_auto_submit 🛡️ |
| `archive_group(group_id, session)`                        | ✔︎     | —          | `None` | — |
| `unarchive_group(group_id, session)`                      | ✔︎     | —          | `None` | — |
| `get_question_order(group_id, session)`                   | ✔︎     | —          | `list[int]` | — |
| `update_question_order(group_id, question_ids, session)`  | ✔︎     | —          | `None` | • Questions exist 🛡️ |

**Rules:**
- Group titles must be unique
- Question display order can be modified
- All questions must exist
- Verification functions must be valid (in verify.py)

---

## QuestionService

| Function                                                 | Status | Parameters | Returns | Rules enforced |
| -------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `get_all_questions(session)`                             | ✔︎     | —          | `DataFrame` | — |
| `add_question(text, qtype, options, default, session, display_values=None, display_text=None, option_weights=None)` | ✔︎ | — | `Question` | • Default in options 🛡️<br>• Unique text 🛡️<br>• display_text is UI-only, text is immutable after creation 🛡️ |
| `get_question_by_text(text, session)`                    | ✔︎     | —          | `Dict` | — |
| `get_question_by_id(question_id, session)`               | ✔︎     | —          | `Dict` | — |
| `edit_question(question_id, new_display_text, new_opts, new_default, session, new_display_values=None, new_option_weights=None)` | ✔︎ | — | `None` | • Cannot change type 🛡️<br>• Cannot change text 🛡️<br>• Default in options 🛡️ |
| `archive_question(question_id, session)`                 | ✔︎     | —          | `None` | — |
| `unarchive_question(question_id, session)`               | ✔︎     | —          | `None` | — |

**Rules:**
- Question text must be unique and is immutable after creation (use display_text for UI changes)
- Question type cannot be changed
- Cannot remove options after created
- Can add new options after created
- Can change display order or text of the options
- Default option must be in options list
- Display values must match options length
- Archived questions are hidden from normal operations

---

## AuthService

| Function                                                        | Status | Parameters | Returns | Rules enforced |
| --------------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `seed_admin(session, email, pwd, user_id)`                                           | ✔︎     | —          | `None` | — |
| `authenticate(email, pwd, role, session)`                       | ✔︎     | —          | `Optional[dict]` | • Disabled = reject 🛡️ |
| `get_all_users(session)`                                        | ✔︎     | —          | `DataFrame` | — |
| `get_users_by_type(user_type, session)`                         | ✔︎     | —          | `list[User]` | — |
| `create_user(user_id, email, pwd, user_type, session, is_archived)` | ✔︎ | — | `User` | • Unique id/email 🛡️ |
| `update_user_id(user_id, new_user_id, session)`                 | ✔︎     | —          | `None` | • Unique id 🛡️ |
| `update_user_email(user_id, new_email, session)`                | ✔︎     | —          | `None` | • Unique email 🛡️ |
| `update_user_password(user_id, new_password, session)`          | ✔︎     | —          | `None` | — |
| `update_user_role(user_id, new_role, session)`                  | ✔︎     | —          | `None` | — |
| `toggle_user_archived(user_id, session)`                        | ✔︎     | —          | `None` | — |
| `get_project_assignments(session)`                              | ✔︎     | —          | `DataFrame` | — |
| `remove_user_from_project(user_id, project_id, session)`        | ✔︎     | —          | `None` | — |

**Rules:**
- User IDs and emails must be unique
- Admin users are also admin for all projects
- Project roles must be valid
- Archived users cannot authenticate
- Bulk operations are atomic

---

## AnnotatorService

| Function                                                                                                                  | Status | Parameters | Returns | Rules enforced |
| ------------------------------------------------------------------------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `submit_answer_to_question_group(video_id, project_id, user_id, question_group_id, answers, session, confidence_scores, notes)` | ✔︎ | — | `None` | • Scope UQ 🛡️<br>• Project not archived 🛡️<br>• User role assigned 🛡️<br>• Type & option validation 🛡️ |
| `get_answers(video_id, project_id, session)`                                                                              | ✔︎     | —          | `DataFrame` | — |
| `get_question_answers(question_id, project_id, session)`                                                                  | ✔︎     | —          | `DataFrame` | — |

**Rules:**
- One answer per (video, question, user, project)
- Project must be active
- User must have annotator role
- Answers must match question type
- Confidence scores must be valid float
- Notes are optional

---

## GroundTruthService

| Function                                                                                                                  | Status | Parameters | Returns | Rules enforced |
| ------------------------------------------------------------------------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `submit_ground_truth_to_question_group(video_id, project_id, reviewer_id, question_group_id, answers, session, confidence_scores, notes)` | ✔︎ | — | `None` | • Reviewer role required 🛡️<br>• Type & option validation 🛡️ |
| `get_ground_truth(video_id, project_id, session)`                                                                         | ✔︎     | —          | `DataFrame` | — |
| `get_ground_truth_for_question(video_id, project_id, question_id, session)`                                                                         | ✔︎     | —          | `Optional[Dict]` | — |
| `get_ground_truth_dict_for_question_group(video_id, project_id, question_group_id, session)`            | ✔︎     | —          | `Optional[Dict]` | — |
| `get_ground_truth_for_question_group(video_id, project_id, question_group_id, session)`            | ✔︎     | —          | `DataFrame` | — |
| `check_ground_truth_exists_for_question(video_id, project_id, question_id, session)`            | ✔︎     | —          | `bool` | — |
| `check_all_questions_have_ground_truth_for_group(video_id, project_id, question_group_id, session)`            | ✔︎     | —          | `bool` | — |
| `override_ground_truth_to_question_group(video_id, project_id, question_group_id, admin_id, answers, session)`           | ✔︎     | —          | `None` | • Admin role required 🛡️<br>• Tracks modifications 🛡️ |
| `get_reviewer_accuracy(project_id, session)`                                                                 | ✔︎     | —          | `Dict[int, Dict[int, Dict[str, int]]]` | — |
| `get_annotator_accuracy(project_id, session)`                                                                | ✔︎     | —          | `Dict[int, Dict[int, Dict[str, int]]]` | — |
| `submit_answer_review(answer_id, reviewer_id, status, session, comment)`                                                  | ✔︎     | —          | `None` | • Valid status 🛡️ |
| `get_answer_review(answer_id, session)`                                                                                   | ✔︎     | —          | `Optional[dict]` | — |

**Rules:**
- One ground truth per (video, question, project)
- Reviewer must have reviewer role
- Admin overrides are tracked
- Accuracy is based on admin modifications
- All operations are at question group level

---

## ProjectGroupService

| Function                                                                                                   | Status | Parameters | Returns | Rules enforced |
| ---------------------------------------------------------------------------------------------------------- | ------ | ---------- | ------- | -------------- |
| `create_project_group(name, description, project_ids, session)`                                            | ✔︎     | —          | `None` | • Unique name 🛡️<br>• Projects exist 🛡️ |
| `edit_project_group(group_id, name, description, add_project_ids, remove_project_ids, session)`            | ✔︎     | —          | `None` | • Unique name 🛡️<br>• Projects exist 🛡️ |
| `get_project_group_by_id(group_id, session)`                                                               | ✔︎     | —          | `Dict` | — |
| `list_project_groups(session)`                                                                             | ✔︎     | —          | `list[Dict]` | — |

**Uniqueness Rule:** For any two projects in a group, if their schemas have overlapping questions, they must not have any overlapping (non-archived) videos. If schemas have no overlapping questions, any videos are allowed.

---

## MetricsService  🚧

| Function                                            | Rule enforced                    |
| --------------------------------------------------- | -------------------------------- |
| `accuracy_by_user(project_id, session)`             | ignores questions lacking GT 🛡️ |
| `accuracy_by_question(project_id, session)`         | —                                |
| `five_day_trend(project_id, user_id=None, session)` | rolling window                   |

---

## NotificationService  (optional 🚧)

| Function                                                            | Purpose                 |
| ------------------------------------------------------------------- | ----------------------- |
| `record_correction(annotator_id, answer_id, new_gt_value, session)` | insert notification row |
| `get_unread(user_id, session)`                                      | fetch & mark-read       |


---

## Package Layout

```
label_pizza/
├ models.py
├ db.py
└ services.py
```