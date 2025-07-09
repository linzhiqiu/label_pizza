# Label Pizza Setup Guide

## Quick Setup

**For a quick start, use the single command‑line tool:**

```bash
python upload_projects_from_folder.py --folder-path ./example
```

This single command imports everything in the `example/` folder — videos, users, question groups, schemas, projects, and even sample annotations — so you get a fully‑working demo in seconds. If you just want to see Label Pizza in action, run it and explore the UI. When you’re ready to tailor the workflow to your own data, continue with the rest of this guide to learn how to batch‑upload users, videos, question groups, schemas, and projects.

---

The rest of this README explains the detailed folder structure, JSON formats, and step‑by‑step process for anyone who wants to learn how to batch‑upload their own projects.

> If you want to archive any schema, you should ensure that all the projects that use the schema have been archived. You could see Step4 and Step5 to see how to archive schemas and projects separately.

## Folder Structure

> This directory provides a compact, end‑to‑end example of the files required to set up a video‑annotation workflow. Copy whichever pieces you already have, adjust the JSON to match your questions and videos, and import them with the project‑creation scripts. Any missing parts (e.g., annotations or reviews) can always be added later through the web interface.

```
example/
├── videos.json
├── question_groups/
│   ├── humans.json
│   ├── pizzas.json
│   └── nsfw.json
├── schemas.json
├── users.json
├── projects.json
├── assignments.json
├── annotations/
│   ├── humans.json
│   ├── pizzas.json
│   └── nsfw.json
└── reviews/
    ├── humans.json
    ├── pizzas.json
    └── nsfw.json
```

## Folder Structure / JSON Format

### `videos.json`

Contains one entry per video.

```json
[
  {
    "video_uid": "human.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human.mp4",
    "is_active": true,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "pizza.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/pizza.mp4",
    "is_active": true,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=8J1NzjA9jNg",
      "license": "Standard YouTube License"
    }
  }
]
```

The **`url`** must point straight to the video file itself, and the link must end with the actual filename like `my_clip.mp4`. Everything inside **`metadata`** is kept as-is for provenance. We recommend hosting services such as Hugging Face Datasets or S3 buckets for video files.

### `question_groups/`

Each JSON file defines *one* group of related questions.

Below is an example question group that asks annotators to report how many people appear in a video and, if any, to describe them.

```json
{
    "title": "Human",
    "display_title": "Human",
    "description": "Detect and describe all humans in the video.",
    "is_reusable": false,
    "is_auto_submit": false,
    "verification_function": "check_human_description",
    "is_active": true,
    "questions": [
        {
            "qtype": "single",
            "text": "Number of people?",
            "display_text": "Number of people?",
            "options": [
                "0",
                "1",
                "2",
                "3 or more"
            ],
            "display_values": [
                "0",
                "1",
                "2",
                "3 or more"
            ],
            "option_weights": [
                1.0,
                1.0,
                1.0,
                1.0
            ],
            "default_option": "0"
        },
        {
            "qtype": "description",
            "text": "If there are people, describe them.",
            "display_text": "If there are people, describe them."
        }
    ]
}
```

* **`text`** and **`options`** are immutable identifiers, whereas **`display_text`** and **`display_values`** can later be edited in the web UI for wording tweaks.
* **`option_weights`** let you assign extra influence to certain answers in the weighted majority vote (for reviewer to resolve annotator disagreement), in case you need one option to carry more weight than the others.
* **`default_option`** pre‑selects a choice when the task opens for both annotators and reviewers.
* **`is_reusable`** indicates whether this question group can be added to multiple schemas.
* **`is_auto_submit`** automatically submits the default answer as soon as the video loads. For example, if 99 % of your clips are safe, auto‑submitting "No" to an NSFW question saves annotators from repeatedly clicking the obvious answer.
* Current `qtype` values are `single` (single‑choice) and `description` (free‑text).

### `schemas.json`

A schema is a set of question groups.

```json
[
  {
    "schema_name": "Questions about Humans",
    "instructions_url": "",
    "question_group_names": [
      "Human", "NSFW"
    ],
    "has_custom_display": true,
    "is_active": true
  },
  {
    "schema_name": "Questions about Pizzas",
    "instructions_url": "",
    "question_group_names": [
      "Pizza", "NSFW"
    ],
    "has_custom_display": true,
    "is_active": true
  }
]
```

### `users.json`

Lists the user accounts that should exist before projects are created. `user_type` can be `admin`, `human`, or `model`.

```json
[
    {
        "user_id": "Admin 1",
        "email": "admin1@example.com",
        "password": "admin111",
        "user_type": "admin",
        "is_active": true
    },
    {
        "user_id": "User 1",
        "email": "user1@example.com",
        "password": "user111",
        "user_type": "human",
        "is_active": true
    },
    {
        "user_id": "Robot 1",
        "password": "robot111",
        "user_type": "model",
        "is_active": true
    }
]
```

### `projects.json`

A project applies a schema to a collection of videos.

```json
[
  {
    "project_name": "Human Test 0",
    "schema_name": "Questions about Humans",
    "description": "Project for human questions",
    "is_active": true,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  },
  {
    "project_name": "Pizza Test 0",
    "schema_name": "Questions about Pizzas",
    "description": "Project for pizza questions",
    "is_active": true,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  }
]
```

### `assignments.json`

Grants a **role** (`annotator`, `reviewer`, `admin`, or `model`) to a user within a project. Admins gain project access automatically, and once a user is created as `model` they cannot be switched to a human role (or vice‑versa) because model accounts store confidence scores.

```json
[
  {
    "user_email": "user1@example.com",
    "project_name": "Pizza Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": true
  },
  {
    "user_email": "user1@example.com",
    "project_name": "Human Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": true
  }
]
```

### `annotations/` and `reviews/`

Both directories share the same JSON structure: each file contains answers for a single question group across all projects and videos. Use `annotations/` for annotator answers and `reviews/` for reviewer ground truth (there can be only one ground‑truth answer per video‑question‑group pair).

#### Example annotations folder:

* `annotations/humans.json` - Contains all human‑related annotations
* `annotations/pizzas.json` - Contains all pizza‑related annotations
* `annotations/nsfw.json`  - Contains all NSFW‑related annotations

**Example `annotations/humans.json`:**

```json
[
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_name": "User 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    },
    "is_ground_truth": false
  },
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_name": "User 1",
    "video_uid": "pizza.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    },
    "is_ground_truth": false
  }
]
```

#### Example reviews folder:

* `reviews/humans.json` - Contains all human‑related ground‑truth reviews
* `reviews/pizzas.json` - Contains all pizza‑related ground‑truth reviews
* `reviews/nsfw.json`   - Contains all NSFW‑related ground‑truth reviews

**Example `reviews/humans.json`:**

```json
[
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_name": "Admin 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    },
    "is_ground_truth": true
  },
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_name": "Admin 1",
    "video_uid": "pizza.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    },
    "is_ground_truth": true
  }
]
```

**Important:** The `is_ground_truth: true` field marks reviewer ground‑truth answers. A (video, question group, project) can have at most one ground truth answer.

## Step‑by‑Step Upload Guide

Follow the steps **in order** so that every dependency (videos → question groups → schemas → users → projects → assignments → annotations) is satisfied.

### Step 0: Initialize Database

**Important:** Initialize the database before running any other steps.

```python
from label_pizza.db import init_database
init_database("DBURL")  # replace with your database URL name as stored in .env, e.g. init_database("DBURL2")
```

### Step 1: Sync Videos

Function for adding / editing / archiving videos

#### - Add videos

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_videos

videos_data = [
  {
    "video_uid": "human.mp4",	# Must NOT exist in the database
    "url": "https://your-repo/human.mp4",
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  }
]

sync_videos(videos_data=videos_data)
```

#### - Update videos

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_videos

videos_data = [
  {
    "video_uid": "human.mp4",                       # Must already exist in the database
    "url": "https://your-repo-new/human.mp4",	      # update url (Must not exist in the database)
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License Updated"	# update metadata
    }
  }
]

sync_videos(videos_data=videos_data)
```

#### - Archive videos

```python
from label_pizza.upload_utils import sync_videos

videos_data = [
  {
    "video_uid": "human.mp4",	# Must already exist in the database
    "url": "https://your-repo/human.mp4",
    "is_active": False,	      # Set to False to archive the video
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  }
]

sync_videos(videos_data=videos_data)
```

### Workflow

#### 1. Verify the json format and get all the videos

Check whether the json file is well-formated. Then get all the videos from the database.

#### 2. Decide what to do with each incoming record

For every video in the input list, the function asks one question:

- *Does the `video_uid` already exist?*

| Does the `video_uid` already exist? | Action                                              |
| ----------------------------------- | --------------------------------------------------- |
| **No**                              | Create this video                                   |
| **Yes**                             | Update this video (only non-UID fields may change). |

#### 3. Run safety checks before touching the database

- **Creates** → `verify_add_video()` confirms the UID is unique and the record meets all rules.
- **Updates** → `verify_update_video()` ensures only `url`, `metadata`, `is_archived`, and an `updated_at` timestamp will change, and that any new `url` still resolves to the same basename/UID.

Any failure here stops the upload cold.

#### 4. Apply everything in one atomic step

The function opens **one** database transaction:

1. **Creates** – insert every video from the create list.
2. **Updates** – modify only the allowed fields for each video in the update list.



### Step 2: Sync Users

Function for adding / editing / archiving users

#### - Add users

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_users

users_data = [
    {
        "user_id": "User 1",          # Must NOT exist in the database
        "email": "user1@example.com", # Must NOT exist in the database
        "password": "user111",
        "user_type": "human",
        "is_active": True
    }
]

sync_users(user_data=users_data)
```

#### - Update users

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_users

users_data = [
    {
        "user_id": "New User 1",          # must already exist OR email must match
        "email": "user1-new@example.com", # must already exist OR user_id must match
        "password": "user111-new",        # update password
        "user_type": "human",             # Could only select from "admin", "human" and "model"
        "is_active": True
    }
]

sync_users(users_data=users_data)
```

>  **Either user_id or email must already exist in the database**

##### Update users via `user_id`

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_users

users_data = [
    {
        "user_id":  "User 1",                  # must already exist
        "email":    "user1-new@example.com",   # new address (must be unused)
        "password": "user111-new",             # new password
        "user_type": "human",                  
        "is_active": True
    }
]

sync_users(users_data=users_data)
```

##### update users via `email`

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_users

users_data = [
    {
        "user_id":  "New User 1",              # new_user_id (must be unused)
        "email":    "user1@example.com",       # Must already exist
        "password": "user111-new",             # new password
        "user_type": "human",
        "is_active": True
    }
]

sync_users(users_data=users_data)
```

#### - Archive users

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_users

users_data = [
    {
        "user_id": "User 1",          # must already exist OR email must match
        "email": "user1@example.com", # exists in the database (optional)
        "password": "user111",
        "user_type": "human",
        "is_active": False            # Set to False to archive the video
    }
]

sync_users(users_data=users_data)
```

### Workflow

#### 1. Verify the json format and get all the videos

Check whether the json file is well-formated. Then get all the users from the database.

#### 2. Decide what to do with each incoming record

For every user in the input list, the function asks two questions:

- *Does the `user_id` already exist?*
- *Does the `email` already exist?*

Four outcomes are possible:

| What matches?                                         | Action                                                       |
| ----------------------------------------------------- | ------------------------------------------------------------ |
| Neither ID nor email                                  | **Create** a brand-new user.                                 |
| Both ID and email (and they point to the same person) | **Update** any other fields that changed.                    |
| ID matches but email belongs to nobody                | If that email is unused, treat it as an **update** (change the email and anything else). If the email is someone else’s, raise a conflict error. |
| Email matches but ID belongs to nobody                | Mirror of the previous case: if the ID is free, treat it as an **update**; otherwise raise a conflict. |

These decisions fill two internal lists—one for creations and one for updates.

#### 3. Run safety checks before touching the database

Every "create" entry goes through the standard `verify_add_user` validation. Every "update" entry is also checked by `verify_update_user`, and then validated again to catch any ID or email conflicts that may have been missed during the initial classification.

#### 4. Apply everything in one atomic step

With all conflicts ruled out, the function opens a single database transaction:

1. **Creates**: inserts every user in the creation list.
2. **Updates**: for each existing user, changes only the fields that differ (email, ID, password, role, active status).



### Step 3: Sync Question Groups

Function for adding / editing / archiving question groups

#### - Add Question Groups

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_question_groups

question_groups_data = [
    {
        "title": "Human",           # Must NOT exist in the database
        "display_title": "Human",
        "description": "Detect and describe all humans in the video.",
        "is_reusable": False,
        "is_auto_submit": False,
        "verification_function": "check_human_description",
        "is_active": True,
        "questions": [
            {
                "qtype": "single",
                "text": "Number of people?",
                "display_text": "Number of people?",
                "options": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "display_values": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "option_weights": [
                    1.0,
                    1.0,
                    1.0,
                    1.0
                ],
                "default_option": "0"
            },
            {
                "qtype": "description",
                "text": "If there are people, describe them.",
                "display_text": "If there are people, describe them."
            }
        ]
    }
]

sync_question_group(question_groups_data=question_groups_data)
```

#### - Update question groups

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_question_groups

question_groups_data = [
    {
        "title": "Human",                  # Must exist in the database
        "display_title": "Human Updated",  # update display_title
        "description": "Detect and describe all humans in the video. (Updated)", # update description here
        "is_reusable": True,               # update is_reusable
        "is_auto_submit": True,            # update is_auto_submit
        "verification_function": "check_human_description_update",  # update verification_function, must exist in verify.py
        "is_active": True,
        "questions": [                     # New question order applied; questions must remain exactly the same as before
            {
                "qtype": "description",
                "text": "If there are people, describe them.",
                "display_text": "If there are people, describe them."
            },
            {
                "qtype": "single",
                "text": "Number of people?",
                "display_text": "Number of people?",
                "options": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "display_values": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "option_weights": [
                    1.0,
                    1.0,
                    1.0,
                    1.0
                ],
                "default_option": "0"
            }
        ]
    }
]

sync_question_group(question_groups_data=question_groups_data)
```

#### - Archive question groups

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_question_groups

question_groups_data = [
    {
        "title": "Human",           # Must exist in the database
        "display_title": "Human",
        "description": "Detect and describe all humans in the video.",
        "is_reusable": False,
        "is_auto_submit": False,
        "verification_function": "check_human_description",
        "is_active": False,         # Set False to archive the question group
        "questions": [
            {
                "qtype": "single",
                "text": "Number of people?",
                "display_text": "Number of people?",
                "options": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "display_values": [
                    "0",
                    "1",
                    "2",
                    "3 or more"
                ],
                "option_weights": [
                    1.0,
                    1.0,
                    1.0,
                    1.0
                ],
                "default_option": "0"
            },
            {
                "qtype": "description",
                "text": "If there are people, describe them.",
                "display_text": "If there are people, describe them."
            }
        ]
    }
]

sync_question_groups(question_groups_data=question_groups_data)
```

### Workflow

#### 1. Verify the json format and get all the videos

Check whether the json file is well-formated. Then get all the question_groups from the database.

#### 2. Decide what to do with each incoming record

For every validated group the function asks one question:

- *Does a group with this `title` already exist?*

| Result  | Action                                                       |
| ------- | ------------------------------------------------------------ |
| **No**  | **Create** the question group.                               |
| **Yes** | **Update** the question group—only these fields may change: • `display_title` • `description` • `is_auto_submit`• `verification_function`• `is_active` |

These decisions fill two internal queues—one for creations and one for updates.

#### 3. Run safety checks before touching the database

- **Create** – run `verify_add_group()` (or similar) to confirm uniqueness of the title and legality of the verification function.
- **Update** – run `verify_update_group()` to ensure only allowed fields differ and that any new questions have unique `text` values.

Any failure aborts the entire import before a single write is attempted.

#### 4. Apply everything in one atomic step

With all checks green, the function starts **one database transaction**:

1. **Creates** – insert every group in the create list, along with all of its questions.
2. **Updates** – for each existing group:
   - Update `display_title`,  `description`,  `is_auto_submit`, `verification_function`, `is_active` if they changed.



### Step 4: Sync Schemas

#### - Add schemas

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_schemas

schemas_data = [
  {
    "schema_name": "Questions about Humans",   # Must NOT exist in the database
    "instructions_url": "",
    "question_group_names": [
      "Human", "NSFW"
    ],
    "has_custom_display": False,
    "is_active": True
  }
]

sync_schemas(schemas_data=schemas_data)
```

#### - Update schemas

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_schemas

schemas_data = [
  {
    "schema_name": "Questions about Humans",             # Must exist in the database
    "instructions_url": "https://your-instruction-rul",  # Update instruction_url
    "question_group_names": [                            # New group order applied; question groups should remain exactly the same as before
      "NSFW", "Human"
    ],
    "has_custom_display": True,                          # Update has_custom_display
    "is_active": True
  }
]

sync_schemas(schemas_data=schemas_data)
```

#### - Archive schemas

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_schemas

schemas_data = [
  {
    "schema_name": "Questions about Humans",   # Must exist in the database
    "instructions_url": "",
    "question_group_names": [
      "Human", "NSFW"
    ],
    "has_custom_display": True,
    "is_active": False                         # Set False to archive schema
  }
]

sync_schemas(schemas_data=schemas_data)
```

### Workflow

#### 1. Verify the json format and get all the videos

Check whether the json file is well-formated. Then get all the schemas from the database.

#### 2. Decide what to do with each incoming record

For every validated schema the function asks one question:

- *Does a schema with this `title` already exist?*

| Title already in DB? | Action                                                       |
| -------------------- | ------------------------------------------------------------ |
| **No**               | **Create** the schema.                                       |
| **Yes**              | **Update** the schema—only `display_title` (if present), `description`, and brand-new questions may change. |

#### 3. Run safety checks before touching the database

Perform a harmless `SELECT` on each title to confirm DB access; run `verify_add_schema()` on the create list and `verify_update_schema()` on the update list, ensuring only permitted changes and unique question texts.

#### 4. Apply everything in one atomic step

Start a single transaction: insert every group in the create list with all its questions, then update existing groups’ descriptions/titles and append only the new questions; commit if everything succeeds, otherwise roll back the entire batch.



### Step 5: Sync Projects

#### - Add projects

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_projects

projects_data = [
  {
    "project_name": "Human Test 0",            # Must Not exist in the database
    "schema_name": "Questions about Humans",   # Must Not exist in the database
    "description": "Test project for human questions",
    "is_active": True,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  }
]

sync_projects(projects_data=projects_data)
```

#### - Update projects

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_projects

projects_data = [
  {
    "project_name": "Human Test 0",            # Must exist in the database
    "schema_name": "Questions about Humans",   # Must exist in the database
    "description": "Test project for human questions updated",   # You could only update the description
    "is_active": True,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  }
]

sync_projects(projects_data=projects_data)
```

#### - Archive projects

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_projects

projects_data = [
  {
    "project_name": "Human Test 0",           # Must exist in the database
    "schema_name": "Questions about Humans",  # Must exist in the database
    "description": "Test project for human questions",
    "is_active": False,                       # Set False to archive the project
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  }
]

sync_projects(projects_data=projects_data)
```

### Workflow

####  1. Make sure the input is well-formed

Parse the project JSON; each entry must include `project_name`, `schema_name`, and a `videos` list that may contain either bare `video_uid` strings or detailed blocks with per-question custom-display definitions.

#### 2. Take stock of what already exists

Look up each `project_name` in the DB.
 *If the project is new* → it will be created.
 *If it already exists* → it will be updated (its description and video links refreshed).

#### 3. Process every video in the project

For each `video_uid` in the JSON: ensure the video exists and is linked to the project (create the link if missing). Then, for **every** question in the project’s schema, apply the rule set below:

| DB display? | JSON display? | Resulting action                                             |
| ----------- | ------------- | ------------------------------------------------------------ |
| **Yes**     | **No**        | **Delete** the custom display from the database.             |
| **No**      | **Yes**       | Text / options → **write** DB entry.                         |
| **Yes**     | **Yes**       | Text / options differ → **overwrite** DB entry. Text / options identical → **skip** (leave unchanged). |

*A bare `video_uid` string means “no JSON displays” for **all** questions of that video, so every existing display for that video is deleted.*

#### 4. Run safety checks before touching the database

Run `verify_add_project()` on new projects and `verify_update_project()` on existing ones, then dry-run every planned video link and custom-display change to ensure they respect schema rules and option labels—abort on the first failure.

#### 5. Apply everything in one atomic step

Start a single transaction: create any new projects with their video links, update existing projects, and execute the per-video/per-question display logic (delete, overwrite, or skip); commit if every operation succeeds, otherwise roll back the entire batch.



### Step 6: Upload Users to Projects

#### - Add user to project

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import bulk_assign_users

assignments_data = [
  {
    "user_name": "User 1",             # Must exists in the database
    "project_name": "Human Test 0",    # Must exists in the database
    "role": "annotator",               # You could only select from ["annotator", "reviewer", "admin", "model"]
    "user_weight": 1.0,                
    "is_active": True
  }
]

bulk_assign_users(assignments_data=assignments_data)
```

#### - Remove user from project

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import bulk_assign_users

assignments_data = [
  {
    "user_name": "User 1",
    "project_name": "Human Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": False                 # Set False to remove user from this project
  }
]

bulk_assign_users(assignments_data=assignments_data)
```

### Workflow

#### 1  |  Validate structure (no DB access)

- Every entry must have:
  `user_email` · `project_name` · `role` · `is_active (bool)`.
- Reject duplicates: the pair `(user_email, project_name)` may appear **only once**.
- Abort immediately on any missing key, wrong type, or duplicate pair.

#### 2  |  Validate dependencies (read-only DB)

For each record:

- Confirm the user (`user_email`) exists.
- Confirm the project (`project_name`) exists.
- Check that the requested `role` is permitted for the user’s `user_type`.
- Still **no writes**; abort on the first failure.

#### 3  |  Classify each assignment

| Current link in DB | `is_active` (JSON) | Role matches DB? | What to do                             |
| ------------------ | ------------------ | ---------------- | -------------------------------------- |
| **Absent**         | `true`             | –                | **Create** the user-project role link. |
| **Present**        | `true`             | **differs**      | **Update** the link to the new role.   |
| **Present**        | `true`             | **same**         | **Skip** – nothing changes.            |
| **Present**        | `false`            | –                | **Remove** (or deactivate) the link.   |
| **Absent**         | `false`            | –                | **Skip** – nothing to remove.          |

Run `verify_add_assignment()`, `verify_update_assignment()`, or `verify_remove_assignment()` for each planned operation before any write.

#### 4  |  Apply changes in one transaction

- Open a single DB transaction.
- Execute every **create**, **update**, and **remove** exactly as classified.
- **Commit** only if all succeed; otherwise **roll back** the entire batch.



### Step 7: Upload Annotations and Reviews

### `annotations/` and `reviews/`

Both directories share the same JSON structure: each file contains answers for a single question group across all projects and videos. Use `annotations/` for annotator answers and `reviews/` for reviewer ground truth (there can be only one ground‑truth answer per video‑question‑group pair).

#### - Upload annotations

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import upload_annotations

annotations_data = [
  {
    "question_group_title": "Human",    # Must exist in the database
    "project_name": "Human Test 0",     # Must exist in the database
    "user_name": "User 1",              # User must have at least "annotation" privileges
    "video_uid": "human.mp4",           # Video must exist in the project
    "answers": {        # Answers must include all and only the questions defined in the question group
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    },
    "is_ground_truth": True
  }
]

upload_annotations(annotations_data=annotations_data)
```

#### - Upload reviews

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import upload_reviews

reviews_data = [
  {
    "question_group_title": "Human",    # Must exist in the database
    "project_name": "Human Test 0",     # Must exist in the database
    "user_name": "User 1",              # User must have at least "annotation" privileges
    "video_uid": "human.mp4",           # Video must exist in the project
    "answers": {        # Answers must include all and only the questions defined in the question group
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    },
    "is_ground_truth": True             # Must be True
  }
]

upload_reviews(reviews_data=annotations_data)
```

### Workflow

1. **Load input**
   • Read JSON from file or memory.

2. **Phase 1 – Validation (no writes)**
   • Project exists and is not archived.
   • Its schema exists, belongs to the project, and has `has_custom_display`.
   • Each `video_uid` exists, belongs to the project, and is not archived.
   • Every `question_text` in detailed blocks is present in the schema.
   • If `custom_option` / `option_map` supplied, keys *exactly* match the original option labels.
   • The batch is rejected on the first error—**no DB writes yet**.

3. **Phase 2 – Apply changes (single transaction)**

   For **every** `(project, video, question)` triple in the project’s schema:

   ```
   json_has_display = definition present in JSON?
   db_has_display   = custom display row exists?
   
   if db_has_display and not json_has_display:
       REMOVE the custom display           # total_removed += 1
   elif json_has_display:
       if JSON == DB (same text & options):
           SKIP                            # total_skipped += 1
       else:
           UPSERT (create or overwrite)    # total_created / total_updated += 1
   ```

   *If a video is listed as a bare UID string* → remove **all** custom displays for that video.

4. **Commit** the transaction, or roll back if **any** error occurs.

5. **Report summary** counts: *created, updated, removed, skipped*.



# Custom Display Text for Video Annotations

### Set Custom Display text for video in any project

### 1. Sync Question Group and Schema

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_question_groups

question_groups_data = [
  {
    "title": "Pizza Custom",
    "display_title": "Pizza Custom",
    "description": "Detect and describe all pizzas in the video.",
    "is_reusable": False,
    "is_auto_submit": False,
    "verification_function": "check_pizza_description",
    "is_active": True,
    "questions": [
      {
          "qtype": "single",
          "text": "Pick one option",
          "display_text": "Pick one option",
          "options": [
              "Option A",
              "Option B"
          ],
          "display_values": [
              "Option A",
              "Option B"
          ],
          "option_weights": [
              1.0,
              1.0
          ],
          "default_option": "Option A"
      },
      {
        "qtype": "description",
        "text": "Describe one aspect of the video",
        "display_text": "Describe one aspect of the video"
      }
    ]
  }
]

sync_question_groups(question_groups_data=question_groups_data)

from label_pizza.upload_utils import sync_schemas

schemas_data = [
  {
    "schema_name": "Questions about Pizzas Custom",
    "instructions_url": "",
    "question_group_names": [
      "Pizza Custom"
    ],
    "has_custom_display": True,
    "is_active": True
  }
]

sync_schemas(schemas_data=schemas_data)
```

### 2. Sync Projects

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_projects

projects_data = [
  {
    "project_name": "Pizza Test 0 Custom",
    "schema_name": "Questions about Pizzas Custom",
    "description": "Test project for custom questions",
    "videos": [
      "human.mp4"
			"pizza.mp4"
    ]
  }
]

sync_projects(projects_data=projects_data)
```

### 3. Set Custom Display

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.upload_utils import sync_projects

projects_data = [
  {
    "project_name": "Pizza Test 0 Custom",
    "schema_name": "Questions about Pizzas Custom",
    "description": "Test project for custom questions",
    "videos": [
      {
        "video_uid": "human.mp4",
        "questions": [
          {
            "question_text": "Pick one option",
            "custom_question": "Is there a pizza in the video?",
            "custom_option": {
              "Option A": "No",
              "Option B": "Yes, there is."
            }
          },
          {
            "question_text": "Describe one aspect of the video",
            "display_text": "If no pizza is shown, describe what is present instead."
          }
        ]
      },
      {
        "video_uid": "pizza.mp4",
        "questions": [
          {
            "question_text": "Pick one option",
            "custom_question": "What type of pizza is shown?",
            "custom_option": {
                "Option A": "Pepperoni",
                "Option B": "Veggie"
            }
          },
          {
            "question_text": "Describe the object",
            "display_text": "Describe the type of pizza shown in the video."
          }
        ]
      }
    ]
  }
]

sync_projects(projects_data=projects_data)
```

### Processing Report

After running the configuration, you'll see a summary:

```
📊 Summary:
   • Created: 4
   • Updated: 2
   • Removed: 1
   • Skipped: 3
   • Total processed: 10
```