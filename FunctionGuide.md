### upload_utils.py Usage Guide



This script provides batch tools for managing your database, including *video import/update*, *schema/question import*, and *project creation via json file*.



#### 1. update_or_add_videos

**Function:**

**Batch add or update videos in the database from a JSON file.**

**If a video already exists (by video_uid), its URL and metadata will be updated; otherwise, a new video will be created.**

**Example usage:**

```
from scripts.upload_utils import update_or_add_videos

update_or_add_videos('your_video_metadata.json')
```

```
###
The format of json file should be:
[
    {
        "video_uid": "xxx.mp4",
        "url": "http://yourdomain.com/xxx.mp4",
        "metadata": {
            "key1": "value1",
            "key2": "value2"
        }
    },
    {
        "video_uid": "xxx.mp4",
        "url": "http://yourdomain.com/xxx.mp4",
        "metadata": {
            "key1": "value1",
            "key2": "value2"
        }
    },
    ...
]
###
```



#### 2. import_schemas

**Function:**

**Batch import or update schemas, question groups, and questions from a JSON file.**

**Existing items will be updated; new ones will be created.**

**Example usage:**

```
from scripts.process_json_to_database import import_schemas

import_schemas('your_schema_questions.json')
```

```
###
The format of json file should be:
{
    "SchemaName1": [
        {
            "title": "Group Title",
            "description": "Description",
            "is_reusable": true,
            "is_auto_submit": false,
            "questions": [
                {
                    "text": "unique_question_id",
                    "display_text": "Question shown in UI",
                    "qtype": "single",
                    "options": ["A", "B", "C"],
                    "display_values": ["Option A", "Option B", "Option C"],
                    "default_option": "A"
                },
                {
                    "text": "unique_question_id",
                    "display_text": "Question shown in UI",
                    "qtype": "single",
                    "options": ["A", "B", "C"],
                    "display_values": ["Option A", "Option B", "Option C"],
                    "default_option": "A"
                }
            ]
        }
    ],
    "SchemaName2": [
        {
            "title": "Group Title",
            "description": "Description",
            "is_reusable": true,
            "is_auto_submit": false,
            "questions": [
                {
                    "text": "unique_question_id",
                    "display_text": "Question shown in UI",
                    "qtype": "single",
                    "options": ["A", "B", "C"],
                    "display_values": ["Option A", "Option B", "Option C"],
                    "default_option": "A"
                },
                {
                    "text": "unique_question_id",
                    "display_text": "Question shown in UI",
                    "qtype": "single",
                    "options": ["A", "B", "C"],
                    "display_values": ["Option A", "Option B", "Option C"],
                    "default_option": "A"
                }
            ]
        }
    ],
    ...
}
###
```



#### 3. **create_project_from_annotation_json**

**Function:**

**Batch create projects and assign videos to them based on video names found in an annotation JSON file.**

**Videos must already exist in the database.**

**Example usage:**

```
from scripts.process_json_to_database import create_project_from_annotation_json

create_project_from_annotation_json(
    json_path="my_annotations.json",
    project_name="MyProject",
    schema_name="MySchema",
    batch_size=50  # Number of videos per project
)
```

```
###
The format of json file should be:
[
  {
    "video1.mp4": [
      { "email1": { "Q1": "A", "Q2": "B" } },
      { "email2": { "Q1": "A", "Q2": "B" } }
    ]
  },
  {
    "video2.mp4": [
      { "email1": { "Q1": "A", "Q2": "B" } }
    ]
  },
  ...
]
###
```



#### 4. upload_users_from_json

**Function:**

**Batch create users in the database from a JSON file.**

**If a user with the same email or user_id already exists, it will be skipped.**

**Example usage:**

```
from scripts.process_json_to_database import upload_users_from_json

upload_users_from_json("your_users.json")
```

```
[
    {
        "user_id": "alice",
        "email": "alice@example.com",
        "password": "alicepassword",
        "user_type": "human"
    },
    {
        "user_id": "bob",
        "email": "bob@example.com",
        "password": "bobpassword",
        "user_type": "human"
    },
    ...
]
```

