# Upload Functions API Reference

A comprehensive suite of functions for managing users, videos, questions, schemas, projects, and annotations in your system.

## Table of Contents

- Overview
- Core Functions
  - `upload_users()`
  - `upload_videos()`
  - `upload_question_groups()`
  - `upload_schemas()`
  - `create_projects()`
  - `bulk_assign_users()`
  - `upload_annotations()`
  - `upload_reviews()`
- Common Features
- Error Handling

## Overview

This API provides a robust set of upload functions with:

- **Atomic transactions** - All-or-nothing operations
- **Comprehensive validation** - Multi-phase checks before database modifications
- **Intelligent deduplication** - Smart handling of existing records
- **Detailed logging** - Clear progress tracking and error reporting

------

## Core Functions

### `upload_users()`

Processes user data supporting both creation and updates with dual-field matching.

#### Function Signature

```python
def upload_users(users_path: str = None, users_data: list[dict] = None) -> None
```

#### Parameters

| Parameter    | Type                    | Description                               |
| ------------ | ----------------------- | ----------------------------------------- |
| `users_path` | `str` (optional)        | Path to JSON file containing user records |
| `users_data` | `list[dict]` (optional) | In-memory list of user dictionaries       |

> **Note:** At least one parameter must be provided.

#### Example

```json
[
    {
        "user_id": "Admin 1",
        "email": "admin1@example.com",
        "password": "admin111",
        "user_type": "admin"
    },
    {
        "user_id": "User 1",
        "email": "user1@example.com",
        "password": "user111",
        "user_type": "human"
    },
    {
        "user_id": "Robot 1",
        "password": "robot111",
        "user_type": "model"
    }
]
```

#### Matching Logic

The function uses **dual-field matching** to determine if a user exists:

- Match on `user_id` OR `email` → **Update existing user**
- No match on either field → **Create new user**

#### Behavior Examples

| Scenario     | Database State             | Action           |
| ------------ | -------------------------- | ---------------- |
| New user     | Not found                  | ✅ Create         |
| Exact match  | Both fields match          | 🔄 Update         |
| Email change | Match on `user_id` only    | 🔄 Update email   |
| ID change    | Match on `email` only      | 🔄 Update user_id |
| Conflict     | Different users own fields | ❌ Error          |

------

### `upload_videos()`

Adds new video records, automatically skipping existing ones.

#### Function Signature

```python
def upload_videos(videos_path: str = None, videos_data: list[dict] = None) -> None
```

#### Parameters

| Parameter     | Type                    | Description                                |
| ------------- | ----------------------- | ------------------------------------------ |
| `videos_path` | `str` (optional)        | Path to JSON file containing video records |
| `videos_data` | `list[dict]` (optional) | In-memory list of video dictionaries       |

#### Example

```json
[
  {
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human.mp4",
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  },
  {
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/pizza.mp4",
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=8J1NzjA9jNg",
      "license": "Standard YouTube License"
    }
  }
]
```

#### Key Features

- **Duplicate handling**: Videos with existing URLs are automatically skipped
- **Batch processing**: Creates all valid new videos in a single transaction
- **Metadata validation**: Ensures proper structure before creation

------

### `upload_question_groups()`

Processes question groups from JSON files, supporting creation and limited updates.

#### Function Signature

```python
def upload_question_groups(question_groups_folder: str) -> dict
```

#### Parameters

| Parameter                | Type  | Description                                         |
| ------------------------ | ----- | --------------------------------------------------- |
| `question_groups_folder` | `str` | Path to folder containing question group JSON files |

#### Example

```json
{
    "title": "Human",
    "description": "Detect and describe all humans in the video.",
    "is_reusable": false,
    "is_auto_submit": false,
    "verification_function": "check_human_description",
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

#### Update Behavior

- **New groups**: Created with all fields and questions
- **Existing groups**: Only `display_title` and metadata can be updated
- **Title immutability**: The `title` field serves as a permanent identifier

#### Return Value

```python
{
    "created": [{"title": str, "id": int}, ...],
    "updated": [{"title": str, "id": int}, ...],
    "questions_created": [str, ...],
    "questions_found": [str, ...],
    "validation_errors": []
}
```

------

### `upload_schemas()`

Creates new schemas linking to question groups. **Create-only operation**.

#### Function Signature

```python
def upload_schemas(schemas_file: str) -> dict
```

#### Parameters

| Parameter      | Type  | Description                                     |
| -------------- | ----- | ----------------------------------------------- |
| `schemas_file` | `str` | Path to JSON file containing schema definitions |

#### Example

```json
[
  {
    "schema_name": "Questions about Humans",
    "question_group_names": [
      "Human", "NSFW"
    ]
  },
  {
    "schema_name": "Questions about Pizzas",
    "question_group_names": [
      "Pizza", "NSFW"
    ]
  }
]
```

#### Key Behaviors

- **No updates**: Existing schemas cause immediate errors
- **Dependency validation**: All referenced question groups must exist and be active
- **Atomic creation**: All schemas created or none

------

### `create_projects()`

Creates new projects linking schemas and videos.

#### Function Signature

```python
def create_projects(projects_path: str = None, projects_data: list[dict] = None) -> None
```

#### Example

```json
[
  {
    "project_name": "Human Test 0",
    "schema_name": "Questions about Humans",
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  },
  {
    "project_name": "Pizza Test 0",
    "schema_name": "Questions about Pizzas",
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  }
]
```

#### Validation

- Schema must exist in database
- All video UIDs must be valid
- Project names must be unique

------

### `bulk_assign_users()`

Assigns users to projects with specific roles, supporting updates.

#### Function Signature

```python
def bulk_assign_users(assignment_path: str = None, assignments_data: list[dict] = None) -> None
```

#### Example

```json
[
  {
    "user_name": "User 1",
    "project_name": "Pizza Test 0",
    "role": "annotator"
  },
  {
    "user_name": "User 1",
    "project_name": "Human Test 0",
    "role": "annotator"
  },
  {
    "user_name": "Robot 1",
    "project_name": "Human Test 0",
    "role": "model"
  }
]
```

#### Role Validation Rules

| User Type | Allowed Roles                  |
| --------- | ------------------------------ |
| `admin`   | Cannot be assigned to projects |
| `model`   | Only `model` role              |
| `human`   | `annotator`, `reviewer`        |

------

### `upload_annotations()`

Uploads user annotations with intelligent change detection.

#### Function Signature

```python
def upload_annotations(rows: List[Dict[str, Any]]) -> None
```

#### Example

```python
[
  {
    "question_group_title": "Pizza",
    "project_name": "Pizza Test 0",
    "user_name": "User 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of pizzas?": "0",
      "If there are pizzas, describe them.": ""
    },
    "is_ground_truth": false
  },
  {
    "question_group_title": "Pizza",
    "project_name": "Pizza Test 0",
    "user_name": "User 1",
    "video_uid": "pizza.mp4",
    "answers": {
      "Number of pizzas?": "1",
      "If there are pizzas, describe them.": "The huge pizza looks delicious."
    },
    "is_ground_truth": false
  }
]
```

#### Key Features

- **Change detection**: Only uploads when values differ from existing
- **Role validation**: User must have annotator role
- **Group-level operations**: Processes entire question groups atomically

------

### `upload_reviews()`

Uploads ground truth reviews with strict validation.

#### Function Signature

```python
def upload_reviews(rows: List[Dict[str, Any]]) -> None
```

#### Example

```python
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

#### Requirements

- `is_ground_truth` must be `True`
- User must have reviewer role
- Creates authoritative reference answers

------

## Common Features

### Transaction Safety

- All operations wrapped in database transactions
- Automatic rollback on any error
- Ensures data consistency

### Validation Phases

1. **Structure validation** - Check JSON/input format
2. **Dependency validation** - Verify referenced entities exist
3. **Business logic validation** - Apply domain-specific rules
4. **Execution** - Perform database operations

### Progress Tracking

- Visual progress bars using `tqdm`
- Detailed logging of operations
- Clear success/skip/error reporting

### Idempotency

- Safe to run multiple times
- Intelligent duplicate handling
- No unintended side effects

------

## Error Handling

### Common Error Types

| Error Type           | Description                    | Recovery                      |
| -------------------- | ------------------------------ | ----------------------------- |
| **Validation Error** | Invalid input format or values | Fix input data                |
| **Dependency Error** | Referenced entity not found    | Create missing entities first |
| **Conflict Error**   | Unique constraint violation    | Resolve conflicts             |
| **Permission Error** | Insufficient user permissions  | Check user roles              |

### Error Response Format

All functions provide detailed error messages including:

- Specific field causing the error
- Clear description of the issue
- Suggested resolution steps

### Best Practices

1. **Validate locally first** - Check JSON structure before uploading
2. **Upload in order** - Users → Videos → Question Groups → Schemas → Projects
3. **Use transactions** - Let the system handle rollbacks
4. **Monitor logs** - Check detailed output for debugging
5. **Handle duplicates** - System will skip or update as appropriate