# Upload Functions API Reference

A comprehensive suite of functions for managing users, videos, questions, schemas, projects, and annotations in your system.

## Table of Contents

- [Overview](#overview)
- [Core Functions](#core-functions)
  - [`upload_users()`](#upload_users)
  - [`upload_videos()`](#upload_videos)
  - [`upload_question_groups()`](#upload_question_groups)
  - [`upload_schemas()`](#upload_schemas)
  - [`create_projects()`](#create_projects)
  - [`bulk_assign_users()`](#bulk_assign_users)
  - [`upload_annotations()`](#upload_annotations)
  - [`upload_reviews()`](#upload_reviews)
  - [`apply_simple_video_configs()`](#apply_simple_video_configs)
  - [`batch_upload_annotations()`](#batch_upload_annotations)
  - [`batch_upload_reviews()`](#batch_upload_reviews)
- [Common Features](#common-features)
- [Error Handling](#error-handling)

## Overview

This API provides a robust set of upload functions with:

- **Atomic transactions** - All-or-nothing operations
- **Comprehensive validation** - Multi-phase checks before database modifications
- **Intelligent deduplication** - Smart handling of existing records
- **Service layer integration** - Uses centralized validation from services
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

Handles both new video creation and existing video updates based on presence of `video_uid`.

#### Function Signature

```python
def upload_videos(videos_path: str = None, videos_data: list[dict] = None) -> None
```

#### Parameters

| Parameter     | Type                    | Description                                |
| ------------- | ----------------------- | ------------------------------------------ |
| `videos_path` | `str` (optional)        | Path to JSON file containing video records |
| `videos_data` | `list[dict]` (optional) | In-memory list of video dictionaries       |

#### Example - New Videos (no video_uid)

```json
[
  {
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human.mp4",
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  }
]
```

#### Example - Update Existing Videos (with video_uid)

```json
[
  {
    "video_uid": "human.mp4",
    "url": "https://example.com/updated-video.mp4",
    "metadata": {
      "title": "Updated Video Title"
    }
  }
]
```

#### Key Features

- **Intelligent routing**: Videos without `video_uid` are always added as new
- **Database lookup**: Videos with `video_uid` are checked against database
- **Batch processing**: Efficiently handles both operations in single call

------

### `upload_question_groups()`

Processes question groups from JSON files with comprehensive validation.

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
    "display_title": "Human Detection Questions",
    "description": "Detect and describe all humans in the video.",
    "is_reusable": false,
    "is_auto_submit": false,
    "verification_function": "check_human_description",
    "questions": [
        {
            "qtype": "single",
            "text": "Number of people?",
            "display_text": "Number of people?",
            "options": ["0", "1", "2", "3 or more"],
            "display_values": ["0", "1", "2", "3 or more"],
            "option_weights": [1.0, 1.0, 1.0, 1.0],
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

#### Processing Logic

- **New groups**: Created with all fields and questions
- **Existing groups**: Only `display_title` and metadata updated
- **Title immutability**: `title` serves as permanent identifier
- **Three-phase validation**: JSON structure → Database state → Execution

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

Handles both schema creation and updates based on name matching.

#### Function Signature

```python
def upload_schemas(schemas_path: str = None, schemas_data: list[dict] = None) -> None
```

#### Parameters

| Parameter      | Type                    | Description                                     |
| -------------- | ----------------------- | ----------------------------------------------- |
| `schemas_path` | `str` (optional)        | Path to JSON file containing schema definitions |
| `schemas_data` | `list[dict]` (optional) | In-memory list of schema dictionaries          |

#### Example - New Schemas

```json
[
  {
    "schema_name": "Questions about Humans",
    "question_group_names": ["Human", "NSFW"],
    "instructions_url": "https://example.com/instructions",
    "has_custom_display": true
  }
]
```

#### Example - Update Existing Schemas

```json
[
  {
    "schema_name": "Existing Schema Name",
    "instructions_url": "https://example.com/new-instructions",
    "has_custom_display": false,
    "is_archived": false
  }
]
```

#### Key Behaviors

- **Name-based matching**: Existing schemas identified by `schema_name`
- **Dependency validation**: All referenced question groups must exist
- **Flexible updates**: Only provided fields are updated

------

### `create_projects()`

Creates new projects linking schemas and videos with comprehensive validation.

#### Function Signature

```python
def create_projects(projects_path: str = None, projects_data: list[dict] = None) -> None
```

#### Example

```json
[
  {
    "project_name": "Human Test 1",
    "schema_name": "Questions about Humans",
    "videos": ["human.mp4", "pizza.mp4"]
  },
  {
    "project_name": "Pizza Test 1", 
    "schema_name": "Questions about Pizzas",
    "videos": {
      "human.mp4": [],
      "pizza.mp4": []
    }
  }
]
```

#### Key Features

- **Flexible video format**: Accepts both arrays and objects
- **Two-phase validation**: Verify all projects before creating any
- **Dependency checks**: Schema and video existence validated

------

### `bulk_assign_users()`

Assigns users to projects with role validation and conflict detection.

#### Function Signature

```python
def bulk_assign_users(assignment_path: str = None, assignments_data: list[dict] = None) -> None
```

#### Example

```json
[
  {
    "user_name": "User 1",
    "project_name": "Pizza Test 1",
    "role": "annotator"
  },
  {
    "user_name": "Robot 1",
    "project_name": "Human Test 1",
    "role": "model"
  }
]
```

#### Validation Rules

| User Type | Allowed Roles                  | Special Notes                    |
| --------- | ------------------------------ | -------------------------------- |
| `admin`   | Cannot be assigned to projects | Admin users bypass project ACLs |
| `model`   | Only `model` role              | AI/automated users               |
| `human`   | `annotator`, `reviewer`        | Human annotators and reviewers   |

#### Key Features

- **Duplicate detection**: Prevents multiple assignments to same project
- **Role validation**: Enforces user type constraints
- **Three-phase processing**: Structure → Dependencies → Execution

------

### `upload_annotations()`

Uploads user annotations with intelligent change detection and service layer validation.

#### Function Signature

```python
def upload_annotations(rows: List[Dict[str, Any]]) -> None
```

#### Example

```python
[
  {
    "question_group_title": "Pizza",
    "project_name": "Pizza Test 1",
    "user_name": "User 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of pizzas?": "0",
      "If there are pizzas, describe them.": ""
    },
    "confidence_scores": {
      "Number of pizzas?": 0.95
    },
    "notes": {
      "Number of pizzas?": "Very clear video"
    }
  }
]
```

#### Key Features

- **Service validation**: Uses `AnnotatorService.verify_submit_answer_to_question_group()`
- **Change detection**: Only uploads when values differ from existing
- **Role validation**: User must have annotator role in project
- **Group-level operations**: Processes entire question groups atomically

------

### `upload_reviews()`

Uploads ground truth reviews with strict validation requirements.

#### Function Signature

```python
def upload_reviews(rows: List[Dict[str, Any]]) -> None
```

#### Example

```python
[
  {
    "question_group_title": "Human",
    "project_name": "Human Test 1",
    "user_name": "Admin 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "Large man with beard"
    },
    "is_ground_truth": true,
    "confidence_scores": {
      "Number of people?": 1.0
    }
  }
]
```

#### Requirements

- **Ground truth flag**: `is_ground_truth` must be `True`
- **Reviewer role**: User must have reviewer role in project
- **Service validation**: Uses `GroundTruthService.verify_submit_ground_truth_to_question_group()`
- **Authoritative answers**: Creates reference truth for evaluation

------

### `apply_simple_video_configs()`

Applies custom display configurations for specific project-video-question combinations.

#### Function Signature

```python
def apply_simple_video_configs(config_file_path: str = None, configs_data: list[dict] = None) -> None
```

#### Example

```json
[
  {
    "project_name": "Human Test 1",
    "videos": {
      "human.mp4": [
        {
          "question_text": "Number of people?",
          "display_text": "How many people are visible?",
          "option_map": {
            "0": "(A) No people",
            "1": "(B) One person",
            "2": "(C) Two people"
          }
        }
      ]
    }
  }
]
```

#### Key Features

- **Custom displays**: Override question text and option labels per video
- **Synchronization**: Removes DB entries not in JSON, updates differences
- **Schema validation**: Requires schema with `has_custom_display=true`
- **Comprehensive validation**: Validates projects, videos, questions before any changes

------

### `batch_upload_annotations()`

Processes multiple annotation files concurrently for improved performance.

#### Function Signature

```python
def batch_upload_annotations(annotations_folder: str = None, annotations_data: list[dict] = None, max_workers: int = 4) -> None
```

#### Parameters

| Parameter            | Type                    | Description                                    |
| -------------------- | ----------------------- | ---------------------------------------------- |
| `annotations_folder` | `str` (optional)        | Path to folder containing annotation JSON files |
| `annotations_data`   | `list[dict]` (optional) | In-memory list of annotation data lists        |
| `max_workers`        | `int`                   | Number of concurrent workers (default: 4)      |

#### Key Features

- **Concurrent processing**: Uses ThreadPoolExecutor for parallel uploads
- **Error isolation**: Failures in one file don't affect others
- **Progress tracking**: Reports success/failure for each file

------

### `batch_upload_reviews()`

Processes multiple review files concurrently for improved performance.

#### Function Signature

```python
def batch_upload_reviews(reviews_folder: str = None, reviews_data: list[dict] = None, max_workers: int = 4) -> None
```

#### Parameters

| Parameter        | Type                    | Description                                |
| ---------------- | ----------------------- | ------------------------------------------ |
| `reviews_folder` | `str` (optional)        | Path to folder containing review JSON files |
| `reviews_data`   | `list[dict]` (optional) | In-memory list of review data lists        |
| `max_workers`    | `int`                   | Number of concurrent workers (default: 4)  |

#### Key Features

- **Concurrent processing**: Parallel upload of ground truth reviews
- **Ground truth validation**: Ensures `is_ground_truth=true` requirement
- **Error handling**: Individual file errors don't stop batch processing

------

## Common Features

### Service Layer Integration

All upload functions now leverage centralized validation through service classes:

- **AnnotatorService**: Handles annotation validation and submission
- **GroundTruthService**: Manages ground truth review validation
- **AuthService**: User authentication and role management
- **ProjectService**: Project and video relationship management
- **SchemaService**: Schema and question group validation

### Transaction Safety

- All operations wrapped in database transactions
- Automatic rollback on any error
- Ensures data consistency across all operations

### Multi-Phase Validation

1. **Input validation** - Check JSON/parameter format
2. **Structure validation** - Verify required fields and data types
3. **Dependency validation** - Ensure referenced entities exist
4. **Business logic validation** - Apply domain-specific rules via services
5. **Execution** - Perform database operations

### Progress Tracking

- Visual progress bars using `tqdm` for long-running operations
- Detailed logging of operations with clear status indicators
- Comprehensive success/skip/error reporting

### Intelligent Processing

- **Change detection**: Only process records that actually need updates
- **Duplicate handling**: Smart handling of existing records
- **Conflict resolution**: Clear rules for handling data conflicts

------

## Error Handling

### Service Validation Errors

All functions now use service layer validation, providing:

- **Specific error messages**: Pinpoint exact validation failures
- **Consistent validation**: Same business rules across all entry points
- **Detailed context**: Clear indication of which record failed and why

### Common Error Types

| Error Type           | Description                    | Recovery                      |
| -------------------- | ------------------------------ | ----------------------------- |
| **Validation Error** | Invalid input format or values | Fix input data and retry      |
| **Dependency Error** | Referenced entity not found    | Create missing entities first |
| **Conflict Error**   | Unique constraint violation    | Resolve conflicts in data     |
| **Permission Error** | Insufficient user permissions  | Check user roles and access   |
| **Service Error**    | Business logic validation fail | Review business rules         |

### Error Response Format

All functions provide detailed error messages including:

- **Row/entry identification**: Which specific record caused the error
- **Validation context**: What validation rule was violated
- **Service feedback**: Specific error from the service layer
- **Suggested resolution**: Clear guidance on how to fix the issue

### Best Practices

1. **Validate dependencies first** - Ensure users, videos, question groups exist
2. **Upload in correct order** - Users → Videos → Question Groups → Schemas → Projects → Assignments → Annotations
3. **Use service validation** - Leverage centralized business logic
4. **Monitor detailed logs** - Check progress output for debugging
5. **Handle partial failures** - Some batch operations continue despite individual failures
6. **Test with small batches** - Validate your data format with small samples first

### Recovery Strategies

- **Transaction rollback**: Failed operations leave database unchanged
- **Partial success reporting**: Know exactly which records succeeded/failed
- **Idempotent operations**: Safe to retry after fixing data issues
- **Incremental processing**: Process data in manageable chunks