# Label Pizza Setup Guide

## Quick Setup

**For a quick start, use the single command‑line tool:**

```bash
python upload_projects_from_folder.py --folder-path ./example
```

This single command imports everything in the `example/` folder — videos, users, question groups, schemas, projects, and even sample annotations — so you get a fully‑working demo in seconds. If you just want to see Label Pizza in action, run it and explore the UI. When you’re ready to tailor the workflow to your own data, continue with the rest of this guide to learn how to batch‑upload users, videos, question groups, schemas, and projects.

---

The rest of this README explains the detailed folder structure, JSON formats, and step‑by‑step process for anyone who wants to learn how to batch‑upload their own projects.

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
    "video_uid": "human.mp4" (optional, just for updating video),
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human.mp4",
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "pizza.mp4" (optional, just for updating video),
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/pizza.mp4",
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=8J1NzjA9jNg",
      "license": "Standard YouTube License"
    }
  }
]
```

The **`url`** must point straight to the video file itself, and the link must end with the actual filename like `my_clip.mp4`. Everything inside **`metadata`** is kept as-is for provenance. We recommend hosting services such as Hugging Face Datasets or S3 buckets for video files.

### `question_groups/`

Each JSON file defines *one* group of related questions.

Below is an example question group that asks annotators to report how many people appear in a video and, if any, to describe them.

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

### `users.json`

Lists the user accounts that should exist before projects are created. `user_type` can be `admin`, `human`, or `model`.

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

### `assignments.json`

Grants a **role** (`annotator`, `reviewer`, `admin`, or `model`) to a user within a project. Admins gain project access automatically, and once a user is created as `model` they cannot be switched to a human role (or vice‑versa) because model accounts store confidence scores.

```json
[
  {
    "user_email": "user1@example.com",
    "project_name": "Pizza Test 0",
    "role": "annotator"
  },
  {
    "user_email": "user1@example.com",
    "project_name": "Human Test 0",
    "role": "annotator"
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
init_database("DBURL")  # replace with your database URL name as stored in .env
```

### Step 1: Upload Videos

Upload all the videos defined in `videos.json`.

```python
from label_pizza.upload_utils import upload_videos

upload_videos(videos_path="./example/videos.json")
```

### Step 2: Register Question Groups and Schemas

Load the question groups and schemas from the `question_groups/` folder and `schemas.json`.

```python
from label_pizza.upload_utils import upload_question_groups, upload_schemas

upload_question_groups(
    question_groups_folder="./example/question_groups"
)

upload_schemas(
    schemas_path="./example/schemas.json"
)
```

### Step 3: Upload Users

Create the user accounts listed in `users.json`.

```python
from label_pizza.upload_utils import upload_users

upload_users(users_path="./example/users.json")
```

### Step 4: Create Projects

Generate projects from `projects.json`.

```python
from label_pizza.upload_utils import create_projects

create_projects(projects_path="./example/projects.json")
```

### Step 5: Assign Users to Projects

Assign roles to users as specified in `assignments.json`.

```python
from label_pizza.upload_utils import bulk_assign_users

bulk_assign_users(assignment_path="./example/assignments.json")
```

### Step 6: Upload Annotations and Reviews

Finally, upload any pre‑existing annotations and reviewer ground truth.

```python
from label_pizza.upload_utils import upload_annotations, upload_reviews

batch_upload_annotations(annotations_folder="./example/annotations")
batch_upload_reviews(reviews_folder="./example/reviews")
```





# Custom Display Text for Video Annotations

## Overview

Label Pizza allows you to customize how questions and options appear to annotators on a per-video basis within a project. This is useful when the same underlying question needs different wording depending on the video content.

## Quick Setup

For a quick start, use the single command-line tool:

bash

```bash
# Ensure your database is configured in .env
python upload_projects_from_folder.py --folder-path ./example_custom_questions/ --database-url-name DBURL_2
```

This command imports everything from the folder — videos, users, question groups, schemas, projects, custom displays, and sample annotations — giving you a fully-working demo in seconds.

## Folder Structure

```
example_custom_question/
├── videos.json          # Video metadata
├── question_groups/     # Question definitions
│   ├── humans.json
│   ├── pizzas.json
│   └── nsfw.json
├── schemas.json         # Schema definitions (must have has_custom_display: true)
├── users.json          # User accounts
├── projects.json       # Project configurations with custom displays
├── assignments.json    # User-project role assignments
├── annotations/        # Sample annotations (optional)
│   ├── humans.json
│   ├── pizzas.json
│   └── nsfw.json
└── reviews/           # Sample reviews (optional)
    ├── humans.json
    ├── pizzas.json
    └── nsfw.json
```



## Custom Display Configuration

### File Structure

The `projects.json` file supports two formats for video lists:

json

```json
[
  {
    "project_name": "Human Test Simple",
    "schema_name": "Questions about Humans Custom",
    "videos": ["human.mp4", "pizza.mp4"]  // Simple format: No custom displays
  },
  {
    "project_name": "Pizza Test Custom",
    "schema_name": "Questions about Pizzas Custom",
    "videos": [
      {
        "video_uid": "human.mp4",
        "questions": [
          {
            "question_text": "Pick one option",        // Original question text (required)
            "custom_question": "Is there a pizza?",    // Custom display text
            "custom_option": {                         // Custom option labels
              "Option A": "No",
              "Option B": "Yes, there is one"
            }
          },
          {
            "question_text": "Describe the object",
            "display_text": "If no pizza is shown, describe what you see instead."
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
            "display_text": "Describe the pizza toppings in detail."
          }
        ]
      }
    ]
  }
]
```

### Field Definitions

- **`question_text`** (required): The original question text as defined in the question group
- **`custom_question`** or **`display_text`**: The custom text to display for this question
- **`custom_option`** or **`option_map`**: Custom labels for multiple-choice options (key = original option, value = custom label)

### Synchronization Logic

When processing custom displays, the system follows these rules:

1. Simple Format

    (

   ```
   "videos": ["video1.mp4"]
   ```

   ):

   - Removes ALL custom displays for these videos
   - Used to reset videos to default question text

2. Detailed Format

    (with questions array):

   - **Creates** custom displays for questions specified in JSON but not in database
   - **Updates** custom displays when JSON differs from database
   - **Skips** custom displays when JSON matches database (no changes)
   - **Removes** custom displays that exist in database but not in JSON

### Requirements

- The schema must have `has_custom_display: true` enabled
- Question text must match exactly with the original question definition
- All videos must exist in the system
- Users must have appropriate permissions

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

## Example Use Cases

### 1. Context-Specific Questions

When annotating different types of videos with the same schema:

- Original: "Pick one option"
- For food videos: "What type of food is shown?"
- For animal videos: "What animal do you see?"

### 2. Instruction Variations

Providing different instructions based on expected content:

- Original: "Describe the object"
- For expected content: "Describe the pizza toppings"
- For unexpected content: "If no pizza is shown, describe what you see"

### 3. Language Adaptations

Adjusting terminology for different annotator groups while maintaining the same underlying data structure.

## Best Practices

1. **Test First**: Use a small subset of videos to verify custom displays work as expected
2. **Keep Backups**: Export your configuration before making bulk changes
3. **Use Clear Naming**: Make custom text clearly different from original to avoid confusion
4. **Document Changes**: Keep track of why certain customizations were made
5. **Validate JSON**: Ensure your JSON is valid before running imports

## Troubleshooting

- **"Schema does not have custom display enabled"**: Enable `has_custom_display` in the schema
- **"Question not in project schema"**: Ensure question_text matches exactly
- **"Video not found"**: Verify video UIDs match uploaded videos
- **Changes not appearing**: Check if displays were skipped (already identical)