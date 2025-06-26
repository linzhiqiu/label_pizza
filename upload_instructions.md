## Quick Start

#### Folder Structure

> This directory provides a compact, end‑to‑end example of the files required to set up a video‑annotation workflow.  It is intended as a reference: copy the pieces you need, adjust the JSON to match your own questions and videos, then import the data with the project‑creation scripts.

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
|   └── nsfw.json
└── reviews/
    ├── humans.json
    ├── pizzas.json
    └── nsfw.json
```

## File‑by‑file guide

### `videos.json`

Contains one entry per video that should be available to projects.

```
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

The **`url`** field must be directly downloadable by your annotation platform.  Everything under **`metadata`** is passed through untouched; store provenance or licence notes here.

### `question_groups/`

Each file defines a *single* group of questions that can be reused across multiple schemas.  The example below shows the `humans.json` question group.

```
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

### `schemas.json`

A schema is simply a list of question‑group titles.  Projects reference schemas by name.

```
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

Defines every account that should be present before projects are created.  Accepted `user_type` values are `admin`, `human`, and `model`.

```
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

Binds a schema to a collection of videos.  Video filenames must match the `original_name` (plus extension) in `videos.json`.

```
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

Grants a **role** (annotator, reviewer, admin, or model) to a user within a specific project. Note that admins are automatically assigned to all projects with an admin role.

```
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

Both directories share the same JSON structure.  Use `annotations/` for initial answers and `reviews/` for ground‑truth results.

**Annotations structure:**
```
[
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_email": "user1@example.com",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    }
  },
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_email": "user1@example.com",
    "video_uid": "pizza.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    }
  }
]
```

**Reviews structure:**
```
[
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "reviewer_email": "admin1@example.com",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    }
  },
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "reviewer_email": "admin1@example.com",
    "video_uid": "pizza.mp4",
    "answers": {
      "Number of people?": "0",
      "If there are people, describe them.": ""
    }
  }
]
```

## Getting Started

Follow the steps **in order** so that every dependency (users → question groups → schemas → videos → projects → assignments → annotations) is satisfied.

#### Step 1 Upload Videos

> Upload all the videos from `./videos` folder. Videos should be stored in `.json` file.

```
from label_pizza.db import init_database
init_database()
from scripts.upload_utils import upload_videos
import json

# quickest: point to the JSON file
upload_videos(videos_path="./example/videos.json")

# alternative: pre-load the file yourself
with open("./example/videos.json") as f:
    videos = json.load(f)
upload_videos(videos_data=videos)
```

#### Step 2 Upload Users

> Upload all the users from `./users` folder. Users should be stored in `.json` file.

```
from label_pizza.db import init_database
init_database()
from scripts.upload_utils import upload_users
import json

upload_users(users_path="./example/users.json")

# or
with open("./example/users.json") as f:
    users = json.load(f)
upload_users(users_data=users)
```

#### Step 3 Register question groups and schemas

> Upload all the schemas / question groups / questions from `./schemas` and `./question_groups` folders. They should be stored in `.json` file.

```
from label_pizza.db import init_database
init_database()
from scripts.upload_utils import create_schemas

create_schemas(
    schemas_path="./example/schemas.json",
    question_groups_folder="./example/question_groups"
)
```

#### Optional: (Optional) Build `projects.json` from annotations

> Skip this step if `projects.json` is already prepared.

```
from label_pizza.db import init_database
init_database()
from import_annotations import get_project_from_annotations
import itertools, json

projects = list(itertools.chain.from_iterable([
    get_project_from_annotations(
        "./example/annotations/humans.json",
        schema_name="Questions about Humans"
    ),
    get_project_from_annotations(
        "./example/annotations/pizzas.json",
        schema_name="Questions about Pizzas"
    ),
]))

with open("./example/projects.json", "w") as f:
    json.dump(projects, f, indent=2)
```

#### Step 4 Create Projects from Annotations

>Create Projects from existing annotations / reviews (This is somehow complex now). You could just look it for reference.

```
from label_pizza.db import init_database
init_database()
from scripts.upload_utils import create_projects
import json

create_projects(projects_path="./example/projects.json")

# or
with open("./example/projects.json") as f:
    projects = json.load(f)
create_projects(projects_data=projects)
```

#### Step 5 Assign users to projects

> Assign Users to Projects from `./assignment` folder. Assignment should be stored in `.json` file.

```
from label_pizza.db import init_database
init_database()
from scripts.upload_utils import bulk_assign_users
import json

bulk_assign_users(assignment_path="./example/assignments.json")

# or
with open("./example/assignments.json") as f:
    assignments = json.load(f)
bulk_assign_users(assignments_data=assignments)
```

#### Step 6 Import annotations and reviews

> Import all the annotations from `./annotations` folder.

```
from label_pizza.db import init_database
init_database()
from import_annotations import import_annotations, import_reviews

import_annotations(annotations_folder="./example/annotations")
import_reviews(reviews_folder="./example/reviews")
```
