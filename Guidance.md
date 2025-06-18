# Database Utilities



This guide documents the **batch‑level helpers** in `upload_utils.py` for managing *videos*, *questions*, *question‑groups*, and *schemas* in your project database **together with the validation rules enforced by each helper**.



## 1 Video helpers

### 1.1 `add_videos(videos_data: list[dict])`

Add **brand‑new** videos in one transaction. If *any* URL already exists, the call aborts with `ValueError`.

```
from scripts.upload_utils import add_videos

videos = [
    {
        "url": "https://mycdn.com/clip_001.mp4",
        "metadata": {
            "fps": 30,
            "duration": 8.2,
            "tag": "snowboard"
        }
    },
    {
        "url": "https://mycdn.com/clip_002.mp4",
        "metadata": {
            "fps": 25,
            "duration": 8.0,
            "tag": "sunny"
        }
    }
]

add_videos(videos)
```

#### Validation rules

- **Input type** – `videos_data` must be a *list* of dictionaries, otherwise `TypeError`.
- **Per‑item structure** – each dict must contain:
  - `"url"` (str)
  - optional `"metadata"` (dict | None)
- **VideoService.verify_add_video** checks:
  - URL starts with `http://` or `https://`, ends with a filename containing an extension, and length ≤ 255.
  - The URL does **not** already exist in the DB. Duplicate URLs are collected and reported together.
  - Optional metadata passes internal schema checks (duration ≥ 0, numeric fps, etc.).
- **Atomicity** – all videos must pass validation; otherwise **nothing** is inserted.



### 1.2 `update_videos(videos_data: list[dict])`

Bulk‑edit **existing** videos (URL and/or metadata). Every target `video_uid` must already be in the DB.

```
from scripts.upload_utils import update_videos

updates = [
    {
        "video_uid": "clip_001.mp4",            # immutable PK
        "url": "https://newcdn.com/clip_001.mp4",  # new CDN
        "metadata": {"fps": 60}                  # patch metadata
    },
    {
        "video_uid": "clip_123.mp4",
        "url": "https://newcdn.com/clip_123.mp4",
        "metadata": None                          # wipe metadata
    }
]

update_videos(updates)
```

#### Validation rules

- `videos_data` must be a list → `TypeError` otherwise.
- Each dict requires `video_uid` (str) + `url` (str) and optional `metadata`.
- **VideoService.verify_update_video** enforces:
  - `video_uid` exists; missing IDs collected and reported.
  - New URL obeys the same syntax restrictions as in *Add* and is **unique** (cannot collide with another video).
  - New metadata conforms to schema.
- Transaction aborted if any entry fails; otherwise all changes are committed in one go.



## 2 Question‑Group workflow

### 2.1 `import_question_group(group_data: dict) -> int`

Create **or** update a *single* question‑group **atomically**. The helper will:

1. Validate / create / update every question inside `group_data["questions"]`.
2. Validate and (re‑)create the group itself with the final question IDs.

```
from scripts.upload_utils import import_question_group

group = {
    "title": "ColorGrading",
    "description": "Assess color temperature, colorfulness, overall brightness/exposure, and allow free-text notes about grading style.",
    "is_reusable": false,
    "is_auto_submit": false,
    "questions": [
        {
        "text": "the_color_tones_in_this_video_are",
        "qtype": "single",
        "required": true,
        "options": [
            "black_white",
            "complex_others",
            "complex_changing",
            "complex_contrasting",
            "warm",
            "cool",
            "neutral"
        ],
        "display_values": [
            "N/A (black-white)",
            "Changing and Contrasting",
            "Changing",
            "Contrast",
            "Warm",
            "Cool",
            "Neither Warm nor Cool"
        ],
        "default_option": "neutral",
        "display_text": "The color tones in this video are:"
        },
        {
        "text": "how_colorful_is_this_video",
        "qtype": "single",
        "required": true,
        "options": [
            "black_white",
            "complex_others",
            "complex_changing",
            "complex_contrasting",
            "low_colorfulness",
            "high_colorfulness",
            "neutral"
        ],
        "display_values": [
            "N/A (black-white)",
            "Changing + Contrast",
            "Changing",
            "Contrast",
            "Low colorfulness",
            "High colorfulness",
            "Neither low nor high colorfulness"
        ],
        "default_option": "neutral",
        "display_text": "How colorful is this video?"
        },
        {
        "text": "brightness_and_exposure",
        "qtype": "single",
        "required": true,
        "options": [
            "complex_others",
            "complex_changing",
            "complex_contrasting",
            "very_bright",
            "neutral",
            "very_dark"
        ],
        "display_values": [
            "Changing + Contrast",
            "Changing",
            "Contrast",
            "Overexposed / Very Bright",
            "Neither too bright nor too dark",
            "Underexposed / Very Dark"
        ],
        "default_option": "neutral",
        "display_text": "Brightness and Exposure:"
        },
        {
        "text": "describe_color_grading",
        "qtype": "text",
        "required": false,
        "options": null,
        "display_values": null,
        "default_option": null,
        "display_text": "Describe the color grading"
        }
    ]
}

group_id = import_question_group(group)
```

#### Validation rules

- `group_data` must be a dict, else `TypeError`.
- **Top‑level keys** required: `title`, `description`, `is_reusable`, `questions` (non‑empty list).
- **Per‑question constraints**
  - `text` immutable and globally unique.
  - `qtype` ∈ {`single`, `description`, `text`}.
  - If `qtype == "single"` ⇒ `options`, `display_values` (same length), **required**; `option_weights` optional but length‑matched.
  - If `qtype` is *not* `single` ⇒ the list‑based fields must be **None**.
  - `default_option`, if given, must be present in `options`.
- **Two‑pass check** – verify questions first (read‑only). Any failure aborts before writing. On success, apply edits/creates and finally create (or recreate) the group inside the same DB transaction.



### 2.2 `update_questions(questions_data: list[dict])`

Bulk‑edit **existing** questions (single‑choice or free‑text). New questions are *not* created here.

```
from scripts.upload_utils import update_questions

questions = [
    {
        "text": "brightness_and_exposure",     # existing single-choice
        "display_text": "Updated: Select the light brightness (new version)",
        "options": [            
            "complex_others",
            "complex_changing",
            "complex_contrasting",
            "very_bright",
            "neutral",
            "very_dark"
        ],
        "display_values": [
            "Changing + Contrast",
            "Changing",
            "Contrast",
            "Overexposed / Very Bright",
            "Neither too bright nor too dark",
            "Underexposed / Very Dark"
        ],
        "default_option": "neutral",
        "option_weights": [1, 1, 1, 0.5, 1, 1],
    },
    {
        "text": "describe_color_grading",
        "qtype": "text",
        "required": false,
        "options": null,
        "display_values": null,
        "default_option": null,
        "display_text": "Describe the color grading (new version)"
    }
]
```

#### Validation rules

- Input must be list → `TypeError` otherwise.
- Every `text` must already exist; missing IDs collected and abort the batch.
- **Per‑field constraints** identical to `import_question_group` rules.
- Two‑pass strategy: verify all edits, then apply inside one transaction.



### 2.3 `update_question_groups(groups_data: list[dict])`

Bulk‑edit **metadata** of existing groups (title itself is immutable).

```
from scripts.upload_utils import update_question_groups

groups = [
    {
        "title": "ColorGrading",
        "description": "Updated description (v2)",
        "is_reusable": True,
        "is_auto_submit": False,
        "verification_function": "" 
    }
]

update_question_groups(groups)
```

#### Validation rules

- Input list check → `TypeError` otherwise.
- Each `title` must exist; missing titles collected.
- `description` non‑empty str; `is_reusable`/`is_auto_submit` bool.
- Optional `verification_function` may be `""` or dotted path to callable.
- Two‑pass verify→apply, full rollback on any error.



## 3 Schema helper

### `create_schema(schema_data: dict) -> int`

Assemble a **new** schema from *existing* question‑groups.

```
from scripts.upload_utils import create_schema

schema_def = {
    "schema_name": "CameraLight v2",
    "question_group_names": ["LightingBasics", "CameraPose"]
}

schema_id = create_schema(schema_def)
```

#### Validation rules

- `schema_data` must be a dict; else `TypeError`.
- Keys required: `schema_name` (str) and non‑empty list `question_group_names`.
- Every referenced group name must exist; missing names abort.
- `SchemaService.verify_create_schema` ensures schema name is unique.
- Transaction rolls back on any failure.