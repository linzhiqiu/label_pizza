# Label Pizza Setup Guide

## Quick Setup

**For a quick start, use the single command‑line tool:**

```bash
python sync_from_folder.py --folder-path ./example
```

This single command imports everything in the `example/` folder — videos, users, question groups, schemas, projects, and even sample annotations — so you get a fully‑working demo in seconds. If you just want to see Label Pizza in action, run it and explore the UI. When you’re ready to tailor the workflow to your own data, continue with the rest of this guide to learn how to batch‑upload users, videos, question groups, schemas, and projects.

---

The rest of this README explains the detailed folder structure, JSON formats, and step‑by‑step process for anyone who wants to learn how to batch‑upload their own projects.

## Folder Structure

> This directory provides a compact, end‑to‑end example of the files required to set up a video‑annotation workflow. Copy whichever pieces you already have, adjust the JSON to match your questions and videos, and import them with the project‑creation scripts. Any missing parts (e.g., annotations) can always be added later through the web interface.

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
├── project_groups.json
├── assignments.json
├── annotations/
│   ├── humans.json
│   ├── pizzas.json
│   └── nsfw.json
└── ground_truths/
    ├── humans.json
    ├── pizzas.json
    └── nsfw.json
```

## Folder Structure / JSON Format

### `videos.json`

Each entry represents one video. See the example file ([videos.json](example/videos.json)) below.

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
      "description": "A video of a pizza being made.",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "human2.mp4",
    "url": "https://www.youtube.com/embed/2vjPBrBU-TM?start=33&end=36",
    "is_active": true,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=2vjPBrBU-TM",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "human3.mp4",
    "url": "https://www.youtube.com/embed/2S24-y0Ij3Y?start=110&end=112",
    "is_active": true,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=2S24-y0Ij3Y",
      "license": "Standard YouTube License"
    }
  }
]
```

### **What is a video?**

The `url` field can point to any video resource—an MP4 file hosted on Hugging Face, S3, or another server, or even a YouTube link—so long as the link resolves to playable video content.

* **`video_uid`** is a unique, permanent identifier. Once a video is created, its `video_uid` never changes.
* **`url`** may be updated later (we will provide instructions for safe updates).
* **`is_active`** defaults to `true`. Setting it to `false` cleanly archives the video across the platform while retaining its record. For robustness, we do not offer a way to delete videos (and similary for other assets we will discuss later in this guide such as users, questions, projects, and etc.).
* Everything inside **`metadata`** remains exactly as supplied for provenance.
* You may supply a YouTube link in `url`. To reference a specific time span, use the YouTube embed format with `start` and `end` parameters, as illustrated above for `human2.mp4` and `human3.mp4`.

### `question_groups/*.json`

Each JSON file under `question_groups/` defines *one* group of logically related questions. The example below ([question_groups/humans.json](example/question_groups/humans.json)) includes a single-choice question and a description question that together ask annotators to state how many people appear in a video and, if any, to describe them.

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
            "display_text": "If there are people, describe them.",
            "default_option": null
        }
    ]
}
```

### **What is a question?**

Each item in the **`questions`** list defines a question shown to annotators. A question can either require choosing one answer from a list (**`single`**) or typing a free-text response (**`description`**).

#### Fields inside each item of the `questions` list

* **`qtype`** – `single` for single‑choice questions, `description` for free text.
* **`text`** – immutable unique identifier for the question; once created, it cannot be changed.
* **`display_text`** – if you want to change the phrasing of the question, update this field. (Custom per-video questions are also supported — this will be covered later in the guide.)
* **`default_option`** – the answer pre-loaded when the task opens. Use `null` if you don’t want to pre-select an answer.

For single-choice questions, you should also define the following fields:

* **`options`** – a list of internal values for each answer choice. These are immutable once created (i.e. you cannot change or delete them later; however, you can add new options, which we cover later in the guide).
* **`display_values`** – a list of human-readable labels shown to annotators. These can be edited later in the web UI.
* **`option_weights`** – an optional list of weights (floats) used when aggregating answers across multiple annotators. You can assign more weight to certain options to make them count more in majority voting. If you do not provide this list (`option_weights` is `null`), all options will have an equal weight of 1.

**Note:** These three lists must share the same length, and entries at the same index correspond to the same choice (i.e., `options[i]`, `display_values[i]`, and `option_weights[i]` all refer to the same choice).

### **What is a question group?**

A question group is an ordered list of logically related **`questions`** shown together on the annotation screen. For example, the group in the JSON example above contains one single-choice question and one description question that together ask annotators to report how many people appear in a video and, if any, to describe them.

#### Question‑group fields

* **`title`** – immutable unique identifier for the question group; once created, it cannot be changed.
* **`display_title`** – editable question group name shown to annotators (tip: use 1–3 words for best UI display).
* **`description`** – optional description text.
* **`is_reusable`** – whether this group can be reused across multiple schemas (a schema is a set of question groups, introduced next).
* **`is_auto_submit`** – if `true`, default answers are automatically submitted when the video loads. This is useful when one answer applies to the vast majority of videos — for example, if 99.9% of your videos are safe, you can auto-submit “No” to the NSFW question so annotators only need to correct it for the rare unsafe videos.
* **`is_active`** – archive flag; set to `false` to hide this question group while preserving history.
* **`verification_function`** – if not `null`, you can provide the name of a Python function defined in [verify.py](label_pizza/verify.py) that checks the consistency of answers before they’re submitted. If the check fails, the function should raise an error to block submission. For example, `check_human_description` ensures that if the number of people is greater than 0, the annotator must provide a non-empty description:

```python
def check_human_description(answers: Dict[str, str]) -> None:
    """Ensure the description answer is consistent with the count answer.

    Args:
        answers: Dictionary mapping question text to answer value
      
    Raises:
        ValueError: If the description is provided when there are no people, or if the description is not provided when there are people.
    """
    num_people = answers.get("Number of people?")
    description = answers.get("If there are people, describe them.")
    
    if num_people == "0" and description:
        raise ValueError("Description cannot be provided when there are no people")
    if num_people != "0" and not description:
        raise ValueError("Description must be provided when there are people")
```

### `schemas.json`

Each schema includes one or more question groups that together define an annotation task. The example below ([schemas.json](example/schemas.json)) includes two schemas:

```json
[
  {
    "schema_name": "Questions about Humans",
    "instructions_url": "https://en.wikipedia.org/wiki/Human",
    "question_group_names": [
      "Human", "NSFW"
    ],
    "has_custom_display": false,
    "is_active": true
  },
  {
    "schema_name": "Questions about Pizzas",
    "instructions_url": "https://en.wikipedia.org/wiki/Pizza",
    "question_group_names": [
      "Pizza", "NSFW"
    ],
    "has_custom_display": false,
    "is_active": true
  }
]
```

### **What is a schema?**

A **schema** defines the full set of questions shown to annotators for a given task. It consists of one or more previously defined question groups, bundled together in a specific order. For example, the first schema in the JSON above combines the `"Human"` and `"NSFW"` question groups into a single annotation interface for videos about humans.

Each schema also includes optional instructions and display settings.

#### Schema fields

* **`schema_name`** – unique identifier for the schema.
* **`instructions_url`** – optional URL pointing to external instructions (e.g., Google Doc, PDF, etc.) for annotators. This link will be shown at the top of the task screen.
* **`question_group_names`** – an ordered list of question group `titles`.
* **`has_custom_display`** – if `true`, allows each video to override `display_text` (question wording) and `display_values` (answer choices). In other words, different videos in the same project can show different questions or options. We explain how to use this later in the guide.
* **`is_active`** – archive flag; set to `false` to hide the schema while keeping its record.

### `users.json`

Each entry defines a user account. See the example file ([users.json](example/users.json)) below:

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

### **What is a user?**

Each user must have a unique `user_id`. If the user is a human or admin, they must also have a unique `email` for logging in. Model users do not use email and cannot log in through the UI.

* **`user_id`** – unique identifier, displayed in the UI.
* **`email`** – required only for human/admin users; must be unique.
* **`password`** – password for the user.
* **`user_type`** – must be one of: `human`, `admin`, or `model`.
  * You may switch between `human` and `admin` later.
  * Switching from `human` to `admin` gives the user access to **all** projects.
  * Downgrading from `admin` to `human` removes access to **all** previously assigned projects.
  * `model` users are fixed — you cannot switch between `model` and `human/admin`, since only model users support confidence scores.
* **`is_active`** – archive flag; set to `false` to deactivate the account while keeping its record.

> Admins automatically gain access to all projects with full privileges. Only assign `admin` to trusted stakeholders.

### `projects.json`

Each project applies a schema to a list of videos. See the example file ([projects.json](example/projects.json)) below:

```json
[
  {
    "project_name": "Human Test 0",
    "schema_name": "Questions about Humans",
    "description": "Test project for humans",
    "is_active": true,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  },
  {
    "project_name": "Pizza Test 0",
    "schema_name": "Questions about Pizzas",
    "description": "Test project for pizzas",
    "is_active": true,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  },
  {
    "project_name": "Human Test 1",
    "schema_name": "Questions about Humans",
    "description": "Test project for humans",
    "is_active": true,
    "videos": [
      "human2.mp4",
      "human3.mp4"
    ]
  }
]
```

### **What is a project?**

A **project** defines one labeling task: a schema applied to a set of videos.

* **`project_name`** – unique identifier for the project; once created, it cannot be changed.
* **`schema_name`** – the name of the schema used (from `schemas.json`).
* **`description`** – optional text shown in the UI.
* **`videos`** – list of video `video_uid`s included in the task.
* **`is_active`** – archive flag; set to `false` to hide this project while preserving history.

> It is also possible to customize the displayed question text (`display_text`) and answer options (`display_values`) for each video in this `videos` list. We explain this later in the guide.

### `project_groups.json`

Organizes multiple related projects into a named group. See the example file ([project\_groups.json](example/project_groups.json)) below:

```json
[
  {
    "project_group_name": "Example Project Group",
    "description": "This is a project group for human test",
    "projects": [
      "Human Test 0",
      "Human Test 1"
    ]
  }
]
```

### **What is a project group?**

A **project group** acts like a folder to organize related projects in the UI. This helps keep projects visually grouped on the website and simplifies answer export later on.

* **`project_group_name`** – unique name for the group.
* **`description`** – optional text.
* **`projects`** – list of project names (must match entries in `projects.json`).

> Two projects can only be grouped together if their schemas don’t share any non-reusable question groups. This avoids duplicate answers for the same video-question pair. Reusable question groups are allowed, but during export, their answers must match across projects. If not (e.g., one project marks a video safe and another marks it unsafe), the export will fail until the conflict is resolved.

### `assignments.json`

Grants a role to a user within a project. See the example file ([assignments.json](example/assignments.json)) below:

```json
[
  {
    "user_name": "User 1",
    "project_name": "Pizza Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": true
  },
  {
    "user_name": "User 1",
    "project_name": "Human Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": true
  }
]
```

### **What is an assignment?**

Each assignment gives a user a role (`annotator`, `reviewer`, `admin`, or `model`) for a specific project. A user can have different roles across different projects.

#### Supported roles

* **`annotator`** – submits first-pass answers. Can be a human or model user.
* **`reviewer`** – reviews annotator answers and sets ground truth. Reviewers may speed up review using majority voting among selected annotators: if all selected annotators agree on the same option and their combined weights (`user_weight` x `option_weight`) exceed a threshold, the answer can be auto-submitted without further review. Reviewers also have annotator access to the same project.
* **`admin`** (or **`meta-reviewer`**) – performs a final audit of reviewer answers. Admins automatically have annotator and reviewer access across all projects and do not require explicit assignments.
* **`model`** – behaves like an annotator but may include confidence scores. Once created, model users cannot be changed to human or admin.

#### Assignment fields

* **`user_name`** – must match a user in `users.json`.
* **`project_name`** – must match a project in `projects.json`.
* **`role`** – defines access level within the project. Admins do not need to be explicitly assigned.
* **`user_weight`** – optional float used in weighted majority voting. If not specified (`user_weight` is `null`), defaults to 1.0.
* **`is_active`** – archive flag; set to `false` to deactivate the assignment.

> For single-choice questions, we compute the weighted vote for each option as the sum of `user_weight × option_weight`. An option is auto-submitted only if it crosses a threshold, helping reviewers skip obvious cases.

### `annotations/*.json` and `ground_truths/*.json`

These folders store all answers submitted by annotators and reviewers:

* **`annotations/*.json`** holds first-pass answers from annotators (human or model).
* **`ground_truths/*.json`** stores the final reviewed answers. Only one ground-truth submission is allowed per *(video, project, question group)* triplet.

> A single *(video, project, question group)* triplet can have **many** annotator submissions, but **only one** reviewer-set ground truth.

For clarity, this guide organizes each file by question group (e.g., [annotations/humans.json](example/annotations/humans.json)). But you can also combine everything into a single file (e.g., `annotations/all.json` and `ground_truths/all.json`) or split by project or user.

#### Example folder structure

```
annotations/
├── humans.json
├── pizzas.json
└── nsfw.json

ground_truths/
├── humans.json
├── pizzas.json
└── nsfw.json
```

Each JSON file is a list of dictionaries. Each dictionary represents one submission: a user's answers to all questions in a **question group**, for a specific video and project. We show an example below from [annotations/humans.json](example/annotations/humans.json):

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
    "is_grou nd_truth": false
  },
  {
    "question_group_title": "Human",
    "project_name": "Human Test 0",
    "user_name": "Robot 1",
    "video_uid": "human.mp4",
    "answers": {
      "Number of people?": "2",
      "If there are people, describe them.": "The two people are a man and a woman."
    },
    "confidence_scores": {
      "Number of people?": 0.3,
      "If there are people, describe them.": 0.2
    },
    "is_ground_truth": false
  }
]
```

**What is an (annotator) answer?**

Each answer is a complete set of responses from a user to all questions within a single question group, for a specific video and project.

- (**`question_group_title`**, **`project_name`**, **`user_name`**, **`video_uid`**) – uniquely defines an answer submission.
- **`answers`** – a dictionary of question names and their corresponding answers.
- **`confidence_scores`** (optional; only for model users) – a dictionary of question names and their corresponding confidence scores.
- **`is_ground_truth`** - must be `false` for annotator answers.


For ground truth answers submitted by reviewers, refer to the following example [ground_truths/humans.json](example/ground_truths/humans.json):


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
  }
]
```

**What is a (reviewer) ground truth?**

A ground-truth record is the **single final answer** chosen by a reviewer for one video, project, and question group. Its JSON looks just like an annotator answer, but it sets `"is_ground_truth": true`. There can be only **one** ground-truth entry for each *(video, project, question group)*.

- (**`question_group_title`**, **`project_name`**, **`video_uid`**) – uniquely defines a ground truth submission.
- **`answers`** – a dictionary of question names and their corresponding answers.
- **`is_ground_truth`** - must be `true` for ground truths.

## Getting your data into Label Pizza

Now that you understand the key components of Label Pizza (videos, users, question groups, schemas, projects, and project-user assignments), you can dive in right away with our provided demo (`example/`) or bring in your own data (`workspace/`):

| Goal                                           | Command                                                | What happens                                                 |
| ---------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| **Explore a working demo**                     | `python sync_from_folder.py --folder-path ./example`   | Loads the sample **example/** folder into a fresh database so you can click around immediately. |
| **Start from scratch or import your own data** | `python sync_from_folder.py --folder-path ./workspace` | The empty **workspace/** folder has the correct structure. Run the command as-is for a blank database, **or** fill the JSON files first to bulk-import all your videos, users, schemas, and projects in one shot. |


### Step-by-step guide


> **Before you start:** make sure the **`DBURL`** key in your [.env](.env) file contains the correct PostgreSQL connection string you got from Supabase (or any other Postgres host).

1. **Initialize the database once and seed the first admin account**

   ```bash
   python label_pizza/init_or_reset_db.py \
       --mode init \
       --database-url-name DBURL \
       --email admin1@example.com \
       --password admin111 \
       --user-id "Admin 1"
   ```

2. **Sync the folder**

   ```bash
   # Our provided demo
   python sync_from_folder.py --folder-path ./example
   
   # Blank or custom project
   python sync_from_folder.py --folder-path ./workspace
   ```

3. **Launch the web UI**

   ```bash
   streamlit run label_pizza/label_pizza_app.py \
       --server.port 8000 --server.address 0.0.0.0 \
       -- --database-url-name DBURL
   ```

   Open **[http://localhost:8000](http://localhost:8000)** and log in with the admin credentials from step 1.

4. **Explore the interface**

   * **Admin portal** – view all videos, users, question groups, schemas, and projects.
   * **Annotation portal** – try answering a few demo tasks.
   * **Reviewer portal** – try setting a few ground truths by looking at the annotator answers.
   * **Meta-reviewer portal** – override reviewer decisions for final ground truth.
   * **Search portal** – look up any video by UID, answer pattern, or completion status.

   You can walk through the full workflow: coaching annotators, collecting ground truth, auditing reviews, and inspecting accuracy analytics.

### Reset or backup the database

Want to save your current work or start over from a blank database? Use the commands below—always back up first so you can restore any time.

| Action                                                       | One-liner to run                                             | Result                                                       |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Create a backup**                                          | `python label_pizza/init_or_reset_db.py --mode backup --backup-file my_sql.sql.gz` | Saves `./backups/my_sql.sql.gz` with every table.            |
| **Reset the database**<br>(nuclear option, makes its own backup) | `python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode reset --auto-backup --backup-file my_sql.sql.gz --email admin1@example.com --password admin111 --user-id "Admin 1"` | Backs up first, then drops every table and recreates them from scratch (all tables start empty). |
| **Restore from backup**                                      | `python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode restore --backup-file my_sql.sql.gz --email admin1@example.com --password admin111 --user-id "Admin 1"` | Loads `my_sql.sql.gz` into a freshly reset database, repopulating all tables with the saved data. |

### Syncing the database

Throughout your work, you can always synchronize the database—adding new items, updating existing ones, or archiving anything you no longer need—using one of the three methods below:

| Method                                                       | When to use it                                               | Typical examples                                             | Pros                                                         | Cons                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Web UI**                                                   | Quick, single-item edits                                     | Add one user and set a password • Assign that user to a project • Change the URL or metadata of one video | Instant; no code                                             | Tedious for hundreds of items                                |
| **`sync_from_folder.py`** (whole-folder sync)                | First import of the demo or your own workspace • One-time migration of a full dataset | Point the script at **workspace/** (or your custom folder) after you’ve prepared JSON for every table | One command; the JSON in **workspace/** is the canonical source of truth you can commit to Git | Scans every file (slow on very large folders) • Can’t handle some cross-table actions such as archiving a schema *and* its projects in the same run (explained later) |
| **Helpers in `sync_utils.py`** (`sync_videos`, `sync_question_groups`, …) | Day-to-day batch jobs or surgical edits                      | Add or update hundreds of videos/question groups/users/schemas/projects/assignments in one call • Import model answers and their confidence scores | Fast; touches only the table you call • Lets you script complex sequences (e.g., archive projects, then archive their now-unused schema) | You must run helpers in dependency order and manage the JSON yourself |

**Rule of thumb**

* **Web UI** → most day-to-day quick operations (e.g., one user, one video, one project assignment).
* **`sync_from_folder.py`** → load the demo, start a fresh workspace, or migrate an entire dataset the first time.
* **`sync_utils.py` helpers** → large or repeatable work once you’re up and running.


## Customizing your sync workflow with `sync_utils.py`

Once your database is live you will likely need to add or update hundreds of videos, projects, question groups, and model answers. The simplest route is to modify the JSON  inside **[`workspace/`](workspace/)** and rerun **[`sync_from_folder.py`](sync_from_folder.py)**, but be aware of how that script works:

* Reads **every** JSON file in the folder – videos, question groups, schemas, users, projects, assignments, annotations, and ground-truths.
* Compares each record to what's already in the database.
* Flags *any* difference as an update.

On a large workspace that full-folder scan can be slow.

Because **[`sync_from_folder.py`](sync_from_folder.py)** is just a thin wrapper around the helper functions in **[`label_pizza/sync_utils.py`](label_pizza/sync_utils.py)**, you can call those helpers directly and update **just** the data you need. 

### Overview of the helpers

| Helper                   | What it syncs                                           | Common tasks                                                 |
| ------------------------ | ------------------------------------------------------- | ------------------------------------------------------------ |
| `sync_videos`            | Videos                                                  | Add new clips, update video URLs or metadata, archive videos |
| `sync_question_groups`   | Question Groups **plus** their Questions                | Create or update question groups, add verification rules, archive question groups |
| `sync_schemas`           | Schemas                                                 | Add or update schemas, update instruction urls, toggle `has_custom_display` for per-video questions/options, archive schemas |
| `sync_users`             | Users                                                   | Bulk-create or archive users, reset passwords                |
| `sync_projects`          | Projects (and per-video custom question text / options) | Add new projects, add or update per-video custom question text / options, archive projects |
| `sync_users_to_projects` | User ↔ Project assignments                              | Grant or revoke project assignments, update user roles in a project, adjust user weights |
| `sync_project_groups`    | Project Groups                                          | Organize projects into groups for easier management, archive project groups |
| `sync_annotations`       | Annotator answers                                       | Import existing human or model predictions                   |
| `sync_ground_truths`     | Reviewer ground-truth answers                           | Import ground-truth answers so new annotators can start a project in Training mode with immediate feedback |

### How the helpers take input

By default the examples in this guide pass a **Python list of dictionaries**:

```python
sync_videos(videos_data=[{...}, {...}])
```

Some of the helpers can also take:

* **A path to a single `.json` file**
  `sync_videos(videos_path="workspace/videos.json")`
* **A folder of many `.json` files** (for `sync_annotations` and `sync_ground_truths`)
  `sync_annotations(annotations_folder="workspace/annotations")`

For this guide, we'll use the Python list of dictionaries.

### Tip — what should I keep in the `workspace/` folder?

You don’t need to keep everything in `workspace/` — just the parts that are helpful to version-control or sync in bulk. Our recommendation is to keep the following:

<!-- * **✅ Videos & Projects**
  These often involve hundreds or thousands of entries. It’s much easier to manage them in JSON and upload via helper scripts than to enter them manually.

* **✅ Question Groups & Schemas**
  While you *can* create or edit these in the browser, it’s a good idea to keep them in JSON too — so you always have a version-controlled record of what’s been used, and you can easily apply the same schemas on another database.

* **🟡 Users & Assignments**
  For small teams, the Admin UI is the simplest way to manage users and assign them to projects. Use the helpers only if you're onboarding dozens of users at once.

* **🚫 Annotations & Ground-Truths**
  These should be collected directly in the web UI. Only use the helpers if you're importing existing labels (e.g., from a model or legacy dataset). -->

| Keep in JSON?                     | Why                                                          |
| --------------------------------- | ------------------------------------------------------------ |
| **Videos & Projects ✅**           | usually hundreds—JSON + helper is far faster than hand entry |
| **Question Groups & Schemas ✅**   | version-control your labeling policy and reuse it elsewhere  |
| **Users & Assignments 🟡**         | UI is fine for small teams; use helpers only for large batches |
| **Annotations & Ground-Truths 🚫** | collect via Web UI; import here only for existing labels (e.g., legacy dataset or from a model) |


Keeping the important JSON files in `workspace/` lets you:

* back them up, version-control them, and reuse them across machines or databases
* feed them directly into helper functions without rewriting scripts or paths


### Tip — order matters

```
Add / update :  videos → question groups → schemas → users → projects → assignments → annotations / ground_truths
Archive      :  annotations / ground_truths → assignments → projects → schemas → question groups → videos
```

Upload things **before** anything that depends on them, and archive in the reverse direction to avoid dependency errors.


#### Prerequisite: `init_database`

**Important:** You must run this once before **importing** any other helper functions, otherwise you will get an error.

```python
from label_pizza.sync_utils import init_database
init_database("DBURL")  # or change DBURL to another key in .env
```


#### 1. `sync_videos`

Function for adding / updating / archiving videos

1.1 - Add videos

To add a video, you must provide a `video_uid` that does not already exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_videos
videos_data = [
  {
    "video_uid": "human.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human.mp4",
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=L3wKzyIN1yk",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "pizza.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/pizza.mp4",
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=8J1NzjA9jNg",
      "description": "A video of a pizza being made.",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "human2.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human2.mp4",
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=2vjPBrBU-TM",
      "license": "Standard YouTube License"
    }
  },
  {
    "video_uid": "human3.mp4",
    "url": "https://huggingface.co/datasets/syCen/example4labelpizza/resolve/main/human3.mp4",
    "is_active": True,
    "metadata": {
      "original_url": "https://www.youtube.com/watch?v=2S24-y0Ij3Y",
      "license": "Standard YouTube License"
    }
  }
]

sync_videos(videos_data=videos_data)
```

1.2 - Update or archive videos

To update a video record, keep the `video_uid` fixed and modify other fields such as `url`, `is_active`, or `metadata`.

```python
# ...After step 1.1

videos_data = [
  {
    "video_uid": "human.mp4",                       # Must already exist in the database
    "url": "https://your-repo-new/human.mp4",       # update url (Must not exist in the database)
    "is_active": False,                             # Set to False to archive the video
    "metadata": {
       # update metadata to be an empty dictionary
    }
  }
]

sync_videos(videos_data=videos_data)
```

### 2. `sync_users`

Function for adding / updating / archiving users

2.1 - Add users

To add a user, provide a unique `user_id` that does not already exist in the database. If specifying an `email`, it must also be unique.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_users

users_data = [
    {
        "user_id": "User 1",          # Must NOT exist in the database
        "email": "user1@example.com", # Must NOT exist in the database
        "password": "user111",
        "user_type": "human",
        "is_active": True
    },
    {
        "user_id": "Admin 1",          # Must NOT exist in the database
        "email": "admin1@example.com", # Must NOT exist in the database
        "password": "admin111",
        "user_type": "admin",
        "is_active": True
    }
]

sync_users(users_data=users_data)
```

2.2 - Update or archive users

To update a user, provide the `user_id` that already exists in the database to update the `email`, `password`, `user_type`, or `is_active`.

```python
# ...After step 2.1

# Update email using user_id
users_data = [
    {
        "user_id": "User 1",              # the existing user_id
        "email": "user1-new@example.com", # new email
        "password": "user111-new",        # new password
        "user_type": "human",             # Could only select from "admin", "human"
        "is_active": True
    }
]

sync_users(users_data=users_data)

# Then update user_id using email + archive the user
users_data = [
    {
        "user_id": "User 1 New",          # new user_id
        "email": "user1-new@example.com", # the existing email
        "password": "user111-new",        
        "user_type": "human",             # Could only select from "admin", "human"
        "is_active": False
    }
]

sync_users(users_data=users_data)

# You can add a model user. Model users do not have `email`
users_data = [
    {
        "user_id": "Model 1",          # new user_id
        "email": "",                   # Model user could not have email
        "password": "",                # Model user could not have password
        "user_type": "model",
        "is_active": True
    }
]

sync_users(users_data=users_data)
```

> **Note:** You cannot change a human/admin user to a model user because model users do not have `email` and might have `confidence_score` in the `annotations` table.

### 3. Sync Question Groups

> Before creating question groups, it's important to understand the role of the (optional) `verification_function`. This function ensures that the answers within a group are logically consistent.
>
> **For example:** Annotators must describe a person if there are more than 0 people in the video; otherwise, the description should be left blank. The following `check_human_description` function enforces this rule.

```python
def check_human_description(answers: Dict[str, str]) -> None:
    num_people = answers.get("Number of people?")
    description = answers.get("If there are people, describe them.")
    
    if num_people == "0" and description:
        raise ValueError("Description cannot be provided when there are no people")
    if num_people != "0" and not description:
        raise ValueError("Description must be provided when there are people")
```

If you do not wish to use a verification function, you can set `verification_function` to `None` (or `null` in JSON).

3.1 - Add Question Groups

To add a question group, provide a unique `title` that does not already exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_question_groups
question_groups_data = [
    {
        "title": "Human",           # Must NOT exist in the database
        "display_title": "Human",
        "description": "Detect and describe all humans in the video.",
        "is_reusable": False,
        "is_auto_submit": False,
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
                "display_text": "If there are people, describe them.",
                "default_option": None
            }
        ]
    }
]

sync_question_groups(question_groups_data=question_groups_data)
```

3.2 - Update question groups

To update a question group, provide the `title` that already exists in the database. You can update the following fields: `display_title`, `description`, `is_reusable`, `is_auto_submit`, `verification_function`, and `questions`. **Make sure** the `questions` list contains exactly the same entries as the existing version—no additions or removals are allowed.

Within the `questions` list, you may change the order of the questions. For each question, use the existing `text` to update its corresponding `display_text`, `display_values`, and `default_option`.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_question_groups

question_groups_data = [
    {
        "title": "Human",                  # Must exist in the database
        "display_title": "Human Updated",  # update display_title
        "description": "Detect and describe all humans in the video. (Updated)", # update description here
        "is_reusable": True,               # update is_reusable
        "is_auto_submit": True,            # update is_auto_submit
        "verification_function": None,     # remove verification_function
        "questions": [                     # update the question order to move description to the first question
            {
                "qtype": "description",
                "text": "If there are people, describe them.",
                "display_text": "If there are people, describe them.",
                "default_option": "This answer will be pre-filled for all annotators."
            },
            {
                "qtype": "single",
                "text": "Number of people?",
                "display_text": "Number of people updated?",     # Update the display_text of a question
                "options": [ # update the order of options
                    "3 or more",
                    "2",
                    "1",
                    "0",
                ],
                "display_values": [ # be sure to update the display_values to match the new order of options
                    "3+ updated",   # Update the display_values of a question
                    "2 updated",
                    "1 updated",
                    "0 updated",
                ],
                "option_weights": [
                    1.0,
                    1.0,
                    1.0,
                    1.0
                ],
                "default_option": "3 or more" # update the default option
            }
        ]
    },
    {
        "title": "NSFW",
        "display_title": "NSFW",
        "description": "Check whether the video is not safe for work.",
        "is_reusable": True,
        "is_auto_submit": True,
        "verification_function": None,
        "questions": [
            {
                "text": "Is the video not safe for work?",
                "qtype": "single",
                "options": [
                    "Yes",
                    "No"
                ],
                "display_values": [
                    "Yes",
                    "No"
                ],
                "option_weights": [
                    1.0,
                    99.0
                ],
                "default_option": "No",
                "display_text": "Is the video not safe for work?"
            }
        ]
    }
]

sync_question_groups(question_groups_data=question_groups_data)
```

> **Note:** Updating a question in one question group will also update it in all other groups that include the same question.

### 4. Sync Schemas

Function for adding / updating / archiving schemas

4.1 - Add schemas

To add a schema, provide a unique `schema_name` that does not already exist in the database. Make sure all the question groups in `question_group_names` should exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_schemas

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

4.2 - Update schemas

To update a schema, provide the `schema_name` that already exists in the database. You can update the `instructions_url`, `has_custom_display`, `is_active`, and `question_group_names`. Note that you may only change the order of the `question_group_names`; the list itself must match the existing version exactly—no additions or removals are allowed.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_schemas

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

4.3 - Archive schemas

> If you want to archive any schema, you should ensure that all the projects that use the schema have been archived. You could see Step4 and Step5 to see how to archive schemas and projects separately.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_schemas

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

### 5. Sync Projects

5.1 - Add projects

To add a schema, provide a unique `project_name` that does not already exist in the database. Make sure both the `videos` and `schema_name` should exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_projects

projects_data = [
  {
    "project_name": "Human Test 0",            # Must Not exist in the database
    "schema_name": "Questions about Humans",   # Must exist in the database
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

5.2 - Update projects

To update a project, provide the `project_name` that already exists in the database. You can update the `description`.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_projects

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

5.3 - Archive projects

Set `is_active` to `False` to archive the project.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_projects

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

### 6. Sync Project Groups

6.1 - Adding project group

To add a schema, provide a unique `project_group_name` that does not already exist in the database. Make sure all the projects in  `projects` should exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_projects, sync_project_groups

"""
Before creating / editing project groups, we need to make sure that all the projects we used are already in the database.
"""

"""
Adding projects first
"""
projects_data = [
  {
    "project_name": "Human Test 0",
    "schema_name": "Questions about Humans",
    "description": "Test project for humans",
    "is_active": True,
    "videos": [
      "human.mp4",
      "pizza.mp4"
    ]
  },
  {
    "project_name": "Human Test 1",
    "schema_name": "Questions about Humans",
    "description": "Test project for humans",
    "is_active": True,
    "videos": [
      "human2.mp4",
      "human3.mp4"
    ]
  }
]

sync_projects(projects_data=projects_data)

"""
Adding project groups
"""
project_groups_data = [
    {
        "project_group_name": "Example Project Group",                # Must NOT exist in the database
        "description": "This is a project group for human test",
        "projects": [             # All these projects MUST exist in the database
            "Human Test 0",
            "Human Test 1"
        ]
    }
]

sync_project_groups(project_groups_data=project_groups_data)
```

> If you find any videos that not exist in the database, please add them to the database according to Step 1.

6.2 - Update project groups

To update a project group, provide the `project_group_name` that already exists in the database. You can update the `description` and modify the `projects` list to add or remove projects.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_project_groups

"""
Assuming we already have Human Test 2 in our database.
"""
project_groups_data = [
  {
      "project_group_name": "Example Project Group",                         # Must exist in the database
      "description": "This is a project group for human test updated",       # Update the description
      "projects": [            # Adding / deleting existing project names to update the project groups
          "Human Test 0",
          "Human Test 2"
      ]
  }
]
```

### 7. Sync Users to Projects

7.1 - Add user to project

To add a user to project, provide a unique (`user_name, project_name`) that does not already exist in the database. Make sure both the `user_name` and `project_name` should exist in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_users_to_projects

assignments_data = [
  {
    "user_name": "User 1",             # Must exists in the database
    "project_name": "Human Test 0",    # Must exists in the database
    "role": "annotator",               # You could only select from ["annotator", "reviewer", "admin", "model"]
    "user_weight": 1.0,              
    "is_active": True
  }
]

sync_users_to_projects(assignments_data=assignments_data)
```

> Notice that you cannot assign an "Admin" user to a non-"Admin" role.

7.2 - Update user in project

To update a user's role in a project, provide the (`user_name`, `project_name`) pair that already exists in the database. You can then update the user's `role`.

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_users_to_projects

assignments_data = [
  {
    "user_name": "User 1",             # Must exists in the database
    "project_name": "Human Test 0",    # Must exists in the database
    "role": "reviewer",                # Update it to reviewer
    "user_weight": 1.0,              
    "is_active": True
  }
]

sync_users_to_projects(assignments_data=assignments_data)
```

7.3 - Remove user from project

Set `is_active` to `False` to remove a user from a project. Make sure the (`user_name`, `project_name`) pair already exists in the database.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_users_to_projects

assignments_data = [
  {
    "user_name": "User 1",
    "project_name": "Human Test 0",
    "role": "annotator",
    "user_weight": 1.0,
    "is_active": False                 # Set False to remove user from this project
  }
]

sync_users_to_projects(assignments_data=assignments_data)
```

### 8. Sync Annotations and Reviews

### `annotations/` and `ground_truths/`

Both directories share the same JSON structure: each file contains answers for a single question group across all projects and videos. Use `annotations/` for annotator answers and `ground_truths/` for reviewer ground truth (there can be only one ground‑truth answer per video‑question‑group pair).

8.1 - Sync annotations

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_annotations

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
    "is_ground_truth": False            # For annotations, is_ground_truth MUST be False
  }
]

sync_annotations(annotations_data=annotations_data)
```

8.2 - Sync ground truths

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_ground_truths

ground_truths_data = [
  {
    "question_group_title": "Human",    # Must exist in the database
    "project_name": "Human Test 0",     # Must exist in the database
    "user_name": "Admin 1",              # User must have at least "annotation" privileges
    "video_uid": "human.mp4",           # Video must exist in the project
    "answers": {        # Answers must include all and only the questions defined in the question group
      "Number of people?": "1",
      "If there are people, describe them.": "The person appears to be a large man with a full beard and closely cropped hair."
    },
    "is_ground_truth": True             # Must be True
  }
]

sync_ground_truths(ground_truths_data=ground_truths_data)
```

# Custom Question Per Video

### Set Custom Display text for video in any project

### 1. Sync Question Group and Schema

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_question_groups

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
        "display_text": "Describe one aspect of the video",
        "default_option": null
      }
    ]
  }
]

sync_question_groups(question_groups_data=question_groups_data)

from label_pizza.sync_utils import sync_schemas

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

from label_pizza.sync_utils import sync_projects

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

To set custom displays for any project, make sure:

- Schema with `schema_name` must have `has_custom_display` == `True`.

- `question_text` must exists in the database.
- `custom_option` must matches the number and the order of the `options` of this question. 

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.sync_utils import sync_projects

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
```xxxxxxxxxx 📊 Summary:   • Created: 4   • Updated: 2   • Removed: 1   • Skipped: 3   • Total processed: 10
```



## Force Override Functions

### Before Using This

All functions in this module provide automatic backup functionality. You can configure the backup settings at the beginning of the file:

```
# Backup configuration
BACKUP_DIR = "./db_backups"
MAX_BACKUPS = 10
```

**Important:** All destructive operations automatically create timestamped backups before execution. The backup system requires `backup_restore.py` to be available.

#### Setup

```python
from label_pizza.db import init_database
init_database()  # Uses DBURL environment variable
from label_pizza.models import *  # Import all models
```

### Functions

#### 1. change_question_text(original_text, new_text)

Updates `text` in `Question` table.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import change_question_text

change_question_text(
  original_text="original_text",
  new_text="new_text"
)
```

> **Important:** This will update the question across all question groups and projects that use it.

**Database Operations:**

- **Table:** `Question`
- **Updates:** `text` and `display_text` fields
- **Query:** `UPDATE questions SET text = ?, display_text = ? WHERE text = ?`
- **Validation:** Checks if question exists and new text doesn't conflict

#### 2. update_question_group_titles(group_id, new_title, new_display_title=None)

Update the `title` and `display_title` in `QuestionGroup` table.

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import update_question_group_titles

update_question_group_titles(
    group_id=5,
    new_title="Human Detection V2",
    new_display_title="Human Detection (Updated)"
)
```

**Database Operations:**

- **Table:** `QuestionGroup`
- **Updates:** `title` and `display_title` fields
- **Query:** `UPDATE question_groups SET title = ?, display_title = ? WHERE id = ?`
- **Validation:** Checks if group exists and new title doesn't conflict with existing groups

#### 3. change_project_schema_simple(project_id, new_schema_id)

```
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.force_override import change_project_schema_simple

result = change_project_schema_simple(
    project_id=3,
    new_schema_id=7
)
```

> Changes a project's schema and automatically cleans up incompatible data.

**Database Operations:**

1. **Finds removed questions** by comparing old vs new schema
2. Deletes ALL custom displays:
   - `DELETE FROM project_video_question_displays WHERE project_id = ?`
3. Deletes incompatible answers:
   - `DELETE FROM annotator_answers WHERE project_id = ? AND question_id IN (?)`
4. Deletes incompatible ground truth:
   - `DELETE FROM reviewer_ground_truth WHERE project_id = ? AND question_id IN (?)`
5. Updates project schema:
   - `UPDATE projects SET schema_id = ? WHERE id = ?`
6. Resets user completion status: 
   - `UPDATE project_user_roles SET completed_at = NULL WHERE project_id = ?`

## ⚠️ Force Delete Functions

### 1. Delete from Table `User`

#### `delete_user_by_id`

```
from label_pizza.force_override import delete_user_by_id

user_id = 3
delete_user_by_id(user_id=user_id)
```

#### `delete_user_by_user_id_str`

```
from label_pizza.force_override import delete_user_by_user_id_str

user_id_str = 'User 1'
delete_user_by_user_id_str(user_id=user_id_str)
```

### 2. Delete from Table `Video`

#### `delete_video_by_id`

```
from label_pizza.force_override import delete_video_by_id

video_id = 1
delete_video_by_id(video_id=video_id)
```

#### `delete_video_by_uid`

```
from label_pizza.force_override import delete_video_by_uid

video_uid = 'pizza.mp4'
delete_video_by_id(video_uid=video_uid)
```

### 3. Delete from Table `VideoTag`

#### `delete_video_tag_by_video_id`

```
from label_pizza.force_override import delete_video_tag_by_video_id

video_id = 1
delete_video_tag_by_video_id(video_id=video_id)
```

### 4. Delete from Table `QuestionGroup`

#### `delete_question_group_by_id`

```
from label_pizza.force_override import delete_question_group_by_id

question_group_id = 1
delete_question_group_by_id(question_group_id=question_group_id)
```

#### `delete_question_group_by_title`

```
from label_pizza.force_override import delete_question_group_by_title

title = 'Pizza'
delete_question_group_by_title(title=title)
```

### 5. Delete from table `Question`

#### `delete_question_by_id`

```
from label_pizza.force_override import delete_question_by_id

question_id = 1
delete_question_by_id(question_id=question_id)
```

#### `delete_question_by_text`

```
from label_pizza.force_override import delete_question_by_text

text = 'Number of pizzas?'
delete_question_by_text(text=text)
```

**`delete_question_group_question_by_question_id`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_question_id

question_id = 1
delete_question_group_questions_by_question_id(question_id=question_id)
```

**`delete_question_group_question_by_group_id`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_group_id

question_group_id = 1
delete_question_group_questions_by_group_id(question_group_id=question_group_id)
```

**`delete_question_group_question_by_both_ids`**

python

```python
from label_pizza.force_override import delete_question_group_questions_by_both_ids

question_group_id = 1
question_id = 5
delete_question_group_questions_by_both_ids(question_group_id=question_group_id, question_id=question_id)
```

### 6. Delete from Table `Schema`

#### **`delete_schema_by_id`**

```python
from label_pizza.force_override import delete_schema_by_id

schema_id = 1
delete_schema_by_id(schema_id=schema_id)
```

**`delete_schema_by_name`**

python

```python
from label_pizza.force_override import delete_schema_by_name

name = 'Default Schema'
delete_schema_by_name(name=name)
```

### 7. Delete from Table `SchemaQuestionGroup`

#### **`delete_schema_question_group_by_schema_id`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_schema_id

schema_id = 1
delete_schema_question_groups_by_schema_id(schema_id=schema_id)
```

#### **`delete_schema_question_group_by_question_group_id`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_question_group_id

question_group_id = 1
delete_schema_question_groups_by_question_group_id(question_group_id=question_group_id)
```

#### **`delete_schema_question_group_by_both_ids`**

```python
from label_pizza.force_override import delete_schema_question_groups_by_both_ids

schema_id = 1
question_group_id = 5
delete_schema_question_groups_by_both_ids(schema_id=schema_id, question_group_id=question_group_id)
```

### 8. Delete from Table `Project`

#### **`delete_project_by_id`**

```python
from label_pizza.force_override import delete_project_by_id

project_id = 1
delete_project_by_id(project_id=project_id)
```

#### **`delete_project_by_name`**

```python
from label_pizza.force_override import delete_project_by_name

name = 'My Project'
delete_project_by_name(name=name)
```

### 9. Delete from Table `ProjectVideo`

#### **`delete_project_video_by_project_id`**

```python
from label_pizza.force_override import delete_project_video_by_project_id

project_id = 1
delete_project_video_by_project_id(project_id=project_id)
```

#### **`delete_project_video_by_video_id`**

```python
from label_pizza.force_override import delete_project_video_by_video_id

video_id = 1
delete_project_video_by_video_id(video_id=video_id)
```

#### **`delete_project_video_by_both_ids`**

```python
from label_pizza.force_override import delete_project_video_by_both_ids

project_id = 1
video_id = 5
delete_project_video_by_both_ids(project_id=project_id, video_id=video_id)
```

### 10. Delete from Table `ProjectUserRole`

#### **`delete_project_user_role_by_project_id`**

```python
from label_pizza.force_override import delete_project_user_role_by_project_id

project_id = 1
delete_project_user_role_by_project_id(project_id=project_id)
```

#### **`delete_project_user_role_by_user_id`**

```python
from label_pizza.force_override import delete_project_user_role_by_user_id

user_id = 1
delete_project_user_role_by_user_id(user_id=user_id)
```

#### **`delete_project_user_role_by_both_ids`**

```python
from label_pizza.force_override import delete_project_user_role_by_both_ids

project_id = 1
user_id = 5
delete_project_user_role_by_both_ids(project_id=project_id, user_id=user_id)
```

#### 11. Delete from Table `ProjectGroup`

#### **`delete_project_group_by_id`**

```python
from label_pizza.force_override import delete_project_group_by_id

project_group_id = 1
delete_project_group_by_id(project_group_id=project_group_id)
```

#### **`delete_project_group_by_name`**

```python
from label_pizza.force_override import delete_project_group_by_name

name = 'Project Group 1'
delete_project_group_by_name(name=name)
```

### 12. Delete from Table `ProjectGroupProject`

#### **`delete_project_group_project_by_group_id`**

```python
from label_pizza.force_override import delete_project_group_project_by_group_id

project_group_id = 1
delete_project_group_project_by_group_id(project_group_id=project_group_id)
```

#### **`delete_project_group_project_by_project_id`**

```python
from label_pizza.force_override import delete_project_group_project_by_project_id

project_id = 1
delete_project_group_project_by_project_id(project_id=project_id)
```

#### **`delete_project_group_project_by_both_ids`**

```python
from label_pizza.force_override import delete_project_group_project_by_both_ids

project_group_id = 1
project_id = 5
delete_project_group_project_by_both_ids(project_group_id=project_group_id, project_id=project_id)
```

### 13. Delete from Table `ProjectVideoQuestionDisplay`

#### **`delete_project_video_question_display_by_project_id`**

```python
from label_pizza.force_override import delete_project_video_question_display_by_project_id

project_id = 1
delete_project_video_question_display_by_project_id(project_id=project_id)
```

#### **`delete_project_video_question_displays_by_video_id`**

```python
from label_pizza.force_override import delete_project_video_question_displays_by_video_id

video_id = 1
delete_project_video_question_displays_by_video_id(video_id=video_id)
```

#### **`delete_project_video_question_displays_by_question_id`**

```python
from label_pizza.force_override import delete_project_video_question_displays_by_question_id

question_id = 1
delete_project_video_question_displays_by_question_id(question_id=question_id)
```

#### **`delete_project_video_question_display_by_ids`**

```python
from label_pizza.force_override import delete_project_video_question_display_by_ids

project_id = 1
video_id = 5
question_id = 10
delete_project_video_question_display_by_ids(project_id=project_id, video_id=video_id, question_id=question_id)
```

### 14. Delete from Table `AnnotatorAnswer`

#### **`delete_annotator_answer_by_project_id`**

```python
from label_pizza.force_override import delete_annotator_answer_by_project_id

project_id = 1
delete_annotator_answer_by_project_id(project_id=project_id)
```

#### **`delete_annotator_answers_by_video_id`**

```python
from label_pizza.force_override import delete_annotator_answers_by_video_id

video_id = 1
delete_annotator_answers_by_video_id(video_id=video_id)
```

#### **`delete_annotator_answers_by_user_id`**

```python
from label_pizza.force_override import delete_annotator_answers_by_user_id

user_id = 1
delete_annotator_answers_by_user_id(user_id=user_id)
```

### 15. Delete from Table `ReviewerGroundTruth`

#### **`delete_reviewer_ground_truth_by_project_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_project_id

project_id = 1
delete_reviewer_ground_truth_by_project_id(project_id=project_id)
```

#### **`delete_reviewer_ground_truth_by_video_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_video_id

video_id = 1
delete_reviewer_ground_truth_by_video_id(video_id=video_id)
```

#### **`delete_reviewer_ground_truth_by_reviewer_id`**

```python
from label_pizza.force_override import delete_reviewer_ground_truth_by_reviewer_id

reviewer_id = 1
delete_reviewer_ground_truth_by_reviewer_id(reviewer_id=reviewer_id)
```