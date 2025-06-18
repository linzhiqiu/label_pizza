import json
from sqlalchemy.orm import Session
from tqdm import tqdm
from label_pizza.services import (
    VideoService, 
    ProjectService, 
    SchemaService, 
    QuestionGroupService, 
    QuestionService,
    AuthService
)
from label_pizza.db import SessionLocal, engine
import hashlib


def add_videos(videos_data: list[dict]) -> None:
    """
    Add new videos from an in-memory list of dicts.  
    Raises ValueError if any video already exists or the metadata is invalid.

    Args:
        videos_data: A list of dictionaries, each with keys
                     "url" (str) and "metadata" (dict).
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    # Validate and add inside one DB session
    with Session(engine) as session:
        # 1️⃣ Pre-check for duplicates or other validation errors
        duplicate_urls = []
        for video in videos_data:
            try:
                VideoService.verify_add_video(
                    url=video["url"],
                    session=session,
                    metadata=video.get("metadata")
                )
            except ValueError as e:
                # Collect “already exists” errors, propagate the rest
                if "already exists" in str(e):
                    duplicate_urls.append(video["url"])
                else:
                    raise ValueError(
                        f"Validation failed for {video['url']}: {e}"
                    ) from None

        if duplicate_urls:
            raise ValueError(
                "Videos already exist: " + ", ".join(duplicate_urls)
            )

        # 2️⃣ No duplicates → add everything
        for video in tqdm(videos_data, desc="Adding videos", unit="video"):
            VideoService.add_video(
                url=video["url"],
                session=session,
                metadata=video.get("metadata")
            )
            print(f"✓ Added new video: {video['url']}")

        # 3️⃣ Commit once at the end
        try:
            session.commit()
            print("✔ All videos processed and committed!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None

def update_videos(videos_data: list[dict]) -> None:
    """
    Update existing videos given an in-memory list of dicts.

    Args:
        videos_data: A list of dictionaries, each containing
                     "video_uid" (str), "url" (str), and "metadata" (dict).
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    with Session(engine) as session:
        # 1️⃣ Pre-check that every target video exists & the update is valid
        missing_uids = []
        for video in videos_data:
            try:
                VideoService.verify_update_video(
                    video_uid=video["video_uid"],
                    new_url=video["url"],
                    new_metadata=video.get("metadata"),
                    session=session
                )
            except ValueError as e:
                if "not found" in str(e):
                    missing_uids.append(video["video_uid"])
                else:
                    raise ValueError(
                        f"Validation failed for {video['video_uid']}: {e}"
                    ) from None

        if missing_uids:
            raise ValueError(
                "Videos do not exist: " + ", ".join(missing_uids)
            )

        # 2️⃣ All good → perform the updates
        for video in tqdm(videos_data, desc="Updating videos", unit="video"):
            VideoService.update_video(
                video_uid=video["video_uid"],
                new_url=video["url"],
                new_metadata=video.get("metadata"),
                session=session
            )
            print(f"✓ Updated video: {video['video_uid']}")

        # 3️⃣ Commit once at the end
        try:
            session.commit()
            print("✔ All videos processed and committed!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None

def import_question_group(group_data: dict) -> int:
    """
    Atomically import (or update) a Question-Group definition.

    Parameters
    ----------
    group_data : dict
        A dictionary with the structure below.

        ──  Top-level keys  ───────────────────────────────────────────────
        title                : str        # unique name of the group
        description          : str        # human-readable description
        is_reusable          : bool       # can be attached to multiple projects?
        is_auto_submit       : bool       # UI may auto-submit when all required answered
        verification_function: str | ""   # (optional) custom server-side checker
        questions            : list[dict] # list of question definitions (see next)

        ──  Each item in `questions`  ─────────────────────────────────────
        text            : str                    # immutable, unique identifier
        qtype           : "single" | "description" | "text"
        required        : bool                   # must annotator answer?
        options         : list[str] | None       # only for qtype == "single"
        display_values  : list[str] | None       # parallel to options (UI labels)
        default_option  : str | None             # must be in options
        display_text    : str | None             # wording shown above control
        option_weights  : list[float] | None     # numeric weight per option

        • For qtype == "single": `options`, `display_values` (same length),
          and, optionally, `option_weights` (same length) are **required**.
        • For qtype == "description` or `"text"`: all list-based fields
          *must* be None.

    Returns
    -------
    int
        ID of the created (or updated) question group.

    Raises
    ------
    ValueError
        If any verification step fails (duplicate title, bad options, etc.).
    Exception
        For unexpected DB errors (I/O, integrity, etc.).

    Notes
    -----
    The helper runs in two passes:
      1. Verification pass (read-only) — nothing is written unless every
         question and the group itself validate.
      2. Apply pass — create or update questions, then create the group,
         all inside a single transaction. Any failure rolls back everything.
    """
    if not isinstance(group_data, dict):
        raise TypeError("group_data must be a dictionary")

    with SessionLocal() as session:
        try:
            # ──────────────  Phase 1: VERIFY questions only  ──────────────
            for q in group_data["questions"]:
                try:
                    existing = QuestionService.get_question_by_text(q["text"], session)
                    # Validate an edit
                    QuestionService.verify_edit_question(
                        question_id=existing["id"],
                        new_display_text=q.get("display_text"),
                        new_opts=q.get("options"),
                        new_default=q.get("default_option"),
                        new_display_values=q.get("display_values"),
                        new_option_weights=q.get("option_weights"),
                        session=session,
                    )
                except ValueError as e:
                    if "not found" in str(e):  # ⇒ will be a new question
                        QuestionService.verify_add_question(
                            text=q["text"],
                            qtype=q["qtype"],
                            options=q.get("options"),
                            default=q.get("default_option"),
                            display_values=q.get("display_values"),
                            display_text=q.get("display_text"),
                            option_weights=q.get("option_weights"),
                            session=session,
                        )
                    else:
                        raise  # any other validation error

            # ──────────────  Phase 2: APPLY changes  ──────────────
            question_ids = []
            for q in tqdm(group_data["questions"], desc="Applying changes"):
                try:
                    existing = QuestionService.get_question_by_text(q["text"], session)
                    # Edit
                    QuestionService.edit_question(
                        question_id=existing["id"],
                        new_display_text=q.get("display_text"),
                        new_opts=q.get("options"),
                        new_default=q.get("default_option"),
                        new_display_values=q.get("display_values"),
                        new_option_weights=q.get("option_weights"),
                        session=session,
                    )
                    print(f"✓ Updated question: {q['text']}")
                    question_ids.append(existing["id"])
                except ValueError as e:
                    if "not found" in str(e):  # Add
                        new_q = QuestionService.add_question(
                            text=q["text"],
                            qtype=q["qtype"],
                            options=q.get("options"),
                            default=q.get("default_option"),
                            display_values=q.get("display_values"),
                            display_text=q.get("display_text"),
                            option_weights=q.get("option_weights"),
                            session=session,
                        )
                        print(f"✓ Created question: {q['text']}")
                        question_ids.append(new_q.id)
                    else:
                        raise

            # Now the group itself
            QuestionGroupService.verify_create_group(
                title=group_data["title"],
                description=group_data["description"],
                is_reusable=group_data["is_reusable"],
                question_ids=question_ids,
                verification_function=group_data.get("verification_function", ""),
                is_auto_submit=group_data.get("is_auto_submit", False),
                session=session,
            )

            qgroup = QuestionGroupService.create_group(
                title=group_data["title"],
                description=group_data["description"],
                is_reusable=group_data["is_reusable"],
                question_ids=question_ids,
                verification_function=group_data.get("verification_function", ""),
                is_auto_submit=group_data.get("is_auto_submit", False),
                session=session,
            )

            session.commit()
            print(f"✔ Successfully created question group: {group_data['title']}")
            return qgroup.id

        except Exception:
            session.rollback()
            raise

        
def update_questions(questions_data: list[dict]) -> None:
    """
    Bulk-update **existing** questions (free-text or single-choice).

    Parameters
    ----------
    questions_data : list[dict]
        A list where each element describes *one* question update:

        ── Required keys ───────────────────────────────────────────────
        text            : str                    # immutable identifier (must exist)
        display_text    : str | None             # new UI wording / prompt

        ── Only for single-choice questions ────────────────────────────
        options         : list[str]   | None     # full set of options (must include old ones)
        display_values  : list[str]   | None     # UI labels  (len == len(options))
        default_option  : str         | None     # pre-selected option (must be in options)
        option_weights  : list[float] | None     # numeric weights (len == len(options))

    Notes
    -----
    * The helper **does not** add new questions; every `text`
      must already exist in the DB.
    * Runs in two passes:
        1. Verify all updates (read-only).
        2. Apply edits in a single transaction.
      Any error aborts the whole batch.
    """
    if not isinstance(questions_data, list):
        raise TypeError("questions_data must be a list of dictionaries")

    with SessionLocal() as session:
        try:
            # ───────── Phase 1: VERIFY everything ─────────
            missing = []
            for q in questions_data:
                try:
                    existing = QuestionService.get_question_by_text(q["text"], session)
                    QuestionService.verify_edit_question(
                        question_id=existing["id"],
                        new_display_text=q.get("display_text"),
                        new_opts=q.get("options"),
                        new_default=q.get("default_option"),
                        new_display_values=q.get("display_values"),
                        new_option_weights=q.get("option_weights"),
                        session=session,
                    )
                except ValueError as e:
                    if "not found" in str(e):
                        missing.append(q["text"])
                    else:
                        raise ValueError(
                            f"Validation failed for '{q['text']}': {e}"
                        ) from None

            if missing:
                raise ValueError(f"Questions not found: {missing}")

            # ───────── Phase 2: APPLY edits ─────────
            for q in tqdm(questions_data, desc="Updating questions"):
                existing = QuestionService.get_question_by_text(q["text"], session)
                QuestionService.edit_question(
                    question_id=existing["id"],
                    new_display_text=q.get("display_text"),
                    new_opts=q.get("options"),
                    new_default=q.get("default_option"),
                    new_display_values=q.get("display_values"),
                    new_option_weights=q.get("option_weights"),
                    session=session,
                )
                print(f"✓ Updated question: {q['text']}")

            session.commit()
        except Exception:
            session.rollback()
            raise

def update_question_groups(groups_data: list[dict]) -> None:
    """
    Bulk-update **existing** question-groups.

    Parameters
    ----------
    groups_data : list[dict]
        A list where each element describes one group update:

        ── Required keys ───────────────────────────────────────────────
        title                : str   # current (immutable) name of the group
        description          : str   # new description shown in UI
        is_reusable          : bool  # update the “reusable” flag
        is_auto_submit       : bool  # update auto-submit behaviour

        ── Optional key ────────────────────────────────────────────────
        verification_function: str | ""   # new server-side validator (may be "")

        Example
        -------
        [
            {
                "title": "SubjectLight",
                "description": "This is the new description, hhh",
                "is_reusable": true,
                "verification_function": "",
                "is_auto_submit": false
            }
        ]

    Raises
    ------
    ValueError
        If any group doesn’t exist or a validation step fails.
    Exception
        For unexpected DB errors (integrity, I/O, etc.).

    Notes
    -----
    • This helper **does not** create new groups; each `title`
      must already exist in the DB.
    • Two-pass workflow:
        1. Verify every edit (read-only).
        2. Apply edits inside a single transaction.
    """
    if not isinstance(groups_data, list):
        raise TypeError("groups_data must be a list of dictionaries")

    with SessionLocal() as session:
        try:
            # ───────── Phase 1: VERIFY everything ─────────
            missing, validation_errors = [], []
            for g in groups_data:
                try:
                    grp = QuestionGroupService.get_group_by_name(g["title"], session)
                    QuestionGroupService.verify_edit_group(
                        group_id=grp.id,
                        new_title=g["title"],                     # title is immutable
                        new_description=g["description"],
                        is_reusable=g["is_reusable"],
                        verification_function=g.get("verification_function"),
                        is_auto_submit=g.get("is_auto_submit", False),
                        session=session,
                    )
                except ValueError as e:
                    if "not found" in str(e):
                        missing.append(g["title"])
                    else:
                        validation_errors.append(
                            f"Group '{g['title']}': {e}"
                        )

            if missing:
                raise ValueError(
                    "Question groups do not exist: " + ", ".join(missing)
                )
            if validation_errors:
                raise ValueError(
                    "Validation errors:\n" + "\n".join(validation_errors)
                )

            # ───────── Phase 2: APPLY updates ─────────
            for g in tqdm(groups_data, desc="Updating question groups"):
                grp = QuestionGroupService.get_group_by_name(g["title"], session)
                QuestionGroupService.edit_group(
                    group_id=grp.id,
                    new_title=g["title"],
                    new_description=g["description"],
                    is_reusable=g["is_reusable"],
                    verification_function=g.get("verification_function"),
                    is_auto_submit=g.get("is_auto_submit", False),
                    session=session,
                )
                print(f"✓ Updated question group: {g['title']}")

            session.commit()

        except Exception:   # catches ValueError and generic Exception
            session.rollback()
            raise


def create_schema(schema_data: dict) -> int:
    """
    Create a new Schema from existing Question-Groups.

    Parameters
    ----------
    schema_data : dict
        Required keys:

        ── Top-level ───────────────────────────────────────────────
        schema_name           : str        # name of the new schema
        question_group_names  : list[str]  # titles of the groups to include

        Example
        -------
        schema_data = {
            "schema_name": "My Schema",
            "question_group_names": ["Group 1", "Group 2", "Group 3"]
        }

    Returns
    -------
    int
        ID of the newly created schema.

    Raises
    ------
    ValueError
        • Any referenced question-group is missing  
        • Validation fails inside `SchemaService.verify_create_schema`
    Exception
        Unexpected database errors (integrity, I/O, etc.).
    """
    if not isinstance(schema_data, dict):
        raise TypeError("schema_data must be a dictionary")

    name   = schema_data.get("schema_name")
    groups = schema_data.get("question_group_names")

    if not name or not isinstance(groups, list) or not groups:
        raise ValueError(
            "schema_data must contain 'schema_name' (str) and "
            "'question_group_names' (non-empty list[str])"
        )

    with SessionLocal() as session:
        try:
            # ── Resolve group names → IDs ────────────────────────────
            qgroup_ids = []
            for gname in groups:
                grp = QuestionGroupService.get_group_by_name(gname, session)
                if not grp:
                    raise ValueError(f"Question group '{gname}' not found")
                qgroup_ids.append(grp.id)

            # ── Verify schema creation ───────────────────────────────
            SchemaService.verify_create_schema(name, qgroup_ids, session)

            # ── Create schema ────────────────────────────────────────
            schema = SchemaService.create_schema(
                name=name,
                question_group_ids=qgroup_ids,
                session=session,
            )
            session.commit()
            print(f"✓ Successfully created schema: {schema.name}")
            return schema.id

        except Exception:
            session.rollback()
            raise

def upload_users_from_json(json_path: str = None):
    """
    Batch upload users from a JSON file.

    Args:
        json_path (str): Path to the user JSON file.

    JSON format:
        [
            {
                "user_id": "alice",
                "email": "alice@example.com",
                "password": "alicepassword",
                "user_type": "human"
            },
            ...
        ]
    """
    import json

    with open(json_path, 'r') as f:
        users = json.load(f)

    with SessionLocal() as session:
        existing_users = AuthService.get_all_users(session)
        existing_emails = set(existing_users['Email'].tolist())
        existing_user_ids = set(existing_users['User ID'].tolist())

        for user in users:
            user_id = user['user_id']
            email = user['email']
            password = user['password']
            user_type = user.get('user_type', 'human')

            if email in existing_emails or user_id in existing_user_ids:
                print(f"User {email} or user_id {user_id} already exists, skipping.")
                continue

            # Hash the password (sha256)
            password_hash = hashlib.sha256(password.encode()).hexdigest()

            try:
                AuthService.create_user(
                    user_id=user_id,
                    email=email,
                    password_hash=password_hash,
                    user_type=user_type,
                    session=session
                )
                print(f"Successfully created user {email}")
            except Exception as e:
                print(f"Failed to create user {email}: {e}")


def extract_video_names_from_annotation_json(json_path: str = None):
    with open(json_path, 'r') as f:
        data = json.load(f)
    video_names = []
    for item in data:
        video_names.extend(item.keys())
    return video_names

def create_project_from_annotation_json(json_path: str = None, project_name: str = None, schema_name: str = None, batch_size: int = 15):
    # 1. Extract video names
    video_names = extract_video_names_from_annotation_json(json_path)
    print(f"Found {len(video_names)} video names in the JSON file.")
    # 2. Connect to the database
    session = SessionLocal()
    try:
        # 3. Get schema id
        schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
        print(f"Found schema '{schema_name}', ID: {schema_id}")
        # 4. Get all videos
        all_videos_df = VideoService.get_all_videos(session)
        existing_video_uids = set(all_videos_df['Video UID'])
        # 5. Check for missing videos
        missing_videos = [name for name in video_names if name not in existing_video_uids]
        if missing_videos:
            print(f"{len(missing_videos)} videos are missing in the database. Project creation aborted.")
            for mv in missing_videos:
                print(mv)
            return
        # 6. Get video IDs
        video_ids = ProjectService.get_video_ids_by_uids(video_names, session)
        # 7. Create projects in batches
        total_videos = len(video_ids)
        for i in range(0, total_videos, batch_size):
            batch_video_ids = video_ids[i:i + batch_size]
            project_name_with_batch = f"{project_name}-{i//batch_size + 1}"
            print(f"Creating project {project_name_with_batch}...")
            try:
                ProjectService.create_project(
                    name=project_name_with_batch,
                    schema_id=schema_id,
                    video_ids=batch_video_ids,
                    session=session
                )
                print(f"Project {project_name_with_batch} created successfully!")
            except ValueError as e:
                if "already exists" in str(e):
                    print(f"Project {project_name_with_batch} already exists, skipping...")
                else:
                    raise e
    finally:
        session.close()

if __name__ == "__main__":
    # Example usage:
    # update_or_add_videos()
    # import_schemas()
    # create_project_with_videos("../new_video_metadata.json", "CamLight", "CameraLightSetup")
    pass