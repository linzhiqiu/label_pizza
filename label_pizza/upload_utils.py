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
from label_pizza.db import SessionLocal, engine # Must have been initialized by init_database() before importing this file
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Any, Set


def add_videos(videos_data: list[dict]) -> None:
    """
    Add new videos from an in-memory list of dicts.  
    Skips videos that already exist and prints info.

    Args:
        videos_data: A list of dictionaries, each with keys
                     "url" (str) and "metadata" (dict).
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    # Validate and add inside one DB session
    with SessionLocal() as session:
        # 1️⃣ Pre-check for duplicates or other validation errors
        duplicate_urls = []
        valid_videos = []
        
        for video in tqdm(videos_data, desc="Verifying videos"):
            try:
                VideoService.verify_add_video(
                    url=video["url"],
                    session=session,
                    metadata=video.get("metadata")
                )
                valid_videos.append(video)
            except ValueError as e:
                # Collect "already exists" errors, propagate the rest
                if "already exists" in str(e):
                    duplicate_urls.append(video["url"])
                    print(f"⏭️  Skipped existing video: {video['url']}")
                else:
                    raise ValueError(
                        f"Validation failed for {video['url']}: {e}"
                    ) from None

        if duplicate_urls:
            print(f"ℹ️  Skipped {len(duplicate_urls)} existing videos")

        # 2️⃣ Add only valid videos
        if valid_videos:
            for video in tqdm(valid_videos, desc="Adding videos", unit="video"):
                VideoService.add_video(
                    url=video["url"],
                    session=session,
                    metadata=video.get("metadata")
                )
                print(f"✓ Added new video: {video['url']}")

            # 3️⃣ Commit once at the end
            try:
                session.commit()
                print(f"✔ Successfully added {len(valid_videos)} new videos!")
            except Exception as e:
                session.rollback() 
                raise RuntimeError(f"Error committing changes: {e}") from None
        else:
            print("ℹ️  No new videos to add - all videos already exist")

def upload_videos(videos_path: str = None, videos_data: list[dict] = None) -> None:

    # Check that at least one parameter is provided
    if videos_path is None and videos_data is None:
        raise ValueError("At least one parameter must be provided: video_path or videos_data")
    
    if videos_path is not None:
        # Upload from file path
        import glob

        with open(videos_path, 'r') as f:
            video_data = json.load(f)
    add_videos(video_data)

def update_videos(videos_data: list[dict]) -> None:
    """
    Update existing videos given an in-memory list of dicts.

    Args:
        videos_data: A list of dictionaries, each containing
                     "video_uid" (str), "url" (str), and "metadata" (dict).
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    with SessionLocal() as session:
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

def import_questions() -> None:
    import glob
    with SessionLocal() as session:
        question_paths = glob.glob('./questions/*.json')
        for question_path in tqdm(question_paths, desc="Importing questions"):
            with open(question_path, 'r') as f:
                question_data = json.load(f)
                
            # Check if question already exists
            try:
                existing_question = QuestionService.get_question_by_text(question_data["text"], session)
                print(f"⏭️  Skipped existing question: {question_data['text']}")
                continue
            except ValueError:
                # Question doesn't exist, proceed with adding it
                pass
            
            QuestionService.add_question(
                text=question_data["text"],
                qtype=question_data["type"],
                options=question_data.get("options", None),
                default=question_data.get("default_option", None),
                session=session,
                display_values=question_data.get("display_values", None),
                display_text=question_data.get("display_text", None),
                option_weights=question_data.get("option_weights", None),
            )
            print(f"✓ Imported question: {question_data['text']}")

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
            # ──────────────  Phase 2: APPLY changes  ──────────────
            question_ids = [QuestionService.get_question_by_text(q["text"], session)["id"] for q in group_data["questions"]]

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

def import_question_groups(question_groups_folder: str = None, question_groups_data: list[dict] = None) -> None:
    
    if question_groups_folder is None and question_groups_data is None:
        raise ValueError("At least one parameter must be provided: question_groups_folder or question_groups_data")
    
    import glob
    if question_groups_folder is not None:
        question_groups_data = []
        group_paths = glob.glob(question_groups_folder + '/*.json')
        for group_path in group_paths:
            with open(group_path, 'r') as f:
                question_groups_data.append(json.load(f))

    with SessionLocal() as session:
        for question_group_data in tqdm(question_groups_data, desc="Importing question groups"):
            questions = question_group_data['questions']
            # Check whether all questions are existing in the Question table
            for question in questions:
                try:
                    question = QuestionService.get_question_by_text(question.get("text", None), session)
                except ValueError as e:
                    if "not found" in str(e):
                        print(f"Question {question.get('text', None)} not found in the Question table")
                        print(f"Import Question: {question.get('text', None)}...")
                        QuestionService.add_question(
                            text=question["text"],
                            qtype=question["qtype"],
                            options=question.get("options", None),
                            default=question.get("default_option", None),
                            session=session,
                            display_values=question.get("display_values", None),
                            display_text=question.get("display_text", None),
                            option_weights=question.get("option_weights", None),
                        )
                        print(f"✓ Imported question: {question.get('text', None)}")
                    else:
                        raise ValueError(f"Error occurs:{e}")
            title = question_group_data['title']
            try:
                QuestionGroupService.get_group_by_name(title, session)
                print(f"⏭️  Skipped existing question group: {title}")
                continue
            except ValueError:
                # Question group doesn't exist, proceed with adding it
                import_question_group(question_group_data)
                pass
        
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

def create_schemas(schemas_path: str = None, schemas_data: list[dict] = None, question_groups_folder: str = None, question_groups_data: list[dict] = None) -> None:
    if schemas_path is None and schemas_data is None:
        raise ValueError("At least one parameter must be provided: schemas_path or schemas_data")
    
    if schemas_path is not None:
        with open(schemas_path, 'r') as f:
            schemas_data = json.load(f)
    import_question_groups(question_groups_folder, question_groups_data)
    with SessionLocal() as session:
        for schema_data in schemas_data:
            schema_name = schema_data.get('schema_name', None)
            try:
                SchemaService.get_schema_id_by_name(schema_name, session)
            except ValueError as e:
                if "not found" in str(e):
                    create_schema(schema_data)
                else:
                    pass

def upload_users(users_path: str = None, users_data: list[dict] = None):
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
    if users_path is None and users_data is None:
        raise ValueError("At least one parameter must be provided: users_path or users_data")
    
    if users_path is not None:
        with open(users_path, 'r') as f:
            users_data = json.load(f)

    with SessionLocal() as session:
        existing_users = AuthService.get_all_users(session)
        existing_user_ids = set(existing_users['User ID'].tolist())

        for user in users_data:
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            password = user['password']
            user_type = user.get('user_type', 'human')

            if user_id in existing_user_ids:
                print(f"User {user_id} already exists, skipping.")
                continue

            # Hash the password (sha256)
            password_hash = password

            try:
                AuthService.create_user(
                    user_id=user_id,
                    email=email,
                    password_hash=password_hash,
                    user_type=user_type,
                    session=session
                )
                print(f"Successfully created user {user_id}")
            except Exception as e:
                print(f"Failed to create user {user_id}: {e}")

# ---------------------------------------------------------------------------
# 0. helper – assert that all UIDs exist in DB
# ---------------------------------------------------------------------------
def _assert_all_videos_exist(video_uids: List[str], session: Session) -> None:
    """
    Raise ValueError listing *all* missing video_uids (if any).
    """
    missing: List[str] = [
        uid for uid in video_uids
        if VideoService.get_video_by_uid(uid, session) is None
    ]

    if missing:
        msg = (
            f"[ABORT] {len(missing)} videos are not present in the database.\n"
            f"First 10 missing: {missing[:10]}"
        )
        raise ValueError(msg)

# ──────────────────────────────────────────────────────────────────────
# 1. Collect UIDs and verify they exist as we go
# ──────────────────────────────────────────────────────────────────────
def _collect_existing_uids(ndjson_path: str | Path, session: Session) -> List[str]:
    """
    Read the NDJSON and return a list of unique video_uids that already
    exist in the DB.  If *any* uid is missing we raise immediately.
    """
    ndjson_path = Path(ndjson_path)
    existing: Set[str] = set()

    with ndjson_path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            blob = json.loads(raw)
            try:
                uid = blob["data_row"]["external_id"]
            except KeyError:
                raise ValueError(f"line {line_no}: missing data_row.external_id")

            # ----- existence check -------------------------------------
            if not VideoService.get_video_by_uid(uid, session):
                raise ValueError(
                    f"[ABORT] Video '{uid}' (line {line_no}) does not exist in DB"
                )

            existing.add(uid)

    return sorted(existing)

# ──────────────────────────────────────────────────────────────────────
# 3. Create projects from extracted annotations JSON
# ──────────────────────────────────────────────────────────────────────
def create_projects(
    projects_path: str = None,
    projects_data: list[dict] = None,
) -> None:

    if projects_path is None and projects_data is None:
        raise ValueError("At least one parameter must be provided: projects_path or projects_data")
    
    if projects_path is not None:
        with open(projects_path, 'r') as f:
            projects_data = json.load(f)
    
    with SessionLocal() as session:
        for project_data in projects_data:
            # Check schema existence
            try:
                schema_id = SchemaService.get_schema_id_by_name(project_data.get('schema_name', None), session)
            except ValueError as e:
                if "not found" in str(e):
                    raise ValueError(f"Schema {project_data.get('schema_name', None)} not found! Please create the schema first!")
                else:
                    raise e
            # Check all videos exist
            missing_videos = []
            video_uids_list = project_data.get('videos', None)
            project_name = project_data.get('project_name', None)
            for uid in video_uids_list:
                if not VideoService.get_video_by_uid(uid, session):
                    missing_videos.append(uid)
            if missing_videos:
                raise ValueError(f"[WARNING] Project '{project_name}' has missing videos: {missing_videos}")
                continue
            video_ids = ProjectService.get_video_ids_by_uids(video_uids_list, session)
            # Check if project already exists
            try:
                project = ProjectService.get_project_by_name(project_data.get('project_name', None), session)
                print(f"Project {project.name} already exists")
            except Exception as e:
                if ("not found" in str(e)):
                    ProjectService.create_project(name = project_name, schema_id = schema_id, video_ids = video_ids, session = session)
                else:
                    raise ValueError(f"Error creating project {project_name}: {e}")

def bulk_assign_users(assignment_path: str = None, assignments_data: list[dict] = None):
    """
    Bulk assign users to projects using only service functions.
    """
    if assignment_path is None and assignments_data is None:
        raise ValueError("At least one parameter must be provided: assignment_path or assignments_data")
    
    if assignment_path is not None:
        with open(assignment_path, 'r') as f:
            assignments_data = json.load(f)
    
    with SessionLocal() as session:
        try:
            for assignment in assignments_data:
                try:
                    user = AuthService.get_user_by_name(assignment["user_name"], session)
                    project = ProjectService.get_project_by_name(assignment["project_name"], session)
                    
                    # Skip global admin users
                    if user.user_type == "admin":
                        print(f"⚠️ Skipped: {assignment['user_name']} is a global admin, cannot assign non-admin role")
                        continue
                    if user.user_type == "model":
                        new_role = assignment["role"]
                        ProjectService.add_user_to_project(
                            project_id=project.id,
                            user_id=user.id, 
                            role=new_role,
                            session=session
                        )
                        print(f"✓ Assigned model user {assignment['user_name']} to {assignment['project_name']} as {new_role}")
                        continue
                    # Get user's projects by role using service function
                    user_projects = AuthService.get_user_projects_by_role(user.id, session)
                    
                    # Check if user has any role in this project
                    user_has_role = False
                    current_role = None
                    
                    for role_type, projects in user_projects.items():
                        for proj in projects:
                            if proj["id"] == project.id:
                                user_has_role = True
                                current_role = role_type
                                break
                        if user_has_role:
                            break
                    
                    new_role = assignment["role"]
                    
                    # Apply business logic
                    if not user_has_role:
                        # No existing role - assign new role
                        ProjectService.add_user_to_project(
                            project_id=project.id,
                            user_id=user.id, 
                            role=new_role,
                            session=session
                        )
                        print(f"✓ Assigned {assignment['user_name']} to {assignment['project_name']} as {new_role}")
                        
                    elif current_role == "annotator" and new_role == "reviewer":
                        # annotator -> reviewer: Update
                        ProjectService.add_user_to_project(
                            project_id=project.id,
                            user_id=user.id, 
                            role=new_role,
                            session=session
                        )
                        print(f"✓ Updated {assignment['user_name']} from annotator to reviewer in {assignment['project_name']}")
                        
                    elif current_role == "reviewer" and new_role == "annotator":
                        # reviewer -> annotator: Ignore
                        print(f"⚠️ Ignored: {assignment['user_name']} already reviewer, not downgrading to annotator in {assignment['project_name']}")
                        
                    else:
                        # Other cases: Update role
                        ProjectService.add_user_to_project(
                            project_id=project.id,
                            user_id=user.id, 
                            role=new_role,
                            session=session
                        )
                        print(f"✓ Updated {assignment['user_name']} role to {new_role} in {assignment['project_name']}")
                    
                except Exception as e:
                    print(f"✗ Failed: {e}")
                    session.rollback()
                    continue
            
            session.commit()
            print("🎉 Bulk assignment completed!")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            session.rollback()