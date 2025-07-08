import json
from sqlalchemy.orm import Session
from functools import lru_cache
from tqdm import tqdm
from label_pizza.services import (
    VideoService, 
    ProjectService, 
    SchemaService, 
    QuestionGroupService, 
    QuestionService,
    AuthService,
    AnnotatorService,
    GroundTruthService,
    CustomDisplayService
)
from label_pizza.db import SessionLocal
from pathlib import Path
from typing import List, Dict, Optional, Any, Set, Tuple
import pandas as pd
import os


# --------------------------------------------------------------------------- #
# Core operations                                                             #
# --------------------------------------------------------------------------- #

def add_videos(videos_data: List[Dict]) -> None:
    """Insert videos that are *not* yet in DB – relies on verify_add_video."""
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list[dict]")

    with SessionLocal() as sess:
        duplicates = []
        
        # Verify all videos with progress bar
        with tqdm(total=len(videos_data), desc="Verifying videos for addition", unit="video") as pbar:
            for v in videos_data:
                try:
                    VideoService.verify_add_video(
                        video_uid=v["video_uid"],
                        url=v["url"],
                        metadata=v.get("metadata"),
                        session=sess,
                    )
                except ValueError as err:
                    if "already exists" in str(err):
                        duplicates.append(v["video_uid"])
                    else:
                        raise
                pbar.update(1)

        if duplicates:
            raise ValueError("Add aborted – already in DB: " + ", ".join(duplicates))

        # Add videos with progress bar
        with tqdm(total=len(videos_data), desc="Adding videos", unit="video") as pbar:
            for v in videos_data:
                VideoService.add_video(
                    video_uid=v["video_uid"],
                    url=v["url"],
                    metadata=v.get("metadata"),
                    session=sess,
                )
                pbar.set_postfix(uid=v["video_uid"][:20] + "..." if len(v["video_uid"]) > 20 else v["video_uid"])
                pbar.update(1)
                
        sess.commit()
        print(f"✔ Added {len(videos_data)} new video(s)")


def update_videos(videos_data: List[Dict]) -> None:
    """Update videos that *must* exist – relies on verify_update_video."""
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list[dict]")

    with SessionLocal() as sess:
        missing = []
        
        # Verify all videos with progress bar
        with tqdm(total=len(videos_data), desc="Verifying videos for update", unit="video") as pbar:
            for v in videos_data:
                try:
                    VideoService.verify_update_video(
                        video_uid=v["video_uid"],
                        new_url=v["url"],
                        new_metadata=v.get("metadata"),
                        session=sess,
                    )
                except ValueError as err:
                    if "not found" in str(err):
                        missing.append(v["video_uid"])
                    else:
                        raise
                pbar.update(1)

        if missing:
            raise ValueError("Update aborted – not found in DB: " + ", ".join(missing))

        # Update videos with progress bar
        with tqdm(total=len(videos_data), desc="Updating videos", unit="video") as pbar:
            for v in videos_data:
                VideoService.update_video(
                    video_uid=v["video_uid"],
                    new_url=v["url"],
                    new_metadata=v.get("metadata"),
                    session=sess,
                )
                
                # Handle archive status if present
                if "is_archived" in v:
                    rec = VideoService.get_video_by_uid(v["video_uid"], sess)
                    if rec and v["is_archived"] != rec.is_archived:
                        if v["is_archived"]:
                            VideoService.archive_video(rec.id, sess)
                        else:
                            rec.is_archived = False
                
                pbar.set_postfix(uid=v["video_uid"][:20] + "..." if len(v["video_uid"]) > 20 else v["video_uid"])
                pbar.update(1)

        sess.commit()
        print(f"✔ Updated {len(videos_data)} video(s)")

# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #

def sync_videos(
    *, videos_path: str | Path | None = None, videos_data: List[Dict] | None = None
) -> None:
    """Load data, normalise video_uid, and route to add/update pipelines."""

    if videos_path is None and videos_data is None:
        raise ValueError("Provide either videos_path or videos_data")

    # Load JSON if a path is provided
    if videos_path:
        print(f"📂 Loading videos from {videos_path}")
        with open(videos_path, "r") as f:
            videos_data = json.load(f)

    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list[dict]")

    print(f"\n🚀 Starting video sync pipeline with {len(videos_data)} videos...")

    # Validate & enrich each record with progress bar
    processed: List[Dict] = []
    with tqdm(total=len(videos_data), desc="Validating video data", unit="video") as pbar:
        for idx, item in enumerate(videos_data, 1):
            required = {"url", "video_uid", "metadata", "is_active"}
            if missing := required - set(item.keys()):
                raise ValueError(f"Entry #{idx} missing: {', '.join(missing)}")

            # optional active → archived conversion
            if "is_active" in item:
                item["is_archived"] = not item.pop("is_active")

            processed.append(item)
            pbar.update(1)

    # Decide add vs update with a single read-only look‑up
    print("\n📊 Categorizing videos...")
    with SessionLocal() as sess:
        to_add, to_update = [], []
        with tqdm(total=len(processed), desc="Checking existing videos", unit="video") as pbar:
            for v in processed:
                existing = VideoService.get_video_by_uid(v["video_uid"], sess)
                if existing:
                    to_update.append(v)
                else:
                    to_add.append(v)
                pbar.update(1)
    
    print(f"\n📈 Summary: {len(to_add)} videos to add, {len(to_update)} videos to update")
    
    if to_add:
        print(f"\n➕ Adding {len(to_add)} new videos...")
        add_videos(to_add)
        
    if to_update:
        print(f"\n🔄 Updating {len(to_update)} existing videos...")
        update_videos(to_update)
        
    print("\n🎉 Video pipeline complete!")


# --------------------------------------------------------------------------- #
# Core operations                                                             #
# --------------------------------------------------------------------------- #

def add_users(users_data: List[Dict]) -> None:
    """Insert users that are *not* yet in DB – relies on verify_add_user."""
    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list[dict]")

    with SessionLocal() as sess:
        duplicates = []
        for u in users_data:
            try:
                AuthService.verify_create_user(
                    user_id=u.get("user_id"),
                    email=u.get("email"),
                    password_hash=u.get("password"),
                    user_type=u.get("user_type", "human"),
                    session=sess,
                )
            except ValueError as err:
                if "already exists" in str(err):
                    duplicates.append(u.get("user_id") or u.get("email"))
                else:
                    raise

        if duplicates:
            raise ValueError("Add aborted – already in DB: " + ", ".join(duplicates))

        for u in users_data:
            AuthService.create_user(
                user_id=u.get("user_id"),
                email=u.get("email"),
                password_hash=u.get("password"),
                user_type=u.get("user_type", "human"),
                is_archived=u.get("is_archived", False),
                session=sess,
            )
        sess.commit()
        print(f"✔ Added {len(users_data)} new user(s)")


def update_users(users_data: List[Dict]) -> None:
    """Update users that *must* exist – checks user existence before updating."""
    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list[dict]")

    with SessionLocal() as sess:
        missing = []
        for u in users_data:
            user_exists = False
            
            # Check if user exists by user_id first
            if u.get("user_id"):
                try:
                    existing_user = AuthService.get_user_by_id(u["user_id"], sess)
                    if existing_user:
                        user_exists = True
                except ValueError as err:
                    if "not found" in str(err).lower():
                        user_exists = False
                    else:
                        raise
            
            # If not found by user_id, check by email
            if not user_exists and u.get("email"):
                try:
                    existing_user = AuthService.get_user_by_email(u["email"], sess)
                    if existing_user:
                        user_exists = True
                except ValueError as err:
                    if "not found" in str(err).lower():
                        user_exists = False
                    else:
                        raise
            
            # If user doesn't exist, add to missing list
            if not user_exists:
                missing.append(u.get("user_id") or u.get("email"))

        if missing:
            raise ValueError("Update aborted – not found in DB: " + ", ".join(missing))

        for u in users_data:
            try:
                user_rec = AuthService.get_user_by_id(u["user_id"], sess) if u.get("user_id") else AuthService.get_user_by_email(u["email"], sess)

                if "email" in u and u["email"] != user_rec.email:
                    AuthService.update_user_email(user_rec.id, u["email"], sess)
                if "password" in u:
                    AuthService.update_user_password(user_rec.id, u["password"], sess)
                if "user_type" in u and u["user_type"] != user_rec.user_type:
                    AuthService.update_user_role(user_rec.id, u["user_type"], sess)
                if "user_id" in u and u["user_id"] != user_rec.user_id_str:
                    AuthService.update_user_id(user_rec.id, u["user_id"], sess)
                if "is_archived" in u and u["is_archived"] != user_rec.is_archived:
                    AuthService.toggle_user_archived(user_rec.id, sess)
            except ValueError as err:
                if "not found" in str(err).lower():
                    # This shouldn't happen since we verified earlier, but handle gracefully
                    print(f"⚠ User not found during update: {u.get('user_id') or u.get('email')}")
                    continue
                else:
                    raise

        sess.commit()
        print(f"✔ Updated {len(users_data)} user(s)")

# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #

def sync_users(
    *, users_path: str | Path | None = None, users_data: List[Dict] | None = None
) -> None:
    """Load JSON / list and route to add/update."""

    if users_path is None and users_data is None:
        raise ValueError("Provide either users_path or users_data")

    if users_path:
        with open(users_path, "r") as f:
            users_data = json.load(f)

    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list[dict]")

    # Convert is_active → is_archived and validate required fields
    processed: List[Dict] = []
    for idx, user in enumerate(users_data, 1):
        required = {"user_id", "email", "password", "user_type", "is_active"}
        if missing := required - set(user.keys()):
            raise ValueError(f"Entry #{idx} missing: {', '.join(missing)}")
        if "is_active" in user:
            user["is_archived"] = not user.pop("is_active")
        processed.append(user)

    # Separate users into add/update lists with proper error handling
    to_add, to_update = [], []
    
    with SessionLocal() as sess:
        for u in processed:
            user_exists = False
            
            # Check if user exists by user_id first
            if u.get("user_id"):
                try:
                    existing_user = AuthService.get_user_by_id(u["user_id"], sess)
                    if existing_user:
                        user_exists = True
                except (ValueError, Exception) as e:
                    # If error contains "not found", user doesn't exist
                    if "not found" in str(e).lower():
                        user_exists = False
                    else:
                        # Re-raise unexpected errors
                        raise
            
            # If not found by user_id, check by email
            if not user_exists and u.get("email"):
                try:
                    existing_user = AuthService.get_user_by_email(u["email"], sess)
                    if existing_user:
                        user_exists = True
                except (ValueError, Exception) as e:
                    # If error contains "not found", user doesn't exist
                    if "not found" in str(e).lower():
                        user_exists = False
                    else:
                        # Re-raise unexpected errors
                        raise
            
            # Add to appropriate list based on existence
            if user_exists:
                to_update.append(u)
            else:
                to_add.append(u)

    print(f"📊 {len(to_add)} to add · {len(to_update)} to update")
    
    if to_add:
        add_users(to_add)
    if to_update:
        update_users(to_update)
    
    print("🎉 User pipeline complete")


# --------------------------------------------------------------------------- #
# Core operations                                                             #
# --------------------------------------------------------------------------- #


def add_question_groups(groups: List[Tuple[str, Dict]]) -> Tuple[List[Dict], List[str]]:
    """Create brand‑new groups after *full* verification, single commit."""
    if not isinstance(groups, list):
        raise TypeError("groups must be list[(filename, dict)]")

    created: List[Dict] = []
    questions_created: List[str] = []

    with SessionLocal() as sess:
        # ── Phase 0: duplicate title check (cheap, read‑only) ───────────────
        dup_titles = []
        for _, g in groups:
            try:
                QuestionGroupService.get_group_by_name(g["title"], sess)
                dup_titles.append(g["title"])
            except ValueError as err:
                # Only ignore "not found" errors, re-raise others
                if "not found" not in str(err).lower():
                    raise
                # Group doesn't exist, which is what we want for adding
        
        if dup_titles:
            raise ValueError("Add aborted – already in DB: " + ", ".join(dup_titles))

        # ── Phase 1: prepare each group (create missing questions) ──────────
        prepared: List[Tuple[Dict, List[int]]] = []  # (group_data, question_ids)
        for _, g in groups:
            q_ids: List[int] = []
            for q in g["questions"]:
                try:
                    q_rec = QuestionService.get_question_by_text(q["text"], sess)
                    q_ids.append(q_rec["id"])
                except ValueError:
                    # Question doesn't exist, create it
                    q_rec = QuestionService.add_question(
                        text=q["text"],
                        qtype=q["qtype"],
                        options=q.get("options"),
                        default=q.get("default_option"),
                        display_values=q.get("display_values"),
                        display_text=q.get("display_text"),
                        option_weights=q.get("option_weights"),
                        session=sess,
                    )
                    questions_created.append(q["text"])
                    q_ids.append(q_rec.id)
            prepared.append((g, q_ids))

        # ── Phase 2: verify ALL groups before any create_group ──────────────
        for g, q_ids in prepared:
            QuestionGroupService.verify_create_group(
                title=g["title"],
                display_title=g.get("display_title", g["title"]),
                description=g["description"],
                is_reusable=g.get("is_reusable", True),
                question_ids=q_ids,
                verification_function=g.get("verification_function"),
                is_auto_submit=g.get("is_auto_submit", False),
                session=sess,
            )

        # ── Phase 3: all verifications passed – perform creations ───────────
        for g, q_ids in prepared:
            grp = QuestionGroupService.create_group(
                title=g["title"],
                display_title=g.get("display_title", g["title"]),
                description=g["description"],
                is_reusable=g.get("is_reusable", True),
                question_ids=q_ids,
                verification_function=g.get("verification_function"),
                is_auto_submit=g.get("is_auto_submit", False),
                session=sess,
            )
            if g.get("is_archived", False):
                QuestionGroupService.archive_group(grp.id, sess)
            created.append({"title": g["title"], "id": grp.id})

        sess.commit()
    return created, list(set(questions_created))


def update_question_groups(groups: List[Tuple[str, Dict]]) -> List[Dict]:
    """Edit existing groups after *full* verification, single commit."""
    if not isinstance(groups, list):
        raise TypeError("groups must be list[(filename, dict)]")

    updated: List[Dict] = []
    with SessionLocal() as sess:
        # ── Phase 0: existence check (cheap, read‑only) ────────────────────
        missing = []
        for _, g in groups:
            try:
                QuestionGroupService.get_group_by_name(g["title"], sess)
            except ValueError as err:
                # Only treat "not found" as missing, re-raise other errors
                if "not found" not in str(err).lower():
                    raise
                # Group doesn't exist
                missing.append(g["title"])
        
        if missing:
            raise ValueError("Update aborted – not found in DB: " + ", ".join(missing))

        # ── Phase 1: verify ALL edits first ─────────────────────────────────
        for _, g in groups:
            grp = QuestionGroupService.get_group_by_name(g["title"], sess)
            QuestionGroupService.verify_edit_group(
                group_id=grp.id,
                new_display_title=g.get("display_title", g["title"]),
                new_description=g["description"],
                is_reusable=g.get("is_reusable", True),
                verification_function=g.get("verification_function"),
                is_auto_submit=g.get("is_auto_submit", False),
                session=sess,
            )

        # ── Phase 2: apply edits after all verifications passed ─────────────
        for _, g in groups:
            grp = QuestionGroupService.get_group_by_name(g["title"], sess)
            QuestionGroupService.edit_group(
                group_id=grp.id,
                new_display_title=g.get("display_title", g["title"]),
                new_description=g["description"],
                is_reusable=g.get("is_reusable", True),
                verification_function=g.get("verification_function"),
                is_auto_submit=g.get("is_auto_submit", False),
                session=sess,
            )
            
            # Handle archiving/unarchiving
            if "is_archived" in g and g["is_archived"] != grp.is_archived:
                if g["is_archived"]:
                    QuestionGroupService.archive_group(grp.id, sess)
                else:
                    QuestionGroupService.unarchive_group(grp.id, sess)
            updated.append({"title": g["title"], "id": grp.id})

        sess.commit()
    return updated

# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #

def sync_question_groups(
    question_groups_folder: str = None, 
    question_groups_data: List[Dict] = None) -> None:
    """Validate every file first, then route to add/update ops."""

    # Validate input parameters
    if question_groups_folder is None and question_groups_data is None:
        raise ValueError("Either question_groups_folder or question_groups_data must be provided")
    
    # 1️⃣ Load & JSON-level validation
    loaded: List[Tuple[str, Dict]] = []
    
    if question_groups_folder is not None:
        # Load from folder
        folder = Path(question_groups_folder)
        if not folder.exists() or not folder.is_dir():
            raise ValueError(f"Invalid folder: {question_groups_folder}")

        json_paths = list(folder.glob("*.json"))
        if not json_paths:
            raise ValueError(f"No JSON files in {question_groups_folder}")

        for pth in json_paths:
            with open(pth, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError(f"{pth.name}: file must contain a JSON object")
            
            # Validate required fields
            for fld in ("title", "description", "questions", "is_active"):
                if fld not in data:
                    raise ValueError(f"{pth.name}: missing required field '{fld}'")
            
            # Set defaults and normalize
            data.setdefault("display_title", data["title"])
            if "is_active" in data:
                data["is_archived"] = not data.pop("is_active")
            
            if not isinstance(data["questions"], list):
                raise ValueError(f"{pth.name}: 'questions' must be a list")
            
            loaded.append((pth.name, data))
    
    else:
        # Load from data list
        if not isinstance(question_groups_data, list):
            raise TypeError("question_groups_data must be a list of dictionaries")
        
        for idx, data in enumerate(question_groups_data, 1):
            if not isinstance(data, dict):
                raise ValueError(f"Item #{idx}: must be a dictionary")
            
            # Create a copy to avoid modifying original data
            data_copy = data.copy()
            
            # Validate required fields
            for fld in ("title", "description", "questions", "is_active"):
                if fld not in data_copy:
                    raise ValueError(f"Item #{idx}: missing required field '{fld}'")
            
            # Set defaults and normalize
            data_copy.setdefault("display_title", data_copy["title"])
            if "is_active" in data_copy:
                data_copy["is_archived"] = not data_copy.pop("is_active")
            
            if not isinstance(data_copy["questions"], list):
                raise ValueError(f"Item #{idx}: 'questions' must be a list")
            
            # Use index as filename for data items
            loaded.append((f"data_item_{idx}", data_copy))

    print(f"✅ JSON validation passed for {len(loaded)} items")

    # 2️⃣ Classify add vs update with one read-only session
    to_add, to_update = [], []
    with SessionLocal() as sess:
        for fn, g in loaded:
            group_exists = False
            try:
                QuestionGroupService.get_group_by_name(g["title"], sess)
                group_exists = True
            except ValueError as err:
                # Only treat "not found" as non-existence, re-raise other errors
                if "not found" not in str(err).lower():
                    raise
                # Group doesn't exist
                group_exists = False
            
            if group_exists:
                to_update.append((fn, g))
            else:
                to_add.append((fn, g))

    print(f"📊 {len(to_add)} to add · {len(to_update)} to update")

    # 3️⃣ Execute operations
    created, questions_created = [], []
    updated = []
    
    if to_add:
        c, qc = add_question_groups(to_add)
        created.extend(c)
        questions_created.extend(qc)
    
    if to_update:
        updated.extend(update_question_groups(to_update))

    print("🎉 Question-group pipeline complete")
    print(f"   • Groups created: {len(created)}")
    print(f"   • Groups updated: {len(updated)}")
    print(f"   • New questions:  {len(questions_created)}")

# --------------------------------------------------------------------------- #
# Core operations                                                             #
# --------------------------------------------------------------------------- #


def add_schemas(schemas: List[Dict]) -> List[Dict]:
    """Create brand‑new schemas after full verification, single commit."""
    if not isinstance(schemas, list):
        raise TypeError("schemas must be list[dict]")

    created: List[Dict] = []

    with SessionLocal() as sess:
        # ── Phase 0: duplicate name check (cheap, read‑only) ───────────────
        dup_names = []
        for s in schemas:
            try:
                SchemaService.get_schema_by_name(s["schema_name"], sess)
                dup_names.append(s["schema_name"])
            except ValueError as err:
                # Only ignore "not found" errors, re-raise others
                if "not found" not in str(err).lower():
                    raise
                # Schema doesn't exist, which is what we want for adding
        
        if dup_names:
            raise ValueError("Add aborted – already in DB: " + ", ".join(dup_names))

        # ── Phase 1: resolve group names → ids & run verify_create_schema ────
        prepared: List[Tuple[Dict, List[int]]] = []
        for s in schemas:
            group_ids: List[int] = []
            for gname in s["question_group_names"]:
                group = QuestionGroupService.get_group_by_name(gname, sess)
                group_ids.append(group.id)

            SchemaService.verify_create_schema(
                name=s["schema_name"],
                question_group_ids=group_ids,
                instructions_url=s.get("instructions_url"),
                has_custom_display=s.get("has_custom_display", False),
                session=sess,
            )
            prepared.append((s, group_ids))

        # ── Phase 2: create after all verifications passed ──────────────────
        for s, group_ids in prepared:
            sch = SchemaService.create_schema(
                name=s["schema_name"],
                question_group_ids=group_ids,
                instructions_url=s.get("instructions_url"),
                has_custom_display=s.get("has_custom_display", False),
                session=sess,
            )
            if s.get("is_archived", False):
                SchemaService.archive_schema(sch.id, sess)
            created.append({"name": s["schema_name"], "id": sch.id})

        sess.commit()
    return created


def update_schemas(schemas: List[Dict]) -> List[Dict]:
    """Edit existing schemas after full verification, single commit."""
    if not isinstance(schemas, list):
        raise TypeError("schemas must be list[dict]")

    updated: List[Dict] = []
    with SessionLocal() as sess:
        # ── Phase 0: existence check ───────────────────────────────────────
        missing = []
        for s in schemas:
            try:
                SchemaService.get_schema_by_name(s["schema_name"], sess)
            except ValueError as err:
                # Only treat "not found" as missing, re-raise other errors
                if "not found" not in str(err).lower():
                    raise
                # Schema doesn't exist
                missing.append(s["schema_name"])
        
        if missing:
            raise ValueError("Update aborted – not found in DB: " + ", ".join(missing))

        # ── Phase 1: verify ALL edits first ─────────────────────────────────
        for s in schemas:
            sch = SchemaService.get_schema_by_name(s["schema_name"], sess)
            SchemaService.verify_edit_schema(
                schema_id=sch.id,
                instructions_url=s.get("instructions_url"),
                has_custom_display=s.get("has_custom_display", False),
                is_archived=s.get("is_archived"),
                session=sess,
            )

        # ── Phase 2: apply edits ────────────────────────────────────────────
        for s in schemas:
            sch = SchemaService.get_schema_by_name(s["schema_name"], sess)
            SchemaService.edit_schema(
                schema_id=sch.id,
                name=s["schema_name"],
                instructions_url=s.get("instructions_url"),
                has_custom_display=s.get("has_custom_display", False),
                is_archived=s.get("is_archived"),
                session=sess,
            )
            updated.append({"name": s["schema_name"], "id": sch.id})

        sess.commit()
    return updated

# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #

def sync_schemas(*, schemas_path: str | Path | None = None, schemas_data: List[Dict] | None = None) -> None:
    """Normalise input, classify add vs update, delegate, print summary."""

    if schemas_path is None and schemas_data is None:
        raise ValueError("Provide either schemas_path or schemas_data")

    # Load JSON if path provided
    if schemas_path:
        with open(schemas_path, "r") as f:
            schemas_data = json.load(f)

    if not isinstance(schemas_data, list):
        raise TypeError("schemas_data must be list[dict]")

    processed: List[Dict] = []
    for idx, s in enumerate(schemas_data, 1):
        required = {"schema_name", "question_group_names", "instructions_url", "has_custom_display", "is_active"}
        if missing := required - set(s.keys()):
            raise ValueError(f"Entry #{idx} missing: {', '.join(missing)}")
        if not isinstance(s["question_group_names"], list):
            raise ValueError(f"Entry #{idx}: 'question_group_names' must be list")
        if "is_active" in s:
            s["is_archived"] = not s.pop("is_active")
        processed.append(s)

    # Decide add vs update ---------------------------------------------------
    to_add, to_update = [], []
    with SessionLocal() as sess:
        for s in processed:
            schema_exists = False
            try:
                SchemaService.get_schema_by_name(s["schema_name"], sess)
                schema_exists = True
            except ValueError as err:
                # Only treat "not found" as non-existence, re-raise other errors
                if "not found" not in str(err).lower():
                    raise
                # Schema doesn't exist
                schema_exists = False
            
            if schema_exists:
                to_update.append(s)
            else:
                to_add.append(s)

    print(f"📊 {len(to_add)} to add · {len(to_update)} to update")

    created, updated = [], []
    if to_add:
        created.extend(add_schemas(to_add))
    if to_update:
        updated.extend(update_schemas(to_update))

    print("🎉 Schema pipeline complete")
    print(f"   • Schemas created: {len(created)}")
    print(f"   • Schemas updated: {len(updated)}")



# --------------------------------------------------------------------------- #
# Helper utilities                                                             #
# --------------------------------------------------------------------------- #

def _normalize_video_data(videos: list[Any]) -> Dict[str, List[Dict]]:
    """Convert both list styles into {video_uid: [question_cfg, ...]}"""
    if not isinstance(videos, list):
        raise TypeError("'videos' must be a list")
    out: Dict[str, List[Dict]] = {}
    for item in videos:
        if isinstance(item, str):
            out[item] = []
        elif isinstance(item, dict) and "video_uid" in item:
            q_cfgs: List[Dict] = []
            for q in item.get("questions", []):
                q_cfgs.append(
                    {
                        "question_text": q.get("question_text"),
                        "display_text": q.get("display_text") or q.get("custom_question"),
                        "option_map": q.get("custom_option") or q.get("option_map"),
                    }
                )
            out[item["video_uid"]] = q_cfgs
        else:
            raise ValueError(f"Invalid video entry: {item}")
    return out

# --------------------------------------------------------------------------- #
# Custom‑display synchroniser                                                  #
# --------------------------------------------------------------------------- #

@staticmethod
def _sync_custom_displays(project_id: int, videos: list[Any], sess) -> Dict[str, int]:
    """Create / update / remove / skip custom displays to match JSON spec."""
    stats = {"created": 0, "updated": 0, "removed": 0, "skipped": 0}

    # Get project info including schema
    project = ProjectService.get_project_by_id(project_id, sess)
    schema_id = project.schema_id
    
    # Early exit if schema doesn't support custom displays
    schema = SchemaService.get_schema_by_id(schema_id, sess)
    if not schema.has_custom_display:
        # Count all potential operations as skipped for reporting
        proj_q = ProjectService.get_project_questions(project_id, sess)
        proj_v = VideoService.get_project_videos(project_id, sess)
        stats["skipped"] = len(proj_q) * len(proj_v)
        return stats

    cfg = _normalize_video_data(videos)
    
    # Get project questions and videos using service methods
    proj_q = {q["id"]: q["text"] for q in ProjectService.get_project_questions(project_id, sess)}
    proj_v = {v["id"]: v["uid"] for v in VideoService.get_project_videos(project_id, sess)}

    # Calculate total operations for progress bar
    total_operations = len(proj_v) * len(proj_q)
    
    with tqdm(total=total_operations, desc="Syncing custom displays", unit="display") as pbar:
        for vid_id, uid in proj_v.items():
            json_q_cfg = {qc["question_text"]: qc for qc in cfg.get(uid, [])}

            for q_id, q_text in proj_q.items():
                # Get existing custom display
                db_rec = CustomDisplayService.get_custom_display(q_id, project_id, vid_id, sess)
                json_cfg = json_q_cfg.get(q_text)

                if db_rec and not json_cfg:
                    # Remove custom display if it exists but not in JSON
                    CustomDisplayService.remove_custom_display(project_id, vid_id, q_id, sess)
                    stats["removed"] += 1
                elif json_cfg:
                    # Check if we need to update or create
                    same_text = db_rec and db_rec.get("display_text") == json_cfg["display_text"]
                    same_map = db_rec and db_rec.get("display_values") == json_cfg["option_map"]
                    
                    if db_rec and same_text and same_map:
                        stats["skipped"] += 1
                    else:
                        # Verify before setting custom display
                        try:
                            CustomDisplayService.verify_set_custom_display(
                                project_id=project_id,
                                video_id=vid_id,
                                question_id=q_id,
                                custom_display_text=json_cfg["display_text"],
                                custom_option_display_map=json_cfg["option_map"],
                                session=sess
                            )
                            
                            # If verification passes, set the custom display
                            CustomDisplayService.set_custom_display(
                                project_id=project_id,
                                video_id=vid_id,
                                question_id=q_id,
                                custom_display_text=json_cfg["display_text"],
                                custom_option_display_map=json_cfg["option_map"],
                                session=sess
                            )
                            
                            if db_rec:
                                stats["updated"] += 1
                            else:
                                stats["created"] += 1
                                
                        except ValueError as e:
                            # Log the error but continue processing other displays
                            tqdm.write(f"⚠️  Warning: Failed to set custom display for question {q_id} on video {vid_id}: {e}")
                            stats["skipped"] += 1
                
                # Update progress bar
                pbar.update(1)
                pbar.set_postfix(created=stats["created"], updated=stats["updated"], 
                                removed=stats["removed"], skipped=stats["skipped"])
                        
    return stats

# --------------------------------------------------------------------------- #
# Creation logic                                                               #
# --------------------------------------------------------------------------- #

@staticmethod
def add_projects(projects: List[Dict]) -> List[Dict]:
    """Create brand‑new projects (verify → create → display sync)."""
    if not isinstance(projects, list):
        raise TypeError("projects must be list[dict]")

    output: List[Dict] = []
    with SessionLocal() as sess:
        # Check for duplicates
        duplicates: List[str] = []
        for cfg in projects:
            try:
                ProjectService.get_project_by_name(cfg["project_name"], sess)
                duplicates.append(cfg["project_name"])
            except ValueError:
                pass  # Project doesn't exist, which is good
                
        if duplicates:
            raise ValueError("Add aborted – already in DB: " + ", ".join(duplicates))

        # Prepare and create projects with progress bar
        with tqdm(total=len(projects), desc="Adding projects", unit="project") as pbar:
            for cfg in projects:
                project_name = cfg["project_name"]
                pbar.set_description(f"Adding project: {project_name}")
                
                # Get schema ID
                schema_id = SchemaService.get_schema_id_by_name(cfg["schema_name"], sess)
                
                # Get video IDs
                video_uids = list(_normalize_video_data(cfg["videos"]).keys())
                video_ids = ProjectService.get_video_ids_by_uids(video_uids, sess)
                
                # Verify creation parameters
                ProjectService.verify_create_project(project_name, schema_id, video_ids, sess)
                
                # Create the project
                ProjectService.create_project(project_name, schema_id, video_ids, sess)
                
                # Get the created project by name to get its ID
                proj = ProjectService.get_project_by_name(project_name, sess)
                
                # Handle archive status
                if cfg.get("is_archived", False):
                    ProjectService.archive_project(proj.id, sess)
                
                tqdm.write(f"✅ Created project '{project_name}' (ID: {proj.id})")
                
                # Sync custom displays
                stats = _sync_custom_displays(proj.id, cfg["videos"], sess)
                
                output.append({
                    "name": proj.name, 
                    "id": proj.id, 
                    **stats
                })
                
                pbar.update(1)

        sess.commit()
    return output

# --------------------------------------------------------------------------- #
# Sync logic (display overrides + archive toggle)                              #
# --------------------------------------------------------------------------- #

@staticmethod
def update_projects(projects: List[Dict]) -> List[Dict]:
    """Synchronise custom displays & archived flag for existing projects."""
    if not isinstance(projects, list):
        raise TypeError("projects must be list[dict]")

    output: List[Dict] = []
    with SessionLocal() as sess:
        # Validate all projects exist
        for cfg in projects:
            try:
                ProjectService.get_project_by_name(cfg["project_name"], sess)
            except ValueError:
                raise ValueError(f"Sync aborted – not found in DB: {cfg['project_name']}")

        # Process each project with progress bar
        with tqdm(total=len(projects), desc="Syncing projects", unit="project") as pbar:
            for cfg in projects:
                project_name = cfg["project_name"]
                pbar.set_description(f"Syncing project: {project_name}")
                
                proj = ProjectService.get_project_by_name(project_name, sess)
                
                # Handle archive flag (is_active has priority for backwards compatibility)
                if "is_active" in cfg:
                    cfg["is_archived"] = not cfg.pop("is_active")
                    
                if "is_archived" in cfg:
                    desired_archived = cfg["is_archived"]
                    if desired_archived != proj.is_archived:
                        if desired_archived:
                            ProjectService.archive_project(proj.id, sess)
                            tqdm.write(f"📦 Archived project '{project_name}'")
                        else:
                            # Unarchive by setting the flag directly
                            proj.is_archived = False
                            tqdm.write(f"📂 Unarchived project '{project_name}'")
                            
                # Sync custom displays
                stats = _sync_custom_displays(proj.id, cfg["videos"], sess)
                
                output.append({
                    "name": proj.name, 
                    "id": proj.id, 
                    **stats
                })
                
                tqdm.write(f"🔄 Synced project '{project_name}' - "
                          f"created: {stats['created']}, updated: {stats['updated']}, "
                          f"removed: {stats['removed']}, skipped: {stats['skipped']}")
                
                pbar.update(1)

        sess.commit()
    return output

# --------------------------------------------------------------------------- #
# Orchestrator                                                                 #
# --------------------------------------------------------------------------- #

@staticmethod
def sync_projects(*, projects_path: str | Path | None = None, projects_data: List[Dict] | None = None) -> None:
    """Add new projects or sync existing ones based on JSON input."""
    if projects_path is None and projects_data is None:
        raise ValueError("Provide either projects_path or projects_data")
        
    if projects_path:
        with open(projects_path, "r") as f:
            projects_data = json.load(f)
            
    if not isinstance(projects_data, list):
        raise TypeError("projects_data must be list[dict]")

    print("\n🚀 Starting project upload pipeline...")
    
    # Validate and normalize project data with progress bar
    processed: List[Dict] = []
    with tqdm(total=len(projects_data), desc="Validating projects", unit="project") as pbar:
        for idx, cfg in enumerate(projects_data, 1):
            # Validate required fields
            for key in ("project_name", "schema_name", "videos"):
                if key not in cfg:
                    raise ValueError(f"Entry #{idx}: missing '{key}'")
                    
            # Normalize is_active to is_archived
            if "is_active" in cfg:
                cfg["is_archived"] = not cfg.pop("is_active")
                
            processed.append(cfg)
            pbar.update(1)

    # Separate projects to add vs sync
    to_add, to_sync = [], []
    with SessionLocal() as sess:
        print("\n📊 Categorizing projects...")
        for cfg in tqdm(processed, desc="Checking existing projects", unit="project"):
            try:
                ProjectService.get_project_by_name(cfg["project_name"], sess)
                to_sync.append(cfg)  # exists → sync
            except ValueError:
                to_add.append(cfg)  # not found → add

    print(f"\n📈 Summary: {len(to_add)} projects to add, {len(to_sync)} projects to sync")

    # Process projects
    add_results = []
    sync_results = []
    
    if to_add:
        print(f"\n➕ Adding {len(to_add)} new projects...")
        add_results = add_projects(to_add)
            
    if to_sync:
        print(f"\n🔄 Syncing {len(to_sync)} existing projects...")
        sync_results = update_projects(to_sync)

    # Final summary
    print("\n🎉 Project pipeline complete!")
    print(f"✅ Added: {len(add_results)} projects")
    print(f"🔄 Synced: {len(sync_results)} projects")
    
    total_custom_displays = 0
    for result in add_results + sync_results:
        total_custom_displays += result['created'] + result['updated']
    
    if total_custom_displays > 0:
        print(f"🎨 Total custom displays processed: {total_custom_displays}")


def bulk_assign_users(assignment_path: str = None, assignments_data: list[dict] = None) -> None:
    """Bulk assign users to projects with roles."""
    
    # Load and validate input
    if assignment_path is None and assignments_data is None:
        raise ValueError("Either assignment_path or assignments_data must be provided")
    
    if assignment_path:
        with open(assignment_path, 'r') as f:
            assignments_data = json.load(f)
    
    if not isinstance(assignments_data, list):
        raise TypeError("assignments_data must be a list of dictionaries")

    # Process and validate assignments
    processed = []
    seen_pairs = set()
    valid_roles = {'annotator', 'reviewer', 'admin', 'model'}
    
    with SessionLocal() as session:
        for idx, assignment in enumerate(assignments_data, 1):
            # Validate required fields
            if 'user_email' in assignment and 'user_name' not in assignment:
                try:
                    user = AuthService.get_user_by_email(assignment['user_email'], session)
                    assignment['user_name'] = user.user_id_str
                except ValueError:
                    raise ValueError(f"#{idx}: User email '{assignment['user_email']}' not found")
            
            required = {'user_name', 'project_name', 'role'}
            if missing := required - set(assignment.keys()):
                raise ValueError(f"#{idx}: Missing fields: {', '.join(missing)}")
            
            # Validate role and duplicates
            if assignment['role'] not in valid_roles:
                raise ValueError(f"#{idx}: Invalid role '{assignment['role']}'")
            
            pair = (assignment['user_name'], assignment['project_name'])
            if pair in seen_pairs:
                raise ValueError(f"#{idx}: Duplicate assignment {pair[0]} -> {pair[1]}")
            seen_pairs.add(pair)
            
            # Validate entities exist and aren't archived
            try:
                user = AuthService.get_user_by_name(assignment['user_name'], session)
                project = ProjectService.get_project_by_name(assignment['project_name'], session)
                
                if user.is_archived:
                    raise ValueError(f"#{idx}: User '{assignment['user_name']}' is archived")
                if project.is_archived:
                    raise ValueError(f"#{idx}: Project '{assignment['project_name']}' is archived")
                    
                processed.append({
                    **assignment,
                    'is_active': assignment.get('is_active', True),
                    'user_id': user.id,
                    'project_id': project.id
                })
                
            except ValueError as e:
                if "not found" in str(e).lower():
                    raise ValueError(f"#{idx}: {str(e)}")
                raise

    # Apply assignments
    created = updated = removed = 0
    
    with SessionLocal() as session:
        try:
            for assignment in processed:
                # Check existing assignment using service method
                if assignment['role'] == 'model':
                    existing = False
                else:
                    user_projects = AuthService.get_user_projects_by_role(assignment['user_id'], session)
                    existing = any(
                        assignment['project_id'] in [p['id'] for p in projects] 
                        for projects in user_projects.values()
                    )
                
                if assignment['is_active']:
                    ProjectService.add_user_to_project(
                        project_id=assignment['project_id'],
                        user_id=assignment['user_id'],
                        role=assignment['role'],
                        session=session,
                        user_weight=assignment.get('user_weight')
                    )
                    if existing:
                        updated += 1
                    else:
                        created += 1
                elif existing:
                    AuthService.archive_user_from_project(
                        assignment['user_id'], assignment['project_id'], session
                    )
                    removed += 1
            
            session.commit()
            print(f"✅ Completed: {created} created, {updated} updated, {removed} removed")
            
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Assignment failed: {e}")


def upload_annotations(annotations_path: str = None, annotations_data: list[dict] = None) -> None:
    """Upload annotations from JSON file or data list with optimized connection handling."""
    from tqdm import tqdm
    
    # Load data
    if annotations_path is None and annotations_data is None:
        raise ValueError("Either annotations_path or annotations_data must be provided")
    
    if annotations_path is not None:
        with open(annotations_path, 'r') as f:
            annotations_data = json.load(f)
    
    if not isinstance(annotations_data, list):
        raise TypeError("annotations_data must be a list of dictionaries")
    
    if not annotations_data:
        print("ℹ️  No annotations to upload")
        return
    
    # Process annotations in single session to avoid connection exhaustion
    validated_entries = []
    skipped_entries = []
    
    print("🔍 Validating and uploading annotations...")
    with SessionLocal() as session:
        try:
            # Validation phase with progress bar
            for idx, annotation in enumerate(tqdm(annotations_data, desc="Validating", unit="items"), 1):
                try:
                    # Resolve IDs
                    video_uid = annotation.get("video_uid", "").split("/")[-1]
                    video = VideoService.get_video_by_uid(video_uid, session)
                    project = ProjectService.get_project_by_name(annotation["project_name"], session)
                    user = AuthService.get_user_by_name(annotation["user_name"], session)
                    group = QuestionGroupService.get_group_by_name(annotation["question_group_title"], session)
                    # Verify submission
                    AnnotatorService.verify_submit_answer_to_question_group(
                        video_id=video.id,
                        project_id=project.id,
                        user_id=user.id,
                        question_group_id=group.id,
                        answers=annotation["answers"],
                        session=session,
                        confidence_scores=annotation.get("confidence_scores"),
                        notes=annotation.get("notes")
                    )
                    # Check if answers already exist
                    existing = AnnotatorService.get_user_answers_for_question_group(
                        video_id=video.id,
                        project_id=project.id,
                        user_id=user.id,
                        question_group_id=group.id,
                        session=session
                    )
                    
                    # Determine if update needed
                    needs_update = False
                    for q_text, answer in annotation["answers"].items():
                        if q_text not in existing or existing[q_text] != answer:
                            needs_update = True
                            break
                    
                    if not needs_update:
                        skipped_entries.append({
                            "video_uid": video_uid,
                            "user_name": annotation["user_name"],
                            "group": annotation["question_group_title"]
                        })
                    else:
                        validated_entries.append({
                            "video_id": video.id,
                            "project_id": project.id,
                            "user_id": user.id,
                            "group_id": group.id,
                            "answers": annotation["answers"],
                            "confidence_scores": annotation.get("confidence_scores"),
                            "notes": annotation.get("notes"),
                            "video_uid": video_uid,
                            "user_name": annotation["user_name"],
                            "group_title": annotation["question_group_title"]
                        })
                        
                except Exception as e:
                    raise ValueError(f"[Row {idx}] {annotation.get('video_uid')} | "
                                   f"{annotation.get('user_name')} | "
                                   f"{annotation.get('question_group_title')}: {e}")
            
            print(f"✅ Validation passed: {len(validated_entries)} to upload, {len(skipped_entries)} skipped")
            
            # Upload validated entries in same session with progress bar
            if validated_entries:
                print("📤 Uploading annotations...")
                for entry in tqdm(validated_entries, desc="Uploading", unit="groups"):
                    AnnotatorService.submit_answer_to_question_group(
                        video_id=entry["video_id"],
                        project_id=entry["project_id"],
                        user_id=entry["user_id"],
                        question_group_id=entry["group_id"],
                        answers=entry["answers"],
                        session=session,
                        confidence_scores=entry["confidence_scores"],
                        notes=entry["notes"]
                    )
                
                session.commit()
                print(f"🎉 Successfully uploaded {len(validated_entries)} annotation groups!")
                
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Upload failed: {e}")


def upload_reviews(reviews_path: str = None, reviews_data: list[dict] = None) -> None:
    """Upload ground truth reviews from JSON file or data list.
    
    Args:
        reviews_path: Path to reviews JSON file
        reviews_data: List of review dictionaries
        
    JSON format: Same as annotations but with is_ground_truth: true
    """
    # Load data
    if reviews_path is None and reviews_data is None:
        raise ValueError("Either reviews_path or reviews_data must be provided")
    
    if reviews_path is not None:
        with open(reviews_path, 'r') as f:
            reviews_data = json.load(f)
    
    if not isinstance(reviews_data, list):
        raise TypeError("reviews_data must be a list of dictionaries")
    
    if not reviews_data:
        print("ℹ️  No reviews to upload")
        return
    
    # Validate ground truth flag
    for idx, review in enumerate(reviews_data, 1):
        if not review.get("is_ground_truth", False):
            raise ValueError(f"[Row {idx}] is_ground_truth must be True for reviews")
    
    # Process reviews
    validated_entries = []
    skipped_entries = []
    
    print("🔍 Validating reviews...")
    with SessionLocal() as session:
        for idx, review in enumerate(tqdm(reviews_data, desc="Validating"), 1):
            try:
                # Resolve IDs
                video_uid = review.get("video_uid", "").split("/")[-1]
                video = VideoService.get_video_by_uid(video_uid, session)
                project = ProjectService.get_project_by_name(review["project_name"], session)
                reviewer = AuthService.get_user_by_name(review["user_name"], session)
                group = QuestionGroupService.get_group_by_name(review["question_group_title"], session)
                
                # Verify submission
                GroundTruthService.verify_submit_ground_truth_to_question_group(
                    video_id=video.id,
                    project_id=project.id,
                    reviewer_id=reviewer.id,
                    question_group_id=group.id,
                    answers=review["answers"],
                    session=session,
                    confidence_scores=review.get("confidence_scores"),
                    notes=review.get("notes")
                )
                
                # Check existing ground truth
                existing = GroundTruthService.get_ground_truth_dict_for_question_group(
                    video_id=video.id,
                    project_id=project.id,
                    question_group_id=group.id,
                    session=session
                )
                
                # Determine what needs updating
                to_upload = {}
                for q_text, answer in review["answers"].items():
                    if q_text not in existing or existing[q_text] != answer:
                        to_upload[q_text] = answer
                
                if not to_upload:
                    skipped_entries.append({
                        "video_uid": video_uid,
                        "reviewer": review["user_name"]
                    })
                else:
                    validated_entries.append({
                        "video_id": video.id,
                        "project_id": project.id,
                        "reviewer_id": reviewer.id,
                        "group_id": group.id,
                        "answers": to_upload,
                        "confidence_scores": review.get("confidence_scores"),
                        "notes": review.get("notes"),
                        "video_uid": video_uid,
                        "reviewer_name": review["user_name"]
                    })
                    
            except Exception as e:
                raise ValueError(f"[Row {idx}] {review.get('video_uid')} | "
                               f"reviewer:{review.get('user_name')}: {e}")
    
    print(f"✅ Validation passed: {len(validated_entries)} to upload, {len(skipped_entries)} skipped")
    
    # Upload validated entries
    if validated_entries:
        print("\n📤 Uploading reviews...")
        with SessionLocal() as session:
            try:
                for entry in tqdm(validated_entries, desc="Uploading"):
                    GroundTruthService.submit_ground_truth_to_question_group(
                        video_id=entry["video_id"],
                        project_id=entry["project_id"],
                        reviewer_id=entry["reviewer_id"],
                        question_group_id=entry["group_id"],
                        answers=entry["answers"],
                        session=session,
                        confidence_scores=entry["confidence_scores"],
                        notes=entry["notes"]
                    )
                
                session.commit()
                print(f"🎉 Successfully uploaded {len(validated_entries)} reviews!")
            except Exception as e:
                session.rollback()
                raise RuntimeError(f"Upload failed: {e}")


def batch_upload_annotations(annotations_folder: str = None, 
                           annotations_data: list[list[dict]] = None, 
                           max_workers: int = 5) -> None:
    """Batch upload annotations from folder or data list."""
    import concurrent.futures
    import glob
    
    if annotations_folder:
        json_files = glob.glob(f"{annotations_folder}/*.json")
        
        def process_file(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
            upload_annotations(annotations_data=data)
            return filepath
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_file, f): f for f in json_files}
            
            for future in concurrent.futures.as_completed(futures):
                filepath = futures[future]
                try:
                    future.result()
                    print(f"✓ Processed {filepath}")
                except Exception as e:
                    print(f"✗ Failed {filepath}: {e}")
    
    elif annotations_data:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(upload_annotations, annotations_data=data) 
                      for data in annotations_data]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    future.result()
                    print(f"✓ Processed batch {i+1}")
                except Exception as e:
                    print(f"✗ Failed batch {i+1}: {e}")


def batch_upload_reviews(reviews_folder: str = None, 
                        reviews_data: list[list[dict]] = None, 
                        max_workers: int = 3) -> None:
    """Batch upload reviews from folder or data list."""
    import concurrent.futures
    import glob
    
    if reviews_folder:
        json_files = glob.glob(f"{reviews_folder}/*.json")
        
        def process_file(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
            upload_reviews(reviews_data=data)
            return filepath
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_file, f): f for f in json_files}
            
            for future in concurrent.futures.as_completed(futures):
                filepath = futures[future]
                try:
                    future.result()
                    print(f"✓ Processed {filepath}")
                except Exception as e:
                    print(f"✗ Failed {filepath}: {e}")
    
    elif reviews_data:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(upload_reviews, reviews_data=data) 
                      for data in reviews_data]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    future.result()
                    print(f"✓ Processed batch {i+1}")
                except Exception as e:
                    print(f"✗ Failed batch {i+1}: {e}")