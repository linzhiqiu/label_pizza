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
    GroundTruthService
)
from label_pizza.db import SessionLocal, engine # Must have been initialized by init_database() before importing this file
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Any, Set, Tuple
import pandas as pd


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

def upload_schemas(schemas_path: str) -> dict:
    """
    Upload (create only) schemas from a JSON file.
    This function only creates new schemas - existing schemas cannot be updated.
    All schemas must be valid or NOTHING gets committed to the database.
    
    Parameters
    ----------
    schemas_file : str
        Path to JSON file containing schema definitions
        
    Returns
    -------
    dict
        Summary: {
            "created": [{"name": str, "id": int}, ...],
        }
    """
    import json
    import os
    
    # Load and validate JSON
    if not os.path.exists(schemas_path):
        raise ValueError(f"File does not exist: {schemas_path}")
    
    with open(schemas_path, 'r') as f:
        schemas_data = json.load(f)
    
    if not isinstance(schemas_data, list) or not schemas_data:
        raise ValueError("JSON must contain a non-empty list of schemas")
    
    print(f"📁 Found {len(schemas_data)} schemas to process")
    
    # Validate all schemas before any database operations
    with SessionLocal() as session:
        try:
            for schema_data in schemas_data:
                # Validate structure
                if not isinstance(schema_data, dict):
                    raise ValueError("Each schema must be a dictionary")
                
                schema_name = schema_data.get('schema_name')
                group_names = schema_data.get('question_group_names', [])
                
                if not schema_name or not isinstance(schema_name, str):
                    raise ValueError("Each schema must have a 'schema_name' string")
                
                if not isinstance(group_names, list) or not group_names:
                    raise ValueError(f"Schema '{schema_name}' must have a non-empty list of question_group_names")
                
                # Check if schema already exists
                try:
                    SchemaService.get_schema_by_name(schema_name, session)
                    raise ValueError(f"Schema '{schema_name}' already exists. Schemas cannot be updated.")
                except ValueError as e:
                    if "not found" not in str(e):
                        raise
                
                # Validate all question groups exist
                for group_name in group_names:
                    try:
                        group = QuestionGroupService.get_group_by_name(group_name, session)
                        if group.is_archived:
                            raise ValueError(f"Question group '{group_name}' is archived")
                    except ValueError as e:
                        if "not found" in str(e):
                            raise ValueError(f"Question group '{group_name}' not found")
                        raise
            
            print("✅ All validations passed")
            
            # Create all schemas in a single transaction
            created_schemas = []
            
            for schema_data in schemas_data:
                schema_name = schema_data['schema_name']
                group_names = schema_data['question_group_names']
                
                # Get group IDs
                group_ids = []
                for group_name in group_names:
                    group = QuestionGroupService.get_group_by_name(group_name, session)
                    group_ids.append(group.id)
                
                # Create schema
                schema = SchemaService.create_schema(
                    name=schema_name,
                    question_group_ids=group_ids,
                    session=session
                )
                
                created_schemas.append({"name": schema_name, "id": schema.id})
                print(f"✓ Created schema: {schema_name}")
            
            # Commit only if all succeed
            session.commit()
            print(f"\n✅ Successfully created {len(created_schemas)} schemas")
            
            return {"created": created_schemas}
            
        except Exception as e:
            session.rollback()
            print(f"\n❌ Error: {str(e)}")
            print("⛔ All changes rolled back")
            raise







def upload_question_groups(question_groups_folder: str) -> dict:
    """
    Upload (create or update) question groups from a folder of JSON files.
    
    CRITICAL BEHAVIOR:
    - Validates ALL files AND database state before ANY operations
    - NO database changes unless ENTIRE folder is valid
    - Rolls back ALL changes if ANY error occurs
    - For existing groups: only updates display_title (title is read-only identifier)
    
    Parameters
    ----------
    question_groups_folder : str
        Path to folder containing question group JSON files
        
    Returns
    -------
    dict
        Summary: {
            "created": [{"title": str, "id": int}, ...],
            "updated": [{"title": str, "id": int}, ...],
            "questions_created": [str, ...],
            "questions_found": [str, ...],
            "validation_errors": [str, ...]
        }
    """
    import glob
    import json
    import os
    from tqdm import tqdm
    
    # Initialize result tracking
    validation_errors = []
    
    # Step 1: Check if folder exists
    if not os.path.exists(question_groups_folder):
        error_msg = f"❌ ERROR: Folder does not exist: {question_groups_folder}"
        print(error_msg)
        raise ValueError(error_msg)
    
    if not os.path.isdir(question_groups_folder):
        error_msg = f"❌ ERROR: Path is not a directory: {question_groups_folder}"
        print(error_msg)
        raise ValueError(error_msg)
    
    # Step 2: Find all JSON files
    group_paths = glob.glob(f"{question_groups_folder}/*.json")
    
    if not group_paths:
        error_msg = f"❌ ERROR: No JSON files found in {question_groups_folder}"
        print(error_msg)
        raise ValueError(error_msg)
    
    print(f"📁 Found {len(group_paths)} JSON files to process")
    
    # Step 3: COMPLETE VALIDATION - Both JSON structure AND database state
    question_groups_data = []
    
    print("\n🔍 Phase 1: Validating JSON structure...")
    for i, group_path in enumerate(group_paths, 1):
        filename = os.path.basename(group_path)
        try:
            print(f"  [{i}/{len(group_paths)}] Checking {filename}...", end="")
            
            # Try to load JSON
            with open(group_path, 'r') as f:
                data = json.load(f)
            
            # Basic validation of required fields
            if not isinstance(data, dict):
                validation_errors.append(f"{filename}: Not a valid JSON object (must be a dictionary)")
                print(" ❌ INVALID")
                continue
                
            if 'title' not in data:
                validation_errors.append(f"{filename}: Missing required field 'title' (used for group identification)")
                print(" ❌ MISSING TITLE")
                continue
                
            if 'description' not in data:
                validation_errors.append(f"{filename}: Missing required field 'description'")
                print(" ❌ MISSING DESCRIPTION")
                continue
            
            # For new groups, display_title is optional (defaults to title)
            # For existing groups, display_title can be updated
            if 'display_title' not in data:
                data['display_title'] = data['title']  # Default to title if not provided
            
            if 'questions' not in data or not isinstance(data['questions'], list):
                validation_errors.append(f"{filename}: Missing or invalid 'questions' field (must be a list)")
                print(" ❌ INVALID QUESTIONS")
                continue
            
            # Validate each question structure
            for j, question in enumerate(data['questions']):
                if not isinstance(question, dict):
                    validation_errors.append(f"{filename}: Question {j+1} is not a dictionary")
                    continue
                if 'text' not in question:
                    validation_errors.append(f"{filename}: Question {j+1} missing 'text' field")
                if 'qtype' not in question:
                    validation_errors.append(f"{filename}: Question {j+1} missing 'qtype' field")
            
            if not validation_errors or not any(filename in err for err in validation_errors):
                question_groups_data.append((filename, data))
                print(" ✓")
            
        except json.JSONDecodeError as e:
            validation_errors.append(f"{filename}: Invalid JSON format - {str(e)}")
            print(f" ❌ JSON ERROR")
        except Exception as e:
            validation_errors.append(f"{filename}: Unexpected error - {str(e)}")
            print(f" ❌ ERROR: {str(e)}")
    
    # Stop if JSON validation failed
    if validation_errors:
        print("\n❌ JSON VALIDATION FAILED!")
        for error in validation_errors:
            print(f"   • {error}")
        raise ValueError(f"JSON validation failed with {len(validation_errors)} errors.")
    
    print(f"\n✅ JSON structure validated for all {len(question_groups_data)} files")
    
    # Step 4: CRITICAL - Validate ALL database states BEFORE any modifications
    print("\n🔍 Phase 2: Validating database state (READ-ONLY check)...")
    
    with SessionLocal() as session:
        try:
            for i, (filename, group_data) in enumerate(question_groups_data, 1):
                title = group_data['title']  # Use title as identifier
                print(f"  [{i}/{len(question_groups_data)}] Checking '{title}' in database...", end="")
                
                try:
                    # Check if group exists (READ ONLY - no modifications!)
                    existing_group = QuestionGroupService.get_group_by_name(title, session)
                    print(" ✓ EXISTS (will update display_title only)")
                except ValueError as e:
                    if "not found" in str(e):
                        print(" ✓ NOT FOUND (will create)")
                        # This is OK - we'll create it later
                    else:
                        validation_errors.append(f"{filename}: Database error checking '{title}': {str(e)}")
                        print(" ❌ ERROR")
                except Exception as e:
                    validation_errors.append(f"{filename}: Unexpected database error for '{title}': {str(e)}")
                    print(" ❌ UNEXPECTED ERROR")
            
            # No commit needed - we only did read operations
        except Exception as e:
            validation_errors.append(f"Database connection error: {str(e)}")
    
    # Step 5: STOP if any database validation errors
    if validation_errors:
        print("\n❌ DATABASE VALIDATION FAILED!")
        for error in validation_errors:
            print(f"   • {error}")
        print("\n⛔ STOPPING: No modifications made to the database.")
        raise ValueError(f"Database validation failed with {len(validation_errors)} errors.")
    
    print("\n✅ All validations passed! Safe to proceed with database updates.")
    
    # Step 6: NOW we can safely process ALL files
    created_groups = []
    updated_groups = []
    questions_created = []
    questions_found = []
    
    print("\n📤 Phase 3: Updating database (ALL or NOTHING)...")
    
    with SessionLocal() as session:
        try:
            # Process each question group
            for filename, group_data in tqdm(question_groups_data, desc="Processing groups"):
                title = group_data['title']  # Use title as identifier
                
                try:
                    # Check if group exists
                    existing_group = QuestionGroupService.get_group_by_name(title, session)
                    
                    # Group exists - update it (only display_title and other metadata)
                    group_id = update_existing_question_group(group_data, existing_group, session)
                    updated_groups.append({"title": title, "id": group_id})
                    
                except ValueError as e:
                    if "not found" in str(e):
                        # Group doesn't exist - CREATE it
                        group_id = create_new_question_group(group_data, session)
                        created_groups.append({"title": title, "id": group_id})
                    else:
                        raise
                
                # Track questions
                for question_data in group_data.get("questions", []):
                    try:
                        QuestionService.get_question_by_text(question_data["text"], session)
                        if question_data["text"] not in questions_found:
                            questions_found.append(question_data["text"])
                    except ValueError as e:
                        if "not found" in str(e) and question_data["text"] not in questions_created:
                            questions_created.append(question_data["text"])
            
            # CRITICAL: Commit only after ALL groups processed successfully
            print("\n✅ All groups processed successfully. Committing changes...")
            session.commit()
            print("✅ DATABASE COMMIT SUCCESSFUL!")
            
        except Exception as e:
            # Rollback if ANYTHING goes wrong
            session.rollback()
            print("\n❌ ERROR during processing! Rolling back ALL changes...")
            print(f"   Error: {str(e)}")
            print("\n⛔ ROLLBACK COMPLETE: Database unchanged.")
            raise
    
    # Final summary
    print("\n📊 Upload Complete:")
    print(f"   • Groups created: {len(created_groups)}")
    print(f"   • Groups updated: {len(updated_groups)}")
    print(f"   • Questions found: {len(questions_found)}")
    print(f"   • New questions identified: {len(questions_created)}")
    
    return {
        "created": created_groups,
        "updated": updated_groups,
        "questions_created": questions_created,
        "questions_found": questions_found,
        "validation_errors": []  # Empty if we got here
    }


def create_new_question_group(group_data: dict, session) -> int:
    """
    Create a new question group along with its embedded questions.
    
    Parameters
    ----------
    group_data : dict
        Question group data containing title, description, display_title, questions, etc.
    session : Session
        Database session
        
    Returns
    -------
    int
        ID of the created question group
    """
    if not isinstance(group_data, dict):
        raise TypeError("group_data must be a dictionary")

    # First, ensure all questions exist (create if missing)
    question_ids = []
    for question_data in group_data.get("questions", []):
        try:
            # Try to get existing question
            existing_question = QuestionService.get_question_by_text(
                question_data["text"], session
            )
            question_ids.append(existing_question["id"])
        except ValueError as e:
            if "not found" in str(e):
                # Create the question
                new_question = QuestionService.add_question(
                    text=question_data["text"],
                    qtype=question_data["qtype"],
                    options=question_data.get("options"),
                    default=question_data.get("default_option"),
                    display_values=question_data.get("display_values"),
                    display_text=question_data.get("display_text"),
                    option_weights=question_data.get("option_weights"),
                    session=session,
                )
                question_ids.append(new_question.id)
            else:
                raise

    # Create the question group
    # Note: For new groups, we use display_title if provided, otherwise fall back to title
    display_title = group_data.get("display_title", group_data["title"])
    
    qgroup = QuestionGroupService.create_group(
        title=group_data["title"],  # This becomes the permanent identifier
        description=group_data["description"],
        display_title=display_title,  # This can be updated later
        is_reusable=group_data.get("is_reusable", True),
        question_ids=question_ids,
        verification_function=group_data.get("verification_function"),
        is_auto_submit=group_data.get("is_auto_submit", False),
        session=session,
    )
    
    return qgroup.id


def update_existing_question_group(group_data: dict, existing_group, session) -> int:
    """
    Update an existing question group with new data.
    IMPORTANT: 
    - The 'title' field is READ-ONLY and used only for identification
    - Only 'display_title' and other metadata can be updated via edit_group()
    - The database title field remains unchanged
    - Questions in the group are NOT updated
    
    Parameters
    ----------
    group_data : dict
        New question group data (title used for identification only)
    existing_group : QuestionGroup
        Existing question group object
    session : Session
        Database session
        
    Returns
    -------
    int
        ID of the updated question group
    """
    if not isinstance(group_data, dict):
        raise TypeError("group_data must be a dictionary")

    # CRITICAL: title is READ-ONLY for existing groups
    # Only update display_title and other metadata
    display_title = group_data.get("display_title", group_data["title"])
    
    # Update ONLY the question group metadata - NO question changes
    # Title remains unchanged (it's the permanent identifier in the database)
    QuestionGroupService.edit_group(
        group_id=existing_group.id,
        new_display_title=display_title,  # This can be updated
        new_description=group_data["description"],
        is_reusable=group_data.get("is_reusable", True),
        verification_function=group_data.get("verification_function"),
        is_auto_submit=group_data.get("is_auto_submit", False),
        session=session,
    )
        
    return existing_group.id








# ---------------------------------------------------------------------------
# Add & Update Users
# ---------------------------------------------------------------------------
def add_users(users_data: list[dict]) -> None:
    """
    Add new users from a list of dictionaries.
    
    Args:
        users_data: List of user dictionaries to add
        
    JSON format for each user:
        {
            "user_id": "alice",
            "email": "alice@example.com", 
            "password": "alicepassword",
            "user_type": "human"
        }
        
    Raises:
        ValueError: If any user already exists or validation fails
        RuntimeError: If database commit fails
    """
    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list of dictionaries")
    
    with SessionLocal() as session:
        existing_users = AuthService.get_all_users(session)
        existing_user_ids = set(existing_users['User ID'].tolist())
        existing_emails = set(existing_users['Email'].tolist()) if 'Email' in existing_users.columns else set()
        
        # Check for existing users and collect errors
        existing_user_errors = []
        new_users = []
        
        for user in users_data:
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            
            # Check if either user_id or email already exists
            if user_id in existing_user_ids or email in existing_emails:
                existing_user_errors.append(f"{user_id} (email: {email})")
            else:
                new_users.append(user)
        
        if existing_user_errors:
            raise ValueError(
                f"Cannot add users - the following users already exist: {', '.join(existing_user_errors)}"
            )
        
        if not new_users:
            print("ℹ️  No new users to add - all users already exist")
            return
        
        # Add all new users
        for user in tqdm(new_users, desc="Adding users", unit="user"):
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            password = user['password']
            user_type = user.get('user_type', 'human')
            
            try:
                AuthService.create_user(
                    user_id=user_id,
                    email=email,
                    password_hash=password,
                    user_type=user_type,
                    session=session
                )
                print(f"✓ Added new user: {user_id} ({email})")
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to create user {user_id}: {e}") from None
        
        try:
            session.commit()
            print(f"✔ Successfully added {len(new_users)} new users!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def update_users(users_data: list[dict]) -> None:
    """
    Update existing users from a list of dictionaries.
    Matches users by either user_id OR email.
    
    Args:
        users_data: List of user dictionaries to update
        
    JSON format for each user:
        {
            "user_id": "alice",
            "email": "alice@example.com", 
            "password": "alicepassword",
            "user_type": "human"
        }
        
    Raises:
        ValueError: If any user doesn't exist or validation fails
        RuntimeError: If database commit fails
    """
    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list of dictionaries")
    
    with SessionLocal() as session:
        existing_users = AuthService.get_all_users(session)
        
        # Create lookup maps for both user_id and email
        existing_user_map = {row['User ID']: row for _, row in existing_users.iterrows()}
        existing_email_map = {}
        if 'Email' in existing_users.columns:
            existing_email_map = {row['Email']: row for _, row in existing_users.iterrows() if pd.notna(row['Email'])}
        
        # Check for non-existing users and collect errors
        non_existing_user_errors = []
        users_to_update = []
        
        for user in users_data:
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            
            # Find existing user by either user_id or email
            existing_user_row = None
            match_type = None
            
            if user_id and user_id in existing_user_map:
                existing_user_row = existing_user_map[user_id]
                match_type = "user_id"
            elif email and email in existing_email_map:
                existing_user_row = existing_email_map[email]
                match_type = "email"
            
            if existing_user_row is not None:
                # Add the matched user info to the update data
                user['_existing_user_id'] = existing_user_row['User ID']
                user['_match_type'] = match_type
                users_to_update.append(user)
            else:
                non_existing_user_errors.append(f"{user_id} (email: {email})")
        
        if non_existing_user_errors:
            raise ValueError(
                f"Cannot update users - the following users don't exist: {', '.join(non_existing_user_errors)}"
            )
        
        if not users_to_update:
            print("ℹ️  No existing users to update")
            return
        
        # Pre-validate both email and user_id conflicts before updating
        conflicts = []
        for user in users_to_update:
            new_email = user.get('email', None)
            new_user_id = user.get('user_id', None)
            existing_user_id = user['_existing_user_id']
            match_type = user['_match_type']
            
            # Check email conflicts
            if new_email:
                if new_email in existing_email_map:
                    conflicting_user_id = existing_email_map[new_email]['User ID']
                    if conflicting_user_id != existing_user_id:
                        conflicts.append(f"Email conflict: User '{existing_user_id}' cannot use email '{new_email}' - already belongs to user '{conflicting_user_id}'")
            
            # Check user_id conflicts
            if new_user_id:
                if new_user_id in existing_user_map:
                    conflicting_user_id = existing_user_map[new_user_id]['User ID']
                    if conflicting_user_id != existing_user_id:
                        conflicts.append(f"User ID conflict: Cannot change user '{existing_user_id}' to user_id '{new_user_id}' - already belongs to different user")
        
        if conflicts:
            raise ValueError("Validation conflicts detected:\n" + "\n".join(conflicts))
        
        # Update all existing users
        for user in tqdm(users_to_update, desc="Updating users", unit="user"):
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            password = user.get('password', None)
            user_type = user.get('user_type', None)
            existing_user_id = user['_existing_user_id']
            match_type = user['_match_type']
            
            try:
                # Get the existing user's record by the matched user_id
                existing_user = AuthService.get_user_by_name(existing_user_id, session)
                
                # Update fields if provided and different
                if email is not None and email != existing_user.email:
                    AuthService.update_user_email(existing_user.id, email, session)
                
                if password is not None:
                    AuthService.update_user_password(existing_user.id, password, session)
                
                if user_type is not None and user_type != existing_user.user_type:
                    AuthService.update_user_role(existing_user.id, user_type, session)
                
                if user_id is not None and user_id != existing_user_id:
                    AuthService.update_user_id(existing_user.id, user_id, session)
                
                print(f"✓ Updated user: {existing_user_id} (matched by {match_type})")
                
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to update user {user_id}: {e}") from None
        
        try:
            session.commit()
            print(f"✔ Successfully updated {len(users_to_update)} users!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def upload_users(users_path: str = None, users_data: list[dict] = None) -> None:
    """
    Upload users from a JSON file or data list, handling both new and existing users.
    Users are matched by either user_id OR email - if either matches, the user is updated.
    
    Args:
        users_path: Path to the user JSON file
        users_data: List of user dictionaries
        
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
        
    Raises:
        ValueError: If any validation fails in add_users or update_users
        RuntimeError: If database operations fail
    """
    if users_path is None and users_data is None:
        raise ValueError("At least one parameter must be provided: users_path or users_data")
    
    if users_path is not None:
        with open(users_path, 'r') as f:
            users_data = json.load(f)
    
    if not isinstance(users_data, list):
        raise TypeError("users_data must be a list of dictionaries")
    
    # Split users into existing and new based on user_id OR email match
    with SessionLocal() as session:
        existing_users = AuthService.get_all_users(session)
        existing_user_ids = set(existing_users['User ID'].tolist())
        existing_emails = set()
        if 'Email' in existing_users.columns:
            existing_emails = set(existing_users['Email'].tolist())
            existing_emails.discard(None)  # Remove None values
            existing_emails = {email for email in existing_emails if pd.notna(email)}
        
        print(f"Found {len(existing_user_ids)} existing user IDs and {len(existing_emails)} existing emails")
        
        new_users = []
        existing_users_data = []
        
        for user in users_data:
            user_id = user.get('user_id', None)
            email = user.get('email', None)
            
            # Check if either user_id or email exists
            if (user_id and user_id in existing_user_ids) or (email and email in existing_emails):
                existing_users_data.append(user)
            else:
                new_users.append(user)
    
    # Process new users
    if new_users:
        print(f"📝 Processing {len(new_users)} new users...")
        add_users(new_users)
    else:
        print("ℹ️  No new users to add")
    
    # Process existing users
    if existing_users_data:
        print(f"🔄 Processing {len(existing_users_data)} existing users...")
        update_users(existing_users_data)
    else:
        print("ℹ️  No existing users to update")
    
    print("🎉 User upload completed!")

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
    """
    Create projects from JSON file or data list.
    
    CRITICAL BEHAVIOR:
    - Validates ALL projects before ANY database operations
    - Rolls back ALL changes if ANY error occurs
    """
    import json
    
    if projects_path is None and projects_data is None:
        raise ValueError("At least one parameter must be provided: projects_path or projects_data")
    
    # Load data from file if path provided
    if projects_path is not None:
        try:
            with open(projects_path, 'r') as f:
                projects_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to read project file: {str(e)}")
    
    if not isinstance(projects_data, list) or not projects_data:
        raise ValueError("Projects data must be a non-empty list")
    
    print(f"📁 Processing {len(projects_data)} projects...")
    
    # Validate all projects first (read-only database checks)
    validation_errors = []
    
    with SessionLocal() as session:
        for i, project_data in enumerate(projects_data, 1):
            project_name = project_data.get('project_name', f'Project #{i}')
            
            # Validate structure
            if not isinstance(project_data, dict):
                validation_errors.append(f"Project #{i}: Invalid structure")
                continue
                
            if not project_data.get('project_name'):
                validation_errors.append(f"Project #{i}: Missing project_name")
                continue
                
            if not project_data.get('schema_name'):
                validation_errors.append(f"Project '{project_name}': Missing schema_name")
                continue
                
            if not isinstance(project_data.get('videos'), list) or not project_data['videos']:
                validation_errors.append(f"Project '{project_name}': Invalid or empty videos list")
                continue
            
            # Check database dependencies
            try:
                # Check schema exists
                SchemaService.get_schema_id_by_name(project_data['schema_name'], session)
                
                # Check all videos exist
                missing_videos = []
                for uid in project_data['videos']:
                    if not VideoService.get_video_by_uid(uid, session):
                        missing_videos.append(uid)
                
                if missing_videos:
                    validation_errors.append(f"Project '{project_name}': Missing videos: {missing_videos}")
                    continue
                
                # Check if project already exists
                try:
                    ProjectService.get_project_by_name(project_name, session)
                    validation_errors.append(f"Project '{project_name}': Already exists")
                except ValueError as e:
                    if "not found" not in str(e):
                        validation_errors.append(f"Project '{project_name}': Database error: {str(e)}")
                
            except ValueError as e:
                if "not found" in str(e):
                    validation_errors.append(f"Project '{project_name}': Schema '{project_data['schema_name']}' not found")
                else:
                    validation_errors.append(f"Project '{project_name}': {str(e)}")
            except Exception as e:
                validation_errors.append(f"Project '{project_name}': Unexpected error: {str(e)}")
    
    # Stop if validation failed
    if validation_errors:
        print("❌ Validation failed:")
        for error in validation_errors:
            print(f"   • {error}")
        raise ValueError(f"Validation failed with {len(validation_errors)} errors")
    
    # Create all projects atomically
    print("✅ Validation passed. Creating projects...")
    created_projects = []
    
    with SessionLocal() as session:
        try:
            for project_data in projects_data:
                project_name = project_data['project_name']
                
                # Get dependencies
                schema_id = SchemaService.get_schema_id_by_name(project_data['schema_name'], session)
                video_ids = ProjectService.get_video_ids_by_uids(project_data['videos'], session)
                
                # Create project
                ProjectService.create_project(
                    name=project_name,
                    schema_id=schema_id,
                    video_ids=video_ids,
                    session=session
                )
                created_projects.append(project_name)
            
            session.commit()
            print(f"✅ Successfully created {len(created_projects)} projects")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Error during creation. Rolled back all changes: {str(e)}")
            raise ValueError(f"Project creation failed: {str(e)}")
    
    return None

def bulk_assign_users(assignment_path: str = None, assignments_data: list[dict] = None):
    """
    Bulk assign users to projects.
    - Validates all assignments before any database operations
    - Ensures unique <user, project> pairs
    - Rolls back all changes if any error occurs
    - No database operations occur if JSON processing fails
    """
    import json
    
    # Phase 0: Input validation and JSON processing
    try:
        if assignment_path is None and assignments_data is None:
            raise ValueError("At least one parameter must be provided: assignment_path or assignments_data")
        
        if assignment_path is not None:
            try:
                with open(assignment_path, 'r') as f:
                    assignments_data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                raise ValueError(f"Failed to read or parse JSON file: {str(e)}")
        
        if not isinstance(assignments_data, list) or not assignments_data:
            raise ValueError("Assignments data must be a non-empty list")
            
    except Exception as e:
        print(f"❌ JSON processing error: {str(e)}")
        raise  # Exit immediately without any database operations
    
    # Phase 1: Structure validation and duplicate checking (no DB operations)
    user_project_pairs = set()
    validation_errors = []
    
    for i, assignment in enumerate(assignments_data):
        if not isinstance(assignment, dict):
            validation_errors.append(f"Entry {i+1}: Invalid structure")
            continue
            
        required_fields = ["user_name", "project_name", "role"]
        for field in required_fields:
            if not assignment.get(field):
                validation_errors.append(f"Entry {i+1}: Missing {field}")
                break
        else:
            # Check for duplicates
            user_project_key = (assignment["user_name"], assignment["project_name"])
            if user_project_key in user_project_pairs:
                validation_errors.append(f"Entry {i+1}: Duplicate {assignment['user_name']} -> {assignment['project_name']}")
            else:
                user_project_pairs.add(user_project_key)
    
    # If structural errors exist, fail before any DB operations
    if validation_errors:
        for error in validation_errors:
            print(f"❌ {error}")
        raise ValueError(f"Validation failed with {len(validation_errors)} errors")
    
    # Phase 2: Database dependency validation (read-only operations)
    with SessionLocal() as session:
        for i, assignment in enumerate(assignments_data, 1):
            try:
                user = AuthService.get_user_by_name(assignment["user_name"], session)
                project = ProjectService.get_project_by_name(assignment["project_name"], session)
                
                if user.user_type == "admin":
                    validation_errors.append(f"Entry {i}: {assignment['user_name']} is admin, cannot assign non-admin role")
                elif user.user_type == "model" and assignment["role"] != "model":
                    validation_errors.append(f"Entry {i}: {assignment['user_name']} is model user, can only assign 'model' role")
                    
            except ValueError as e:
                if "not found" in str(e):
                    validation_errors.append(f"Entry {i}: {assignment['user_name']} or {assignment['project_name']} not found")
                else:
                    validation_errors.append(f"Entry {i}: {str(e)}")
            except Exception as e:
                # Catch any unexpected errors during validation
                validation_errors.append(f"Entry {i}: Unexpected error - {str(e)}")
    
    # If any validation errors exist, fail before write operations
    if validation_errors:
        for error in validation_errors:
            print(f"❌ {error}")
        raise ValueError(f"Validation failed with {len(validation_errors)} errors")
    
    # Phase 3: Process all assignments (write operations)
    created = []
    updated = []
    
    with SessionLocal() as session:
        try:
            for assignment in assignments_data:
                user = AuthService.get_user_by_name(assignment["user_name"], session)
                project = ProjectService.get_project_by_name(assignment["project_name"], session)
                
                # Check if user already has role in this project
                user_projects = AuthService.get_user_projects_by_role(user.id, session)
                current_role = None
                
                for role_type, projects in user_projects.items():
                    if any(proj["id"] == project.id for proj in projects):
                        current_role = role_type
                        break
                
                # Add/update assignment
                ProjectService.add_user_to_project(
                    project_id=project.id,
                    user_id=user.id, 
                    role=assignment["role"],
                    session=session
                )
                
                if current_role is None:
                    created.append(f"{assignment['user_name']} -> {assignment['project_name']} as {assignment['role']}")
                else:
                    updated.append(f"{assignment['user_name']} role updated from {current_role} to {assignment['role']} in {assignment['project_name']}")
            
            session.commit()
            
            for msg in created:
                print(f"✓ Assigned {msg}")
            for msg in updated:
                print(f"✓ Updated {msg}")
                
            print(f"✅ Completed: {len(created)} created, {len(updated)} updated")
            
        except Exception as e:
            session.rollback()
            raise ValueError(f"Assignment failed: {str(e)}")
    
    return None



# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────
def _resolve_ids(
    *,
    session: Session,
    question_group_title: str,
    user_name: str,
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, user_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    if user_name:
        user_id  = AuthService.get_user_by_name(user_name, session).id
    else:
        raise ValueError("user_name is required!")

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, user_id, group_id


def _verification_passes(
    *,
    session: Session,
    video_id: int,
    project_id: int,
    user_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & user existence / role checks
    AnnotatorService._validate_project_and_user(project_id, user_id, session)
    AnnotatorService._validate_user_role(user_id, project_id, "annotator", session)

    # 2. fetch group + questions
    group, questions = AnnotatorService._validate_question_group(group_id, session)

    # 3. build helper sets - only check required questions
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra = provided_q_texts - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 4. run optional verification hook
    AnnotatorService._run_verification(group, answers)

    # 5. validate each answer value (check if it's in options for single-choice)
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        AnnotatorService._validate_answer_value(q_lookup[q_text], answers[q_text])


# Cache helper
@lru_cache(maxsize=None)
def _legal_keys_for_group(group_id: int, session: Session) -> set[str]:
    """Return the set of Question.text keys that live in <group_id>."""
    qs = QuestionGroupService.get_group_questions(group_id, session)
    return {row["Text"] for _, row in qs.iterrows()}


def upload_annotations(rows: List[Dict[str, Any]]) -> None:
    """
    Upload annotations from JSON data with strict validation.
    
    Args:
        rows: List of annotation dictionaries
        
    Raises:
        ValueError: If any validation fails
        RuntimeError: If database operations fail
    """
    if not isinstance(rows, list):
        raise TypeError("rows must be a list of dictionaries")
    
    if not rows:
        print("ℹ️  No annotations to upload")
        return
    
    # Phase 1: Validate all entries first (fail-fast)
    print("🔍 Validating all annotations...")
    validated_entries = []
    skipped_entries = []
    
    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="Validating"), start=1):
            try:
                # Resolve IDs
                video_id, project_id, user_id, group_id = _resolve_ids(
                    session=session,
                    question_group_title=row["question_group_title"],
                    user_name=row["user_name"],
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # Get legal questions for this group
                legal_keys = _legal_keys_for_group(group_id, session)
                
                # Check for invalid questions first
                invalid_keys = set(row["answers"]) - legal_keys
                if invalid_keys:
                    raise ValueError(
                        f"Answers contain questions not in the question group '{row['question_group_title']}': "
                        f"{', '.join(invalid_keys)}"
                    )

                # Keep only valid questions
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # Verify answers (validates required fields, options, etc.)
                _verification_passes(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    user_id=user_id,
                    group_id=group_id,
                    answers=answers,
                )

                # Get existing answers for the ENTIRE group at once
                existing_group_answers = AnnotatorService.get_user_answers_for_question_group(
                    video_id=video_id,
                    project_id=project_id,
                    user_id=user_id,
                    question_group_id=group_id,
                    session=session
                )

                # Compare with new answers to determine what needs updating
                needs_update = False
                to_upload = {}
                
                for question_text, new_answer_value in answers.items():
                    existing_value = existing_group_answers.get(question_text)
                    
                    if existing_value is None:
                        # No existing answer for this question - need to create
                        to_upload[question_text] = new_answer_value
                        needs_update = True
                    elif existing_value != new_answer_value:
                        # Existing answer differs - need to update
                        to_upload[question_text] = new_answer_value
                        needs_update = True
                    # else: same value exists, skip this question

                # If nothing needs updating, skip this entry
                if not needs_update:
                    skipped_entries.append({
                        "video_uid": row.get("video_uid", "<unknown>"),
                        "user_name": row["user_name"],
                        "group": row["question_group_title"],
                        "reason": "All answers already exist with same values"
                    })
                    continue

                # Add to validated entries for upload
                validated_entries.append({
                    "video_id": video_id,
                    "project_id": project_id,
                    "user_id": user_id,
                    "group_id": group_id,
                    "answers": answers,  # Submit ALL answers for the group (service handles updates)
                    "confidence": row.get("confidence_scores") or {},
                    "notes": row.get("notes") or {},
                    "video_uid": row.get("video_uid", "<unknown>"),
                    "user_name": row["user_name"],
                    "group_title": row["question_group_title"],
                })

            except Exception as exc:
                raise ValueError(
                    f"[Row {idx}] {row.get('video_uid', 'unknown')} | "
                    f"{row.get('user_name', 'unknown')} | "
                    f"{row.get('question_group_title', 'unknown')}: {exc}"
                ) from None

    print(f"✅ Validation passed for {len(validated_entries)} annotation groups")
    if skipped_entries:
        print(f"⏭️  Skipped {len(skipped_entries)} annotation groups (no changes)")

    # Phase 2: Upload all validated entries
    if validated_entries:
        print("\n📤 Uploading annotations...")
        with SessionLocal() as session:
            try:
                for entry in tqdm(validated_entries, desc="Uploading"):
                    # Submit to the entire group - the service handles create/update logic
                    AnnotatorService.submit_answer_to_question_group(
                        video_id=entry["video_id"],
                        project_id=entry["project_id"],
                        user_id=entry["user_id"],
                        question_group_id=entry["group_id"],
                        answers=entry["answers"],
                        session=session,
                        confidence_scores=entry["confidence"],
                        notes=entry["notes"],
                    )
                    print(
                        f"✓ Uploaded: {entry['video_uid']} | "
                        f"{entry['user_name']} | "
                        f"{entry['group_title']}"
                    )
                
                session.commit()
                print(f"\n🎉 Successfully uploaded {len(validated_entries)} annotation groups!")
                
            except Exception as exc:
                session.rollback()
                raise RuntimeError(f"Upload failed: {exc}") from None
    else:
        print("ℹ️  No new annotations to upload")


def _resolve_ids_for_reviews(
    *,
    session: Session,
    question_group_title: str,
    user_name: str,
    video_ref: str,
    project_name: str,
) -> Tuple[int, int, int, int]:
    """Return (video_id, project_id, reviewer_id, group_id) or raise ValueError."""
    group_id = QuestionGroupService.get_group_by_name(question_group_title, session).id
    if user_name:
        reviewer_id = AuthService.get_user_by_name(user_name=user_name, session=session).id
    else:
        raise ValueError("user_name is required!")

    video_uid  = video_ref.split("/")[-1]
    video_id   = VideoService.get_video_by_uid(video_uid, session).id

    project_id = ProjectService.get_project_by_name(project_name, session).id
    return video_id, project_id, reviewer_id, group_id

def upload_reviews(
    rows: List[Dict[str, Any]],
) -> None:
    """
    Upload ground truth reviews from JSON data with strict validation.
    
    Args:
        rows: List of review dictionaries
        
    Raises:
        ValueError: If any validation fails or if answers contain questions not in the question group
        RuntimeError: If database operations fail
    """
    if not isinstance(rows, list):
        raise TypeError("rows must be a list of dictionaries")
    
    if not rows:
        print("ℹ️  No reviews to upload")
        return
    
    # Phase 1: Validate all entries first (fail-fast)
    print("🔍 Validating all reviews...")
    validated_entries = []
    skipped_entries = []
    
    with SessionLocal() as session:
        for idx, row in enumerate(tqdm(rows, desc="Validating reviews"), start=1):
            # Check ground truth flag
            if row.get("is_ground_truth") == False:
                raise ValueError(f"[Row {idx}] is_ground_truth must be True! Video: {row['video_uid']} is not ground truth.")
            
            try:
                # Resolve IDs
                video_id, project_id, reviewer_id, group_id = _resolve_ids_for_reviews(
                    session=session,
                    question_group_title=row["question_group_title"],
                    user_name=row.get("user_name", None),
                    video_ref=row.get("video_uid") or row["video_uid"],
                    project_name=row["project_name"],
                )

                # Keep only questions that exist in this group
                legal_keys = _legal_keys_for_group(group_id, session)
                answers = {k: v for k, v in row["answers"].items() if k in legal_keys}

                # Check for invalid questions and raise error
                invalid_keys = set(row["answers"]) - legal_keys
                if invalid_keys:
                    raise ValueError(
                        f"Answers contain questions not in the question group '{row['question_group_title']}': "
                        f"{', '.join(invalid_keys)}"
                    )

                # Verify answers
                _verification_passes_reviews(
                    session=session,
                    video_id=video_id,
                    project_id=project_id,
                    reviewer_id=reviewer_id,
                    group_id=group_id,
                    answers=answers,
                )

                # Check for existing ground truth and determine what to upload
                to_upload = {}
                all_skipped = True
                
                # Get ground truth DataFrame for this video and project
                gt_df = GroundTruthService.get_ground_truth(video_id, project_id, session)
                
                for question_text, answer_value in answers.items():
                    # Get question details using service
                    question_info = QuestionService.get_question_by_text(question_text, session)
                    question_id = question_info["id"]
                    
                    # Check if ground truth exists for this question
                    if not gt_df.empty and "Question ID" in gt_df.columns:
                        # Filter for this specific question
                        question_gt = gt_df[gt_df["Question ID"] == question_id]
                        
                        if not question_gt.empty:
                            existing_answer_value = question_gt.iloc[0]["Answer Value"]
                            if existing_answer_value == answer_value:
                                # Already exists with same value, skip
                                continue
                            else:
                                # Already exists but value is different, need to upload (update)
                                to_upload[question_text] = answer_value
                                all_skipped = False
                        else:
                            # Does not exist, need to upload
                            to_upload[question_text] = answer_value
                            all_skipped = False
                    else:
                        # No ground truth exists at all, need to upload
                        to_upload[question_text] = answer_value
                        all_skipped = False

                # If all answers were skipped, record skip information
                if all_skipped:
                    skipped_entries.append({
                        "video_uid": row.get("video_uid", "<unknown>"),
                        "user_name": row["user_name"],
                        "reason": "All ground truth already exist with same values"
                    })
                    continue

                # If there are answers to upload, add to validation list
                if to_upload:
                    validated_entries.append({
                        "video_id": video_id,
                        "project_id": project_id,
                        "reviewer_id": reviewer_id,
                        "group_id": group_id,
                        "answers": to_upload,  # Only include answers that need to be uploaded
                        "confidence": row.get("confidence_scores") or {},
                        "notes": row.get("notes") or {},
                        "video_uid": row.get("video_uid", "<unknown>"),
                        "user_name": row["user_name"],
                    })

            except Exception as exc:
                raise ValueError(f"[Row {idx}] {row.get('video_uid', 'unknown')} | reviewer:{row.get('user_name', 'unknown')}: {exc}") from None

    print(f"✅ Validation passed for {len(validated_entries)} reviews to upload")
    if skipped_entries:
        print(f"⏭️  Skipped {len(skipped_entries)} reviews (already exist with same values)")

    # Phase 2: Upload all validated entries
    if validated_entries:
        print("📤 Uploading reviews...")
        with SessionLocal() as session:
            try:
                for entry in tqdm(validated_entries, desc="Uploading reviews"):
                    GroundTruthService.submit_ground_truth_to_question_group(
                        video_id=entry["video_id"],
                        project_id=entry["project_id"],
                        reviewer_id=entry["reviewer_id"],
                        question_group_id=entry["group_id"],
                        answers=entry["answers"],
                        session=session,
                        confidence_scores=entry["confidence"],
                        notes=entry["notes"],
                    )
                    print(f"✓ Uploaded: {entry['video_uid']} | reviewer:{entry['user_name']}")
                
                session.commit()
                print(f"🎉 Successfully uploaded {len(validated_entries)} reviews!")
                
            except Exception as exc:
                session.rollback()
                raise RuntimeError(f"Upload failed: {exc}") from None
    else:
        print("ℹ️  No new reviews to upload")


def _verification_passes_reviews(
    *,
    session: Session,
    video_id: int, 
    project_id: int,
    reviewer_id: int,
    group_id: int,
    answers: Dict[str, str],
) -> None:
    """
    Validate one review label *without* writing to DB.
    Missing answers are tolerated for questions where `is_required` is False.
    """
    # 1. project & reviewer existence / role checks ------------------------
    GroundTruthService._validate_project_and_user(project_id, reviewer_id, session)
    GroundTruthService._validate_user_role(reviewer_id, project_id, "reviewer", session)

    # 2. fetch group + questions ---------------------------------------
    group, questions = GroundTruthService._validate_question_group(group_id, session)

    # ---- build two helper sets ---------------------------------------
    required_q_texts = {q.text for q in questions if getattr(q, "required", True)}
    provided_q_texts = set(answers)
    missing = required_q_texts - provided_q_texts
    extra   = provided_q_texts  - {q.text for q in questions}

    if missing or extra:
        raise ValueError(
            f"Answers do not match questions in group. "
            f"Missing: {missing}. Extra: {extra}"
        )

    # 3. run optional verification hook -------------------------------
    GroundTruthService._run_verification(group, answers)

    # 4. per-question value validation (only for keys we have) ---------
    q_lookup = {q.text: q for q in questions}
    for q_text in provided_q_texts:
        GroundTruthService._validate_answer_value(q_lookup[q_text], answers[q_text])


def batch_upload_annotations(annotations_folder: str = None, annotations_data: list[dict] = None) -> None:
    if annotations_folder is None and annotations_data is None:
        raise ValueError("At least one parameter must be provided: annotations_folder or annotations_data")
    
    if annotations_folder is not None:
        import os
        import glob
        annotations_data = []
        paths = glob.glob(os.path.join(annotations_folder, '*.json'))
        for path in paths:
            with open(path, 'r') as f:
                annotations_data.append(json.load(f))
    for annotation_data in annotations_data:
        upload_annotations(annotation_data)
    
def batch_upload_reviews(reviews_folder: str = None, reviews_data: list[dict] = None) -> None:
    
    if reviews_folder is None and reviews_data is None:
        raise ValueError("At least one parameter must be provided: reviews_folder or reviews_data")
    
    if reviews_folder is not None:
        import os
        import glob
        reviews_data = []
        paths = glob.glob(os.path.join(reviews_folder, '*'))
        for path in paths:
            with open(path, 'r') as f:
                reviews_data.append(json.load(f))
    for review_data in reviews_data:
        upload_reviews(review_data)

