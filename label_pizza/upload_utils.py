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
from label_pizza.db import SessionLocal, engine # Must have been initialized by init_database() before importing this file
import hashlib
from pathlib import Path
from typing import List, Dict, Optional, Any, Set, Tuple
import pandas as pd

def apply_simple_video_configs(config_file_path: str = None, configs_data: list[dict] = None):
    """
    Apply video configurations with proper verification and synchronization.
    
    This function:
    1. Validates all configurations before making changes
    2. Synchronizes custom displays between JSON and database
    3. Removes custom displays that exist in DB but not in JSON
    4. Updates custom displays that differ between JSON and DB
    5. Skips custom displays that are identical in JSON and DB
    
    Expected structure:
    [
        {
            "project_name": "Project Name",
            "videos": {
                "video_uid": [
                    {
                        "question_text": "Question 1",
                        "display_text": "Display text",
                        "option_map": {"1": "Option 1"}
                    }
                ]
            }
        }
    ]
    """
    import json
    
    if config_file_path is None and configs_data is None:
        raise ValueError("Either config_file_path or configs_data must be provided")
    
    # Load data from file if path provided
    if config_file_path is not None:
        try:
            with open(config_file_path, 'r', encoding='utf-8') as f:
                configs_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to read config file: {str(e)}")
    
    if not isinstance(configs_data, list) or not configs_data:
        raise ValueError("Config data must be a non-empty list")
    
    print(f"📋 Processing {len(configs_data)} configurations...")
    
    # Phase 1: Validation
    validation_errors = []
    
    with SessionLocal() as session:
        for i, config_data in enumerate(configs_data, 1):
            if not isinstance(config_data, dict):
                validation_errors.append(f"Config #{i}: Invalid structure")
                continue
            
            project_name = config_data.get("project_name")
            if not project_name:
                validation_errors.append(f"Config #{i}: Missing project_name")
                continue
            
            # Check project exists
            try:
                project = ProjectService.get_project_by_name(project_name, session)
                project_id = project.id
                
                # Check if project is archived
                if project.is_archived:
                    validation_errors.append(f"Project '{project_name}': Project is archived")
                    continue
                
                # Get schema by ID and check custom display capability
                schema = SchemaService.get_schema_by_id(project.schema_id, session)
                if not schema.has_custom_display:
                    validation_errors.append(f"Project '{project_name}': Schema does not have custom display enabled")
                    continue
                
            except Exception as e:
                validation_errors.append(f"Project '{project_name}': {str(e)}")
                continue
            
            # Validate video configs
            video_configs = config_data.get("videos", {})
            if not isinstance(video_configs, dict):
                validation_errors.append(f"Project '{project_name}': 'videos' must be a dictionary")
                continue
            
            # Get all project questions for validation
            try:
                project_questions = ProjectService.get_project_questions(project_id, session)
                project_question_texts = {q["text"] for q in project_questions}
            except Exception as e:
                validation_errors.append(f"Project '{project_name}': Failed to get project questions: {str(e)}")
                continue
            
            for video_uid, question_list in video_configs.items():
                # Check video exists
                try:
                    video = VideoService.get_video_by_uid(video_uid, session)
                    if not video:
                        validation_errors.append(f"Project '{project_name}': Video '{video_uid}' not found")
                        continue
                    
                    if video.is_archived:
                        validation_errors.append(f"Project '{project_name}': Video '{video_uid}' is archived")
                        continue
                    
                    video_id = video.id
                    
                except Exception as e:
                    validation_errors.append(f"Project '{project_name}': Video '{video_uid}': {str(e)}")
                    continue
                
                # Validate question list
                if not isinstance(question_list, list):
                    validation_errors.append(f"Project '{project_name}': Video '{video_uid}': Questions must be a list")
                    continue
                
                for j, question_config in enumerate(question_list):
                    if not isinstance(question_config, dict):
                        validation_errors.append(
                            f"Project '{project_name}': Video '{video_uid}': Question #{j+1}: Invalid structure"
                        )
                        continue
                    
                    # Check required fields
                    question_text = question_config.get("question_text")
                    if not question_text:
                        validation_errors.append(
                            f"Project '{project_name}': Video '{video_uid}': Question #{j+1}: Missing question_text"
                        )
                        continue
                    
                    if "display_text" not in question_config:
                        validation_errors.append(
                            f"Project '{project_name}': Video '{video_uid}': Question '{question_text}': Missing display_text"
                        )
                        continue
                    
                    # Check if question exists in project schema
                    if question_text not in project_question_texts:
                        validation_errors.append(
                            f"Project '{project_name}': Video '{video_uid}': Question '{question_text}' not in project schema"
                        )
                        continue
                    
                    # Get question details for further validation
                    try:
                        question = QuestionService.get_question_by_text(question_text, session)
                        question_id = question["id"]
                        
                        # Pre-validate using verify_set_custom_display
                        CustomDisplayService.verify_set_custom_display(
                            project_id=project_id,
                            video_id=video_id,
                            question_id=question_id,
                            custom_display_text=question_config.get("display_text"),
                            custom_option_display_map=question_config.get("option_map"),
                            session=session
                        )
                        
                    except Exception as e:
                        validation_errors.append(
                            f"Project '{project_name}': Video '{video_uid}': Question '{question_text}': {str(e)}"
                        )
    
    # Stop if validation failed
    if validation_errors:
        print("❌ Validation failed:")
        for error in validation_errors:
            print(f"   • {error}")
        raise ValueError(f"Validation failed with {len(validation_errors)} errors")
    
    print("✅ Validation passed. Applying configurations...")
    
    # Phase 2: Apply configurations with synchronization
    with SessionLocal() as session:
        try:
            total_created = 0
            total_updated = 0
            total_removed = 0
            total_skipped = 0
            
            for config_data in configs_data:
                project_name = config_data["project_name"]
                project = ProjectService.get_project_by_name(project_name, session)
                project_id = project.id
                
                video_configs = config_data.get("videos", {})
                
                # Get all project questions
                project_questions = ProjectService.get_project_questions(project_id, session)
                
                # Process each video in the project
                project_videos = VideoService.get_project_videos(project_id, session)
                
                for video in project_videos:
                    video_id = video["id"]
                    video_uid = video["uid"]
                    
                    # Get questions configured for this video in JSON
                    json_questions = video_configs.get(video_uid, [])
                    json_question_map = {
                        q["question_text"]: q 
                        for q in json_questions
                    }
                    
                    # Process each question in the schema
                    for question in project_questions:
                        question_id = question["id"]
                        question_text = question["text"]
                        
                        # Check if JSON has custom display for this question
                        json_has_display = question_text in json_question_map
                        
                        # Check if DB has custom display for this question
                        try:
                            existing_display = CustomDisplayService.get_custom_display(
                                project_id=project_id,
                                video_id=video_id,
                                question_id=question_id,
                                session=session
                            )
                            db_has_display = existing_display is not None
                        except:
                            db_has_display = False
                            existing_display = None
                        
                        # Synchronization logic
                        if db_has_display and not json_has_display:
                            # Remove custom display from DB
                            CustomDisplayService.remove_custom_display(
                                project_id=project_id,
                                video_id=video_id,
                                question_id=question_id,
                                session=session
                            )
                            print(f"   ✗ Removed custom display for '{question_text}' from video {video_uid}")
                            total_removed += 1
                            
                        elif json_has_display:
                            json_config = json_question_map[question_text]
                            new_display_text = json_config.get("display_text")
                            new_option_map = json_config.get("option_map")
                            
                            if db_has_display:
                                # Check if they're the same
                                same_display_text = existing_display.get("custom_display_text") == new_display_text
                                same_option_map = existing_display.get("custom_option_display_map") == new_option_map
                                
                                if same_display_text and same_option_map:
                                    # Skip - no changes needed
                                    print(f"   ≈ Skipped '{question_text}' for video {video_uid} (no changes)")
                                    total_skipped += 1
                                else:
                                    # Update custom display
                                    CustomDisplayService.set_custom_display(
                                        project_id=project_id,
                                        video_id=video_id,
                                        question_id=question_id,
                                        custom_display_text=new_display_text,
                                        custom_option_display_map=new_option_map,
                                        session=session
                                    )
                                    print(f"   ↻ Updated custom display for '{question_text}' on video {video_uid}")
                                    total_updated += 1
                            else:
                                # Create new custom display
                                CustomDisplayService.set_custom_display(
                                    project_id=project_id,
                                    video_id=video_id,
                                    question_id=question_id,
                                    custom_display_text=new_display_text,
                                    custom_option_display_map=new_option_map,
                                    session=session
                                )
                                print(f"   ✓ Created custom display for '{question_text}' on video {video_uid}")
                                total_created += 1
                
                print(f"✅ Completed project '{project_name}'")
            
            session.commit()
            
            print(f"\n📊 Summary:")
            print(f"   • Created: {total_created}")
            print(f"   • Updated: {total_updated}")
            print(f"   • Removed: {total_removed}")
            print(f"   • Skipped: {total_skipped}")
            print(f"   • Total processed: {total_created + total_updated + total_removed + total_skipped}")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Error during application. Rolled back all changes: {str(e)}")
            raise ValueError(f"Configuration application failed: {str(e)}")


def add_videos(videos_data: list[dict]) -> None:
    """
    Add new videos from a list of dictionaries.
    
    Args:
        videos_data: List of video dictionaries to add
        
    JSON format for each video:
        {
            "url": "https://example.com/video.mp4",
            "metadata": {
                "title": "Video Title",
                "description": "Video description"
            }
        }
        
    Raises:
        TypeError: If videos_data is not a list
        ValueError: If validation fails for any video
        RuntimeError: If database commit fails
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    with SessionLocal() as session:
        # Check for existing videos and collect errors
        existing_video_errors = []
        new_videos = []

        for video in tqdm(videos_data, desc="Verifying videos"):
            try:
                VideoService.verify_add_video(
                    url=video["url"],
                    session=session,
                    metadata=video.get("metadata")
                )
                new_videos.append(video)
            except ValueError as e:
                if "already exists" in str(e):
                    existing_video_errors.append(video["url"])
                else:
                    raise ValueError(
                        f"Validation failed for {video['url']}: {e}"
                    ) from None

        if existing_video_errors:
            raise ValueError(
                f"Cannot add videos - the following videos already exist: {', '.join(existing_video_errors)}"
            )

        if not new_videos:
            print("ℹ️  No new videos to add - all videos already exist")
            return

        # Add all new videos
        for video in tqdm(new_videos, desc="Adding videos", unit="video"):
            try:
                VideoService.add_video(
                    url=video["url"],
                    session=session,
                    metadata=video.get("metadata")
                )
                print(f"✓ Added new video: {video['url']}")
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to add video {video['url']}: {e}") from None

        try:
            session.commit()
            print(f"✔ Successfully added {len(new_videos)} new videos!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def update_videos(videos_data: list[dict]) -> None:
    """
    Update existing videos from a list of dictionaries.
    
    Args:
        videos_data: List of video dictionaries to update
        
    JSON format for each video:
        {
            "video_uid": "video123",
            "url": "https://example.com/updated-video.mp4",
            "metadata": {
                "title": "Updated Video Title",
                "description": "Updated description"
            }
        }
        
    Raises:
        TypeError: If videos_data is not a list
        ValueError: If any video doesn't exist or validation fails
        RuntimeError: If database commit fails
    """
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")

    with SessionLocal() as session:
        # Check for non-existing videos and collect errors
        non_existing_video_errors = []
        videos_to_update = []

        for video in videos_data:
            try:
                VideoService.verify_update_video(
                    video_uid=video["video_uid"],
                    new_url=video["url"],
                    new_metadata=video.get("metadata"),
                    session=session
                )
                videos_to_update.append(video)
            except ValueError as e:
                if "not found" in str(e):
                    non_existing_video_errors.append(video["video_uid"])
                else:
                    raise ValueError(
                        f"Validation failed for {video['video_uid']}: {e}"
                    ) from None

        if non_existing_video_errors:
            raise ValueError(
                f"Cannot update videos - the following videos don't exist: {', '.join(non_existing_video_errors)}"
            )

        if not videos_to_update:
            print("ℹ️  No existing videos to update")
            return

        # Update all existing videos
        for video in tqdm(videos_to_update, desc="Updating videos", unit="video"):
            try:
                VideoService.update_video(
                    video_uid=video["video_uid"],
                    new_url=video["url"],
                    new_metadata=video.get("metadata"),
                    session=session
                )
                print(f"✓ Updated video: {video['video_uid']}")
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to update video {video['video_uid']}: {e}") from None

        try:
            session.commit()
            print(f"✔ Successfully updated {len(videos_to_update)} videos!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def upload_videos(videos_path: str = None, videos_data: list[dict] = None) -> None:
    """
    Upload videos from a JSON file or data list, handling both new and existing videos.
    If video_uid is not present in JSON data, the video will be added as new.
    If video_uid is present, checks database to determine add vs update.
    
    Args:
        videos_path: Path to the video JSON file
        videos_data: List of video dictionaries
        
    JSON format for new videos (no video_uid):
        [
            {
                "url": "https://example.com/video.mp4",
                "metadata": {
                    "title": "Video Title",
                    "description": "Video description"
                }
            }
        ]
        
    JSON format when video_uid is specified:
        [
            {
                "video_uid": "video.mp4",
                "url": "https://example.com/video.mp4",
                "metadata": {
                    "title": "Video Title"
                }
            }
        ]
        
    Raises:
        ValueError: If no parameters provided or validation fails
        TypeError: If videos_data is not a list
        RuntimeError: If database operations fail
    """
    if videos_path is None and videos_data is None:
        raise ValueError("At least one parameter must be provided: videos_path or videos_data")
    
    if videos_path is not None:
        with open(videos_path, 'r') as f:
            videos_data = json.load(f)
    
    if not isinstance(videos_data, list):
        raise TypeError("videos_data must be a list of dictionaries")
    
    # Split videos based on presence of video_uid in JSON data
    videos_without_uid = []  # Always add these
    videos_with_uid = []     # Check database for these
    
    for video in videos_data:
        if "video_uid" not in video:
            videos_without_uid.append(video)
        else:
            videos_with_uid.append(video)
    
    # Process videos without video_uid - always add
    if videos_without_uid:
        print(f"📝 Processing {len(videos_without_uid)} videos without video_uid (will be added)...")
        add_videos(videos_without_uid)
    else:
        print("ℹ️  No videos without video_uid to add")
    
    # For videos with video_uid, check database to determine add vs update
    if videos_with_uid:
        with SessionLocal() as session:
            new_videos = []
            existing_videos_data = []
            
            for video in videos_with_uid:
                video_uid = video['video_uid']
                
                # Check if video_uid exists in database
                existing_video = VideoService.get_video_by_uid(video_uid, session)
                if existing_video:
                    existing_videos_data.append(video)
                else:
                    new_videos.append(video)
        
        print(f"Found {len(new_videos)} new videos and {len(existing_videos_data)} existing videos (with video_uid)")
        # Process new videos with video_uid
        if new_videos:
            print(f"📝 Processing {len(new_videos)} new videos with video_uid...")
            add_videos(new_videos)
        else:
            print("ℹ️  No new videos with video_uid to add")
        
        # Process existing videos
        if existing_videos_data:
            print(f"🔄 Processing {len(existing_videos_data)} existing videos...")
            update_videos(existing_videos_data)
        else:
            print("ℹ️  No existing videos to update")
    else:
        print("ℹ️  No videos with video_uid to process")
    
    print("🎉 Video upload completed!")


def add_schemas(schemas_data: list[dict]) -> None:
    """
    Add new schemas from a list of dictionaries.
    
    Args:
        schemas_data: List of schema dictionaries to add
        
    JSON format for each schema:
        {
            "schema_name": "Video Classification Schema",
            "question_group_names": [group_1, group_2, group_3],
            "instructions_url": "https://example.com/instructions"  (optional),
            "is_archived": false (optional),
            "has_custom_display": false (optional)
        }
        
    Raises:
        ValueError: If any schema already exists or validation fails
        RuntimeError: If database commit fails
    """
    if not isinstance(schemas_data, list):
        raise TypeError("schemas_data must be a list of dictionaries")
    
    with SessionLocal() as session:
        existing_schemas = SchemaService.get_all_schemas(session)
        existing_schema_names = set(existing_schemas['Name'].tolist())
        
        # Check for existing schemas and collect errors
        existing_schema_errors = []
        new_schemas = []
        
        for schema in schemas_data:
            # Check if schema already exists
            schema_name = schema.get('schema_name', None)
            if schema_name in existing_schema_names:
                existing_schema_errors.append(schema_name)
            else:
                new_schemas.append(schema)
        
        if existing_schema_errors:
            raise ValueError(
                f"Cannot add schemas - the following schemas already exist: {', '.join(existing_schema_errors)}"
            )
        
        if not new_schemas:
            print("ℹ️  No new schemas to add - all schemas already exist")
            return

        # Pre-verify all new schemas before creating any
        for schema in new_schemas:
            try:
                # Convert question_group_names to question_group_ids
                question_group_names = schema.get('question_group_names', [])
                question_group_ids = []
                for group_name in question_group_names:
                    group = QuestionGroupService.get_group_by_name(group_name, session)
                    question_group_ids.append(group.id)
                
                SchemaService.verify_create_schema(
                    name=schema.get('schema_name'),
                    question_group_ids=question_group_ids,
                    instructions_url=schema.get('instructions_url'),
                    has_custom_display=schema.get('has_custom_display', False),
                    session=session
                )
            except ValueError as e:
                raise ValueError(f"Validation failed for schema '{schema.get('schema_name')}': {e}") from None
        
        # Add all new schemas
        for schema in tqdm(new_schemas, desc="Adding schemas", unit="schema"):
            schema_name = schema.get('schema_name', None)
            
            # Convert question_group_names to question_group_ids
            question_group_names = schema.get('question_group_names', [])
            question_group_ids = []
            for group_name in question_group_names:
                group = QuestionGroupService.get_group_by_name(group_name, session)
                question_group_ids.append(group.id)
            
            instructions_url = schema.get('instructions_url')
            has_custom_display = schema.get('has_custom_display', False)
            
            try:
                SchemaService.create_schema(
                    name=schema_name,
                    question_group_ids=question_group_ids,
                    instructions_url=instructions_url,
                    has_custom_display=has_custom_display,
                    session=session
                )
                print(f"✓ Added new schema: {schema_name}")
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to create schema {schema_name}: {e}") from None
        
        try:
            session.commit()
            print(f"✔ Successfully added {len(new_schemas)} new schemas!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def edit_schemas(schemas_data: list[dict]) -> None:
    """
    Edit existing schemas from a list of dictionaries.
    Matches schemas by name.
    
    Args:
        schemas_data: List of schema dictionaries to update
        
    JSON format for each schema:
        {
            "schema_name": "Video Classification Schema",
            "question_group_names": [group_1, group_2, group_3],
            "instructions_url": "https://example.com/instructions"  (optional),
            "is_archived": false (optional),
            "has_custom_display": false (optional)
        }
        
    Raises:
        ValueError: If any schema doesn't exist or validation fails
        RuntimeError: If database commit fails
    """
    if not isinstance(schemas_data, list):
        raise TypeError("schemas_data must be a list of dictionaries")
    
    with SessionLocal() as session:
        existing_schemas = SchemaService.get_all_schemas(session)
        existing_schema_map = {row['Name']: row for _, row in existing_schemas.iterrows()}
        
        # Check for non-existing schemas and collect errors
        non_existing_schema_errors = []
        schemas_to_update = []
        
        for schema in schemas_data:
            schema_name = schema.get('schema_name', None)
            
            if schema_name and schema_name in existing_schema_map:
                schemas_to_update.append(schema)
            else:
                non_existing_schema_errors.append(schema_name)
        
        if non_existing_schema_errors:
            raise ValueError(
                f"Cannot update schemas - the following schemas don't exist: {', '.join(non_existing_schema_errors)}"
            )
        
        if not schemas_to_update:
            print("ℹ️  No existing schemas to update")
            return

        # Pre-verify all schema updates before updating any
        for schema in schemas_to_update:
            try:
                schema_name = schema.get('schema_name')
                existing_schema = SchemaService.get_schema_by_name(schema_name, session)
                schema_id = existing_schema.id
                
                SchemaService.verify_edit_schema(
                    schema_id=schema_id,
                    name=schema.get('schema_name'),
                    instructions_url=schema.get('instructions_url'),
                    has_custom_display=schema.get('has_custom_display'),
                    is_archived=schema.get('is_archived'),
                    session=session
                )
            except ValueError as e:
                raise ValueError(f"Validation failed for schema '{schema_name}': {e}") from None
        
        # Update all existing schemas
        for schema in tqdm(schemas_to_update, desc="Updating schemas", unit="schema"):
            schema_name = schema.get('schema_name')
            instructions_url = schema.get('instructions_url')
            has_custom_display = schema.get('has_custom_display')
            is_archived = schema.get('is_archived')
            
            try:
                # Get schema ID
                existing_schema = SchemaService.get_schema_by_name(schema_name, session)
                schema_id = existing_schema.id
                
                # Update schema
                SchemaService.edit_schema(
                    schema_id=schema_id,
                    name=schema_name,
                    instructions_url=instructions_url,
                    has_custom_display=has_custom_display,
                    is_archived=is_archived,
                    session=session
                )
                
                print(f"✓ Updated schema: {schema_name}")
                
            except Exception as e:
                session.rollback()
                raise ValueError(f"Failed to update schema {schema_name}: {e}") from None
        
        try:
            session.commit()
            print(f"✔ Successfully updated {len(schemas_to_update)} schemas!")
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Error committing changes: {e}") from None


def upload_schemas(schemas_path: str = None, schemas_data: list[dict] = None) -> None:
    """
    Upload schemas from a JSON file or data list, handling both new and existing schemas.
    Schemas are matched by name - if name matches, the schema is updated.
    
    Args:
        schemas_path: Path to the schema JSON file
        schemas_data: List of schema dictionaries
        
    JSON format for new schemas:
        [
            {
                "name": "Video Classification Schema",
                "question_group_ids": [1, 2, 3],
                "instructions_url": "https://example.com/instructions",
                "has_custom_display": false
            }
        ]
        
    JSON format for updating existing schemas:
        [
            {
                "schema_name": "Existing Schema Name",
                "new_name": "Updated Name",
                "instructions_url": "https://example.com/new-instructions",
                "has_custom_display": true,
                "is_archived": false
            }
        ]
        
    Raises:
        ValueError: If any validation fails in add_schemas or edit_schemas
        RuntimeError: If database operations fail
    """
    if schemas_path is None and schemas_data is None:
        raise ValueError("At least one parameter must be provided: schemas_path or schemas_data")
    
    if schemas_path is not None:
        with open(schemas_path, 'r') as f:
            schemas_data = json.load(f)
    
    if not isinstance(schemas_data, list):
        raise TypeError("schemas_data must be a list of dictionaries")
    
    # Split schemas into existing and new based on name match
    with SessionLocal() as session:
        existing_schemas = SchemaService.get_all_schemas(session)
        existing_schema_names = set(existing_schemas['Name'].tolist())
        
        print(f"Found {len(existing_schema_names)} existing schemas")
        
        new_schemas = []
        existing_schemas_data = []
        
        for schema in schemas_data:
            # Check if this is an update (has schema_name) or new schema (has name)
            schema_name = schema.get('schema_name', None)
            
            if schema_name and schema_name in existing_schema_names:
                # This is an update to an existing schema
                existing_schemas_data.append(schema)
            else:
                # This is a new schema
                new_schemas.append(schema)
    
    # Process new schemas
    if new_schemas:
        print(f"📝 Processing {len(new_schemas)} new schemas...")
        add_schemas(new_schemas)
    else:
        print("ℹ️  No new schemas to add")
    
    # Process existing schemas
    if existing_schemas_data:
        print(f"🔄 Processing {len(existing_schemas_data)} existing schemas...")
        edit_schemas(existing_schemas_data)
    else:
        print("ℹ️  No existing schemas to update")
    
    print("🎉 Schema upload completed!")
    

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
            if user_id in existing_user_ids or email in existing_emails and email is not None:
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

        # Pre-verify all new users before creating any
        for user in new_users:
            try:
                AuthService.verify_create_user(
                    user_id=user.get('user_id', None),
                    email=user.get('email', None),
                    password_hash=user['password'],
                    user_type=user.get('user_type', 'human'),
                    session=session
                )
            except ValueError as e:
                raise ValueError(f"Validation failed for user '{user.get('user_id')}': {e}") from None
        
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
    Verifies ALL projects before creating ANY of them.
    """
    import json
    
    # Load data
    if projects_path:
        with open(projects_path, 'r') as f:
            projects_data = json.load(f)
    
    if not projects_data:
        raise ValueError("No project data provided")
    
    print(f"📁 Verifying {len(projects_data)} projects...")
    
    # VERIFY ALL PROJECTS FIRST
    with SessionLocal() as session:
        for project_data in projects_data:
            project_name = project_data['project_name']
            schema_name = project_data['schema_name']
            videos = project_data['videos']
            
            # Extract video UIDs (handle both list and dict formats)
            if isinstance(videos, dict):
                video_uids = list(videos.keys())
            else:
                video_uids = videos
            
            # Get schema ID
            schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
            
            # Get video IDs
            video_ids = ProjectService.get_video_ids_by_uids(video_uids, session)
            
            # Verify project creation
            ProjectService.verify_create_project(
                name=project_name,
                schema_id=schema_id,
                video_ids=video_ids,
                session=session
            )
            
            print(f"✓ Verified project '{project_name}' with {len(video_uids)} videos")
    
    print("✅ All projects verified! Creating...")
    
    # NOW CREATE ALL PROJECTS
    with SessionLocal() as session:
        try:
            for project_data in projects_data:
                project_name = project_data['project_name']
                schema_name = project_data['schema_name']
                videos = project_data['videos']
                
                # Extract video UIDs (handle both list and dict formats)
                if isinstance(videos, dict):
                    video_uids = list(videos.keys())
                else:
                    video_uids = videos
                
                # Get schema ID
                schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
                
                # Get video IDs
                video_ids = ProjectService.get_video_ids_by_uids(video_uids, session)
                
                # Create project
                ProjectService.create_project(
                    name=project_name,
                    schema_id=schema_id,
                    video_ids=video_ids,
                    session=session
                )
                
                print(f"✓ Created project '{project_name}' with {len(video_uids)} videos")
            
            session.commit()
            print(f"✅ Done! Created {len(projects_data)} projects")
            
        except Exception as e:
            session.rollback()
            print(f"❌ Error during creation: {e}")
            raise

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

                # Use service verification - this handles all validation logic
                AnnotatorService.verify_submit_answer_to_question_group(
                    video_id=video_id,
                    project_id=project_id,
                    user_id=user_id,
                    question_group_id=group_id,
                    answers=row["answers"],
                    session=session,
                    confidence_scores=row.get("confidence_scores"),
                    notes=row.get("notes")
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
                
                for question_text, new_answer_value in row["answers"].items():
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
                    "answers": row["answers"],
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


def upload_reviews(rows: List[Dict[str, Any]]) -> None:
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

                # Use service verification - this handles all validation logic
                GroundTruthService.verify_submit_ground_truth_to_question_group(
                    video_id=video_id,
                    project_id=project_id,
                    reviewer_id=reviewer_id,
                    question_group_id=group_id,
                    answers=row["answers"],
                    session=session,
                    confidence_scores=row.get("confidence_scores"),
                    notes=row.get("notes")
                )

                # Check for existing ground truth and determine what to upload
                to_upload = {}
                all_skipped = True
                
                # Get ground truth DataFrame for this video and project
                gt_df = GroundTruthService.get_ground_truth(video_id, project_id, session)
                
                for question_text, answer_value in row.get("answers", {}).items():
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


def batch_upload_annotations(annotations_folder: str = None, annotations_data: list[dict] = None, max_workers: int = 4) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os
    import glob
    import json
    
    if annotations_folder is None and annotations_data is None:
        raise ValueError("At least one parameter must be provided: annotations_folder or annotations_data")
    
    if annotations_folder is not None:
        paths = glob.glob(os.path.join(annotations_folder, '*.json'))
        
        # Option 1: Process files concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all file reading tasks
            future_to_path = {executor.submit(load_and_upload_annotations_file, path): path for path in paths}
            
            # Process completed tasks
            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    future.result()  # This will raise any exception that occurred
                    print(f"Successfully processed: {path}")
                except Exception as e:
                    print(f"Error processing {path}: {e}")
    
    elif annotations_data is not None:
        # Option 2: Process annotation data concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all upload tasks
            futures = [executor.submit(upload_annotations, annotation_data) for annotation_data in annotations_data]
            
            # Wait for all tasks to complete
            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                    print(f"Successfully uploaded annotation {i+1}")
                except Exception as e:
                    print(f"Error uploading annotation {i+1}: {e}")
    
def batch_upload_reviews(reviews_folder: str = None, reviews_data: list[dict] = None, max_workers: int = 4) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import os
    import glob
    import json
    if reviews_folder is None and reviews_data is None:
        raise ValueError("At least one parameter must be provided: reviews_folder or reviews_data")
    
    if reviews_folder is not None:
        paths = glob.glob(os.path.join(reviews_folder, '*'))
        
        # Option 1: Process files concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all file reading tasks
            future_to_path = {executor.submit(load_and_upload_reviews_file, path): path for path in paths}
            
            # Process completed tasks
            for future in as_completed(future_to_path):
                path = future_to_path[future]
                try:
                    future.result()  # This will raise any exception that occurred
                    print(f"Successfully processed: {path}")
                except Exception as e:
                    print(f"Error processing {path}: {e}")
    
    elif reviews_data is not None:
        # Option 2: Process review data concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all upload tasks
            futures = [executor.submit(upload_reviews, review_data) for review_data in reviews_data]
            
            # Wait for all tasks to complete
            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                    print(f"Successfully uploaded review {i+1}")
                except Exception as e:
                    print(f"Error uploading review {i+1}: {e}")

def load_and_upload_reviews_file(file_path: str) -> None:
    """Helper function specifically for loading and uploading review files"""
    try:
        with open(file_path, 'r') as f:
            review_data = json.load(f)
        upload_reviews(review_data)
    except Exception as e:
        raise Exception(f"Failed to process {file_path}: {e}")

def load_and_upload_annotations_file(file_path: str) -> None:
    """Helper function specifically for loading and uploading annotation files"""
    try:
        with open(file_path, 'r') as f:
            annotation_data = json.load(f)
        upload_annotations(annotation_data)
    except Exception as e:
        raise Exception(f"Failed to process {file_path}: {e}")