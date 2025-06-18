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


def add_videos(json_file_path: str = None):
    """
    Add new videos from JSON file, raise error if any video already exists
    
    Args:
        json_file_path: Path to the JSON file containing video data
    """
    # Load and parse JSON file
    with open(json_file_path, 'r') as f:
        videos_data = json.load(f)
    
    # Check all videos existence and validate data
    with Session(engine) as session:
        existing_videos = []
        for video_data in videos_data:
            try:
                # Use VideoService's verification function directly
                VideoService.verify_add_video(
                    url=video_data['url'],
                    session=session,
                    metadata=video_data['metadata']
                )
            except ValueError as e:
                if "already exists" in str(e):
                    existing_videos.append(video_data['url'])
                else:
                    raise ValueError(f"Video data validation failed: {str(e)}")
        
        if existing_videos:
            raise ValueError(f"Videos already exist: {', '.join(existing_videos)}")
        
        # If no existing videos, proceed with adding all videos
        for video_data in tqdm(videos_data, desc="Adding videos", unit="video"):
            VideoService.add_video(
                url=video_data['url'],
                session=session,
                metadata=video_data['metadata']
            )
            print(f"Successfully added new video: {video_data['url']}")
        
        try:
            session.commit()
            print("All videos have been successfully processed!")
        except Exception as e:
            print(f"Error committing changes: {str(e)}")
            session.rollback()

def update_videos(json_file_path: str = None):
    """
    Update existing videos from JSON file, raise error if any video doesn't exist
    
    Args:
        json_file_path: Path to the JSON file containing video data
    """
    # Load and parse JSON file
    with open(json_file_path, 'r') as f:
        videos_data = json.load(f)
    
    # Check all videos existence and validate data
    with Session(engine) as session:
        missing_videos = []
        for video_data in videos_data:
            try:
                # Use VideoService's verification function directly
                VideoService.verify_update_video(
                    video_uid=video_data['video_uid'],
                    new_url=video_data['url'],
                    new_metadata=video_data['metadata'],
                    session=session
                )
            except ValueError as e:
                if "not found" in str(e):
                    missing_videos.append(video_data['video_uid'])
                else:
                    raise ValueError(f"Video data validation failed: {str(e)}")
        
        if missing_videos:
            raise ValueError(f"Videos do not exist: {', '.join(missing_videos)}")
        
        # If all videos exist, proceed with updating all videos
        for video_data in tqdm(videos_data, desc="Updating videos", unit="video"):
            VideoService.update_video(
                video_uid=video_data['video_uid'],
                new_url=video_data['url'],
                new_metadata=video_data['metadata'],
                session=session
            )
            print(f"Successfully updated video: {video_data['video_uid']}")
        
        try:
            session.commit()
            print("All videos have been successfully processed!")
        except Exception as e:
            print(f"Error committing changes: {str(e)}")
            session.rollback()

def import_question_group(json_file_path: str = None):
    """
    Import a single question group from a JSON file. If the question group or any question already exists, raises an error.

    Args:
        json_file_path: Path to the JSON file containing question group data

    Returns:
        int: The ID of the created question group

    Raises:
        ValueError: If question group with the same title already exists
        ValueError: If any question with the same text already exists
        ValueError: If required fields are missing in the JSON
        ValueError: If question data is invalid
        Exception: If database operations fail
    """
    # Load and parse JSON file
    with open(json_file_path, 'r') as f:
        group_data = json.load(f)

    with SessionLocal() as session:
        try:
            # 1. Verify all questions first (verify functions now return None)
            for question_data in tqdm(group_data['questions'], desc="Verifying questions"):
                QuestionService.verify_add_question(
                    text=question_data['text'],
                    qtype=question_data['qtype'],
                    options=question_data.get('options'),
                    default=question_data.get('default_option'),
                    session=session,
                    display_values=question_data.get('display_values'),
                    display_text=question_data.get('display_text'),
                    option_weights=question_data.get('option_weights')
                )

            # 2. If all verifications pass, create all questions
            question_ids = []
            for question_data in tqdm(group_data['questions'], desc="Creating questions"):
                question = QuestionService.add_question(
                    text=question_data['text'],
                    qtype=question_data['qtype'],
                    options=question_data.get('options'),
                    default=question_data.get('default_option'),
                    session=session,
                    display_values=question_data.get('display_values'),
                    display_text=question_data.get('display_text'),
                    option_weights=question_data.get('option_weights')
                )
                question_ids.append(question.id)
                print(f"Created new question: {question_data['text']}")

            # 3. Verify the group (verify function now returns None)
            QuestionGroupService.verify_create_group(
                title=group_data['title'],
                description=group_data['description'],
                is_reusable=group_data['is_reusable'],
                question_ids=question_ids,
                verification_function=group_data.get('verification_function', ''),
                is_auto_submit=group_data.get('is_auto_submit', False),
                session=session
            )

            # 4. Create the group
            question_group = QuestionGroupService.create_group(
                title=group_data['title'],
                description=group_data['description'],
                is_reusable=group_data['is_reusable'],
                question_ids=question_ids,
                verification_function=group_data.get('verification_function', ''),
                is_auto_submit=group_data.get('is_auto_submit', False),
                session=session
            )
            print(f"Successfully created question group: {group_data['title']}")
            return question_group.id

        except ValueError as e:
            session.rollback()
            raise ValueError(f"Error processing question group: {str(e)}")
        except Exception as e:
            session.rollback()
            raise Exception(f"Error processing question group: {str(e)}")

        
def update_questions(json_file_path: str = None) -> None:
    """
    Update existing questions from a JSON file. All questions must exist in the database.
    
    Args:
        json_file_path: Path to the JSON file containing question data
        
    Raises:
        ValueError: If any question not found or validation fails
        Exception: If database operations fail
    """
    with open(json_file_path, 'r') as f:
        questions_data = json.load(f)
    
    with SessionLocal() as session:
        try:
            # First verify all questions
            missing_questions = []
            for question_data in questions_data:
                try:
                    question_info = QuestionService.get_question_by_text(question_data['text'], session)
                    question_id = question_info['id']
                    
                    # Verify the question update (verify function now returns None)
                    QuestionService.verify_edit_question(
                        question_id=question_id,
                        new_display_text=question_data.get('display_text'),
                        new_opts=question_data.get('options'),
                        new_default=question_data.get('default_option'),
                        session=session,
                        new_display_values=question_data.get('display_values'),
                        new_option_weights=question_data.get('option_weights')
                    )
                except ValueError as e:
                    if "not found" in str(e):
                        missing_questions.append(question_data['text'])
                    else:
                        raise ValueError(f"Question validation failed for '{question_data['text']}': {str(e)}")
            
            if missing_questions:
                raise ValueError(f"Questions not found: {missing_questions}")
            
            # If all verifications pass, proceed with database operations
            for question_data in tqdm(questions_data, desc="Updating questions"):
                question_info = QuestionService.get_question_by_text(question_data['text'], session)
                question_id = question_info['id']
                QuestionService.edit_question(
                    question_id=question_id,
                    new_display_text=question_data.get('display_text'),
                    new_opts=question_data.get('options'),
                    new_default=question_data.get('default_option'),
                    session=session,
                    new_display_values=question_data.get('display_values'),
                    new_option_weights=question_data.get('option_weights')
                )
                print(f"Updated question: {question_data['text']}")
                
        except ValueError as e:
            session.rollback()
            raise ValueError(f"Error updating questions: {str(e)}")
        except Exception as e:
            session.rollback()
            raise Exception(f"Error updating questions: {str(e)}")


def update_question_groups(json_file_path: str = None) -> None:
    """
    Update existing question groups from a JSON file. All groups must exist in the database.
    
    Args:
        json_file_path: Path to the JSON file containing question group data
        
    Raises:
        ValueError: If any group not found or validation fails
        Exception: If database operations fail
    """
    # Load and parse JSON file
    with open(json_file_path, 'r') as f:
        groups_data = json.load(f)
    
    with SessionLocal() as session:
        try:
            # First verify all groups
            missing_groups = []
            validation_errors = []
            for group_data in groups_data:
                try:
                    # First get group by title to get its ID
                    group = QuestionGroupService.get_group_by_name(group_data['title'], session)
                    
                    # Then verify group parameters (verify function now returns None)
                    QuestionGroupService.verify_edit_group(
                        group_id=group.id,
                        new_title=group_data['title'],
                        new_description=group_data['description'],
                        is_reusable=group_data['is_reusable'],
                        verification_function=group_data.get('verification_function'),
                        is_auto_submit=group_data.get('is_auto_submit', False),
                        session=session
                    )
                except ValueError as e:
                    if "not found" in str(e):
                        missing_groups.append(group_data['title'])
                    else:
                        validation_errors.append(f"Group '{group_data['title']}': {str(e)}")
            
            # Report all validation errors
            if missing_groups:
                raise ValueError(f"Question groups do not exist: {', '.join(missing_groups)}")
            if validation_errors:
                raise ValueError("Validation errors:\n" + "\n".join(validation_errors))
            
            # If all verifications pass, proceed with database operations
            for group_data in tqdm(groups_data, desc="Updating question groups"):
                group = QuestionGroupService.get_group_by_name(group_data['title'], session)
                QuestionGroupService.edit_group(
                    group_id=group.id,
                    new_title=group_data['title'],
                    new_description=group_data['description'],
                    is_reusable=group_data['is_reusable'],
                    verification_function=group_data.get('verification_function'),
                    is_auto_submit=group_data.get('is_auto_submit', False),
                    session=session
                )
                print(f"Updated question group: {group_data['title']}")
                
        except ValueError as e:
            session.rollback()
            raise ValueError(f"Error updating question groups: {str(e)}")
        except Exception as e:
            session.rollback()
            raise Exception(f"Error updating question groups: {str(e)}")


def create_schema(schema_name: str, question_group_names: list):
    """
    Create a new schema with existing question groups
    
    Args:
        schema_name: Name of the schema to create
        question_group_names: List of question group names to include in the schema
        
    Returns:
        int: ID of the newly created schema
        
    Raises:
        ValueError: If any validation fails
        Exception: If database operations fail
    """
    with SessionLocal() as session:
        try:
            # Get question group IDs
            question_group_ids = []
            for group_name in question_group_names:
                group = QuestionGroupService.get_group_by_name(group_name, session)
                if not group:
                    raise ValueError(f"Question group '{group_name}' not found")
                question_group_ids.append(group.id)
            
            # Verify schema creation parameters
            SchemaService.verify_create_schema(schema_name, question_group_ids, session)
            
            # Create schema
            schema = SchemaService.create_schema(
                name=schema_name,
                question_group_ids=question_group_ids,
                session=session
            )
            print(f"Successfully created Schema: {schema.name}")
            return schema.id
            
        except Exception as e:
            raise Exception(f"Error creating schema: {str(e)}")



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