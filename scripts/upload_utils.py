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

def update_or_add_videos(json_file_path: str = None):
    """
    Update existing videos or add new videos from JSON file
    
    Args:
        json_file_path: Path to the JSON file containing video data
    """
    with open(json_file_path, 'r') as f:
        videos_data = json.load(f)
    
    with Session(engine) as session:
        for video_data in tqdm(videos_data, desc="Processing videos", unit="video"):
            try:
                existing_video = VideoService.get_video_by_uid(video_data['video_uid'], session)

                if existing_video:
                    VideoService.update_video(
                        video_uid=video_data['video_uid'],
                        new_url=video_data['url'],
                        new_metadata=video_data['metadata'],
                        session=session
                    )
                    print(f"Successfully updated video: {video_data['video_uid']}")
                else:
                    VideoService.add_video(
                        url=video_data['url'],
                        session=session,
                        metadata=video_data['metadata']
                    )
                    print(f"Successfully added new video: {video_data['video_uid']}")
                    
            except Exception as e:
                print(f"Error processing video {video_data['video_uid']}: {str(e)}")
                session.rollback()
                continue
        
        try:
            session.commit()
            print("All videos have been successfully processed!")
        except Exception as e:
            print(f"Error committing changes: {str(e)}")
            session.rollback()

def import_schemas(json_file_path: str = None):
    """
    Import schemas and their questions from JSON file
    
    Args:
        json_file_path: Path to the JSON file containing schema data
    """
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    with SessionLocal() as session:
        for schema_name, question_groups in tqdm(data.items(), desc="Processing Schemas"):
            print(f"\nProcessing Schema: {schema_name}")
            
            try:
                existing_schema = SchemaService.get_schema_by_name(schema_name, session)
                print(f"Found existing Schema: {schema_name}")
            except:
                existing_schema = None
                print(f"Creating new Schema: {schema_name}")
            
            schema_question_group_ids = []
            
            for group_data in tqdm(question_groups, desc="Processing Question Groups"):
                print(f"\nProcessing Question Group: {group_data['title']}")
                
                try:
                    existing_group = QuestionGroupService.get_group_by_name(group_data['title'], session)
                    print(f"Found existing Question Group: {group_data['title']}")
                except:
                    existing_group = None
                    print(f"Creating new Question Group: {group_data['title']}")
                
                question_ids = []
                for question_data in tqdm(group_data['questions'], desc="Processing questions"):
                    try:
                        try:
                            existing_question = QuestionService.get_question_by_text(question_data['text'], session)
                            question_exists = True
                        except:
                            existing_question = None
                            question_exists = False
                        
                        if question_exists:
                            print(f"Updating existing question: {question_data['text']}")
                            if existing_question['type'] == 'description':
                                QuestionService.edit_question(
                                    question_id=existing_question['id'],
                                    new_display_text=question_data['display_text'],
                                    new_opts=None,
                                    new_default=None,
                                    new_display_values=None,
                                    session=session
                                )
                            else:
                                QuestionService.edit_question(
                                    question_id=existing_question['id'],
                                    new_display_text=question_data['display_text'],
                                    new_opts=question_data['options'],
                                    new_default=question_data.get('default_option'),
                                    new_display_values=question_data['display_values'],
                                    session=session
                                )
                            question_ids.append(existing_question['id'])
                        else:
                            print(f"Creating new question: {question_data['text']}")
                            if question_data['qtype'] == 'single':
                                question = QuestionService.add_question(
                                    text=question_data['text'],
                                    qtype=question_data['qtype'],
                                    options=question_data['options'],
                                    display_values=question_data['display_values'],
                                    display_text=question_data['display_text'],
                                    default=question_data.get('default_option'),
                                    session=session
                                )
                            elif question_data['qtype'] == 'description':
                                question = QuestionService.add_question(
                                    text=question_data['text'],
                                    qtype=question_data['qtype'],
                                    options=[],
                                    default=None,
                                    display_values=[],
                                    display_text=question_data['display_text'],
                                    session=session
                                )
                            question_ids.append(question.id)
                    except Exception as e:
                        print(f"Error processing question {question_data['text']}: {str(e)}")
                
                try:
                    if existing_group:
                        QuestionGroupService.edit_group(
                            group_id=existing_group.id,
                            new_title=group_data['title'],
                            new_description=group_data['description'],
                            is_reusable=group_data['is_reusable'],
                            verification_function="",
                            session=session
                        )
                        schema_question_group_ids.append(existing_group.id)
                        print(f"Successfully updated question group: {group_data['title']}")
                    else:
                        question_group = QuestionGroupService.create_group(
                            title=group_data['title'],
                            description=group_data['description'],
                            is_reusable=group_data['is_reusable'],
                            verification_function="",
                            question_ids=question_ids,
                            is_auto_submit=group_data['is_auto_submit'],
                            session=session
                        )
                        schema_question_group_ids.append(question_group.id)
                        print(f"Successfully created question group: {group_data['title']}")
                except Exception as e:
                    print(f"Error processing question group: {str(e)}")
            
            try:
                if existing_schema:
                    print(f"\nArchiving old Schema: {schema_name}")
                    SchemaService.archive_schema(existing_schema.id, session)
                
                schema = SchemaService.create_schema(
                    name=schema_name,
                    question_group_ids=schema_question_group_ids,
                    session=session
                )
                print(f"\nSuccessfully created Schema: {schema.name}")
            except Exception as e:
                print(f"\nError processing Schema: {str(e)}")



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