import json
from sqlalchemy.orm import Session
from tqdm import tqdm
from label_pizza.services import (
    VideoService, 
    ProjectService, 
    SchemaService, 
    QuestionGroupService, 
    QuestionService
)
from label_pizza.db import SessionLocal, engine

def update_or_add_videos(json_file_path: str = '../new_video_metadata.json'):
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

def import_schemas(json_file_path: str = '../lighting_schema_questions.json'):
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

def create_project_with_videos(json_file_path: str, project_base_name: str, schema_name: str, batch_size: int = 50):
    """
    Create project from JSON file, all videos must exist in database
    
    Args:
        json_file_path: Path to JSON file
        project_name: Name of the project
        schema_name: Name of the schema
        batch_size: Number of videos per project
    """
    with open(json_file_path, 'r') as f:
        videos_data = json.load(f)
    
    session = SessionLocal()
    try:
        try:
            schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
            print(f"Found schema '{schema_name}', ID: {schema_id}")
        except ValueError as e:
            print(f"Error: {str(e)}")
            return
        
        video_uids = [video['video_uid'] for video in videos_data]
        all_videos_df = VideoService.get_all_videos(session)
        existing_video_uids = set(all_videos_df['Video UID'])
        missing_videos = [uid for uid in video_uids if uid not in existing_video_uids]

        if missing_videos:
            error_msg = f"Found {len(missing_videos)} videos not in database:\n"
            raise ValueError(error_msg)
        
        video_ids = ProjectService.get_video_ids_by_uids(video_uids, session)
        
        total_videos = len(video_ids)
        for i in range(0, total_videos, batch_size):
            batch_video_ids = video_ids[i:i + batch_size]
            project_name_with_batch = f"{project_base_name}-{i//batch_size + 1}"
            
            print(f"Creating project {project_name_with_batch}...")
            try:
                ProjectService.create_project(
                    name=project_name_with_batch,
                    schema_id=schema_id,
                    video_ids=batch_video_ids,
                    session=session
                )
                print(f"Successfully created project {project_name_with_batch}!")
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