import argparse
import os

def run_label_pizza_setup(database_url_name, folder_path):
    """
    Run the complete label pizza setup process.
    Only processes files/folders that exist.
    
    Args:
        database_url_name (str): Database URL name
        folder_path (str): Base folder path containing all data files
    """
    # Initialize database (Important to do this before importing utils which uses the database session)
    from label_pizza.db import init_database
    init_database(database_url_name) # This will initialize the database; importantly to do this before importing utils which uses the database session

    # from label_pizza.upload_utils import upload_videos, upload_users, upload_question_groups, upload_schemas, create_projects, bulk_assign_users, batch_upload_annotations, batch_upload_reviews, apply_simple_video_configs

    from label_pizza.sync_utils import sync_videos
    sync_videos(videos_path=os.path.join(folder_path, "videos.json"))
    
    from label_pizza.sync_utils import sync_users
    sync_users(users_path=os.path.join(folder_path, "users.json"))
    
    from label_pizza.sync_utils import sync_question_groups
    sync_question_groups(question_groups_folder=os.path.join(folder_path, "question_groups"))
    
    from label_pizza.sync_utils import sync_schemas
    sync_schemas(schemas_path=os.path.join(folder_path, "schemas.json"))

    from label_pizza.sync_utils import sync_projects
    sync_projects(projects_path=os.path.join(folder_path, "projects.json"))
    
    from label_pizza.sync_utils import sync_project_groups
    sync_project_groups(project_groups_path=os.path.join(folder_path, "project_groups.json"))
    
    from label_pizza.sync_utils import sync_users_to_projects
    sync_users_to_projects(assignment_path=os.path.join(folder_path, "assignments.json"))
    
    from label_pizza.sync_utils import sync_annotations
    sync_annotations(annotations_folder=os.path.join(folder_path, "annotations"), max_workers=8)

    from label_pizza.sync_utils import sync_ground_truths
    sync_ground_truths(ground_truths_folder=os.path.join(folder_path, "ground_truths"), max_workers=10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--database-url-name", default="DBURL")
    parser.add_argument("--folder-path", default="./workspace", help="Folder path containing data files")
    args, _ = parser.parse_known_args()
    
    run_label_pizza_setup(args.database_url_name, args.folder_path)
