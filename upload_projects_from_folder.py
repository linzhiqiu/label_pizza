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

    from label_pizza.upload_utils import upload_videos, upload_users, upload_question_groups, upload_schemas, create_projects, bulk_assign_users, batch_upload_annotations, batch_upload_reviews

    # 1. Upload videos
    videos_path = os.path.join(folder_path, "videos.json")
    if os.path.exists(videos_path):
        print(f"Uploading videos from {videos_path}")
        upload_videos(videos_path=videos_path)
    else:
        print(f"Skipping videos upload - {videos_path} not found")

    # 2. Upload Users
    users_path = os.path.join(folder_path, "users.json")
    if os.path.exists(users_path):
        print(f"Uploading users from {users_path}")
        upload_users(users_path=users_path)
    else:
        print(f"Skipping users upload - {users_path} not found")

    # 3. Upload Question Groups
    question_groups_folder = os.path.join(folder_path, "question_groups")
    if os.path.exists(question_groups_folder):
        print(f"Uploading question groups from {question_groups_folder}")
        upload_question_groups(question_groups_folder=question_groups_folder)
    else:
        print(f"Skipping question groups upload - {question_groups_folder} not found")

    # 4. Upload schemas
    schemas_path = os.path.join(folder_path, "schemas.json")
    if os.path.exists(schemas_path) and os.path.exists(question_groups_folder):
        print(f"Creating schemas from {schemas_path} and {question_groups_folder}")
        upload_schemas(
            schemas_path=schemas_path
        )
    else:
        print(f"Skipping schemas creation - missing {schemas_path} or {question_groups_folder}")

    # 4. Create projects
    projects_path = os.path.join(folder_path, "projects.json")
    if os.path.exists(projects_path):
        print(f"Creating projects from {projects_path}")
        create_projects(projects_path=projects_path)
    else:
        print(f"Skipping projects creation - {projects_path} not found")

    # Optional: Create assignments.json from annotations.

    # 5. Bulk assign users
    assignment_path = os.path.join(folder_path, "assignments.json")
    if os.path.exists(assignment_path):
        print(f"Bulk assigning users from {assignment_path}")
        bulk_assign_users(assignment_path=assignment_path)
    else:
        print(f"Skipping bulk assign users - {assignment_path} not found")

    # 6. Upload annotations
    annotations_folder = os.path.join(folder_path, "annotations")
    if os.path.exists(annotations_folder):
        print(f"Importing annotations from {annotations_folder}")
        batch_upload_annotations(annotations_folder=annotations_folder)
    else:
        print(f"Skipping annotations import - {annotations_folder} not found")

    # 7. Upload reviews
    reviews_folder = os.path.join(folder_path, "reviews")
    if os.path.exists(reviews_folder):
        print(f"Importing reviews from {reviews_folder}")
        batch_upload_reviews(reviews_folder=reviews_folder)
    else:
        print(f"Skipping reviews import - {reviews_folder} not found")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--database-url-name", default="DBURL")
    parser.add_argument("--folder-path", default="./example", help="Folder path containing data files")
    args, _ = parser.parse_known_args()
    
    run_label_pizza_setup(args.database_url_name, args.folder_path)