from label_pizza.db import init_database
init_database()
from label_pizza.upload_utils import *
from label_pizza.import_annotations import *

# 1. Upload videos
upload_videos(videos_path="./example/videos.json")

# 2. Upload Users
upload_users(users_path="./example/users.json")

# 3. Upload schemas
create_schemas(schemas_path="./example/schemas.json", question_groups_folder="./example/question_groups")

# # Optional: Create projects.json from annotations.
# assignments_data = []
# assignments_data.extend(get_project_from_annotations(
#     annotations_path="./example/annotations/subjects/subject_annotations.json", 
#     schema_name='Subjects in Video'
#     )
# )
# assignments_data.extend(get_project_from_annotations(
#     annotations_path="./example/annotations/weather/weather_annotations.json", 
#     schema_name='Weather in Video'
#     )
# )
# with open('./example/projects.json', 'w') as f:
#     json.dump(assignments_data, f, indent=2)

# 4. Create projects
create_projects(projects_path="./example/projects.json")

# Optional: Create assignments.json from annotations.

# 5. Bulk assign users
bulk_assign_users(assignment_path="./example/assignments.json")

# 6. Upload annotations
import_annotations(annotations_folder="./example/annotations")

# 7. Upload reviews
import_reviews(reviews_folder="./example/reviews")


