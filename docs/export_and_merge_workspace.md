# Label Pizza - Export, Compare, and Merge Workspace Workflows

This guide explains how to export, compare, and merge Label Pizza workspace data. These workflows help you create backups, validate migrations, compare different workspace states, and combine data from multiple sources.

## Getting started with workspace management

The workspace management workflow typically follows this pattern:

| Step        | Goal                                    | Quick Example                                                | What happens                                                 |
| ----------- | --------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **Export**  | Create workspace backup or extract data | `export_workspace(output_folder="./folder1")`                | Exports all database content to JSON files in a structured folder |
| **Compare** | Identify differences between workspaces | `compare_workspace("./folder1", "./folder2", write_to_file=True)` | Generates detailed diff reports showing what changed         |
| **Merge**   | Combine data from multiple sources      | `merge_workspace("./folder1", "./folder2", "./output", use_first_folder_on_conflict=True)` | Combines two workspaces by choosing which folder's data to keep when the same item exists in both folders |

## API Reference

| File / Folder           | Export                                  | Compare                                                      | Merge                                                        |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **users.json**          | `export_users(output_path)`             | `compare_videos(folder1_path, folder2_path, write_to_file)`  | `merge_users(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **videos.json**         | `export_videos(output_path)`            | `compare_users(folder1_path, folder2_path, write_to_file)`   | `merge_videos(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **question_groups/**    | `export_question_groups(output_folder)` | `compare_question_groups(folder1_path, folder2_path, write_to_file)` | `merge_question_groups(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **schemas.json**        | `export_schemas(output_path)`           | `compare_schemas(folder1_path, folder2_path, write_to_file)` | `merge_schemas(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **projects.json**       | `export_projects(output_path)`          | `compare_projects(folder1_path, folder2_path, write_to_file)` | `merge_projects(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **project_groups.json** | `export_project_groups(output_path)`    | `compare_project_groups(folder1_path, folder2_path, write_to_file)` | `merge_project_groups(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **assignments.json**    | `export_assignments(output_path)`       | `compare_assignments(folder1_path, folder2_path, write_to_file)` | `merge_assignments(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **annotations/**        | `export_annotations(output_folder)`     | `compare_annotations(folder1_path, folder2_path, write_to_file)` | `merge_annotations(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **ground_truths/**      | `export_ground_truths(output_folder)`   | `compare_ground_truths(folder1_path, folder2_path, write_to_file)` | `merge_ground_truths(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |
| **The whole folder**    | `export_workspace(output_folder)`       | `compare_workspace(folder1_path, folder2_path, write_to_file)` | `merge_workspace(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)` |



### Common use cases

- **Regular backups**: Export your workspace periodically for disaster recovery
- **Team collaboration**: Merge annotation work from multiple teams
- **Compare two workspace folders**: Compare two workspace folders to see what changed
- **Data analysis**: Export data to view in readable JSON format

## Export Functions

> If you haven't already synced `./example` to the database, backup your existing database or reset your database to empty state first, then run:
> `python sync_from_folder.py --folder-path ./example`

Export functions allow you to extract data from your Label Pizza database and save it as JSON files. These functions create JSON files that can be stored in version control or moved between databases.

### 1. Complete Workspace Export

#### `export_workspace`

Export entire workspace to a structured folder in a single operation.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_workspace

# Export entire workspace to workspace folder
export_workspace(output_folder="./workspace")

# Output: Creates complete workspace structure:
# workspace/
# ├── users.json
# ├── videos.json
# ├── schemas.json
# ├── projects.json
# ├── project_groups.json
# ├── assignments.json
# ├── question_groups/
# │   ├── Human.json
# │   └── Pizza.json
# │   └── NSFW.json
# ├── annotations/
# │   ├── Human_annotations.json
# │   └── Pizza_annotations.json
# │   └── NSFW_annotations.json
# └── ground_truths/
#     ├── Human_ground_truths.json
#     └── Pizza_ground_truths.json
#     └── NSFW_ground_truths.json
```

### 2. Individual Export Functions

#### `export_users`

Export all users including their authentication and status information.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_users

# Export all users to users.json
export_users(output_path="./workspace/users.json")

# Output: users.json containing user data with user_id, email, password, user_type, is_active
```

#### `export_videos`

Export all videos with their URLs, metadata, and status.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_videos

export_videos(output_path="./workspace/videos.json")

# Output: videos.json containing video_uid, url, video_metadata, is_active
```

#### `export_question_groups`

Export all question groups to separate JSON files in a folder.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_question_groups

export_question_groups(
    output_folder="./workspace/question_groups"
)

# Output: Creates question_groups/ folder with individual JSON files
# Each file contains title, display_title, description, questions array, etc.
```

#### `export_schemas`

Export all schemas with their associated question groups.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_schemas

export_schemas(output_path="./workspace/schemas.json")

# Output: schemas.json containing schema_name, instructions_url, question_group_names
```

#### `export_projects`

Export all projects with their associated schemas and videos.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_projects

export_projects(output_path="./workspace/projects.json")

# Output: projects.json containing project_name, description, schema_name, videos
```

#### `export_project_groups`

Export all project groups with their associated projects.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_project_groups

export_project_groups(
    output_path="./workspace/project_groups.json"
)

# Output: project_groups.json containing project_group_name, description, projects
```

#### `export_assignments`

Export all user project role assignments.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_assignments

export_assignments(output_path="./workspace/assignments.json")

# Output: assignments.json containing user_name, project_name, role, user_weight, is_active
```

#### `export_annotations`

Export all annotations grouped by question group to separate JSON files.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_annotations

export_annotations(output_folder="./workspace/annotations")

# Output: Creates annotations/ folder with files like Safety_Assessment_annotations.json
# Each file contains question_group_title, project_name, user_name, video_uid, answers
```

#### `export_ground_truths`

Export all ground truth annotations grouped by question group.

```python
from label_pizza.db import init_database
init_database("DBURL")

from label_pizza.export_utils import export_ground_truths

export_ground_truths(output_folder="./workspace/ground_truths")

# Output: Creates ground_truths/ folder with reviewer-verified answers
```

## Compare Functions

Compare functions help you identify differences between two Label Pizza workspace exports. These functions generate detailed diff reports showing what changed between different workspace states.

### 1. Complete Workspace Compare

#### `compare_workspace`

Compare all components of two workspace folders in a single operation.

```python
from label_pizza.compare_utils import compare_workspace

# Compare entire workspaces between example and workspace
is_identical = compare_workspace(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

print(f"Workspaces are identical: {is_identical}")

# Generates all diff files comparing example vs workspace:
# diff_videos.json, diff_users.json, diff_schemas.json, diff_projects.json,
# diff_project_groups.json, diff_assignments.json, diff_question_groups.json,
# diff_annotations.json, diff_ground_truths.json
```

### 2. Individual Compare Functions

Each compare function can be used in two ways:

- **Basic comparison**: Returns boolean indicating if workspaces are identical
- **Detailed diff report**: Generates JSON diff file with complete analysis

#### `compare_videos`

Compare videos.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_videos

# Basic comparison - returns True/False
is_identical = compare_videos(folder1_path="./example", folder2_path="./workspace")
print(f"Videos are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_videos(
    folder1_path="./example",
    folder2_path="./workspace", 
    write_to_file=True
)

# Creates diff_videos.json with:
# {
#   "identical": false,
#   "folder1_only": [...],     # Videos only in workspace
#   "folder2_only": [...],     # Videos only in workspace_export  
#   "different": [...],        # Videos with same video_uid but different content
#   "summary": {
#     "folder1_count": 10,
#     "folder2_count": 12,
#     "differences_found": 3
#   }
# }
```

#### `compare_users`

Compare users.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_users

# Basic comparison
is_identical = compare_users(folder1_path="./example", folder2_path="./workspace")
print(f"Users are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_users(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

# Creates diff_users.json with users that differ by user_id
```

#### `compare_schemas`

Compare schemas.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_schemas

# Basic comparison
is_identical = compare_schemas(folder1_path="./example", folder2_path="./workspace")
print(f"Schemas are identical: {is_identical}")

# Generate detailed diff report  
is_identical = compare_schemas(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

# Creates diff_schemas.json with schemas that differ by schema_name
```

#### `compare_projects`

Compare projects.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_projects

# Basic comparison
is_identical = compare_projects(folder1_path="./example", folder2_path="./workspace")
print(f"Projects are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_projects(
    folder1_path="./example", 
    folder2_path="./workspace",
    write_to_file=True
)

# Creates diff_projects.json with projects that differ by project_name
```

#### `compare_project_groups`

Compare project_groups.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_project_groups

# Basic comparison
is_identical = compare_project_groups(
  folder1_path="./example", 
  folder2_path="./workspace"
)
print(f"Project groups are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_project_groups(
    folder1_path="./example",
    folder2_path="./workspace", 
    write_to_file=True
)

# Creates diff_project_groups.json with project groups that differ by project_group_name
```

#### `compare_assignments`

Compare assignments.json files between two workspace folders.

```python
from label_pizza.compare_utils import compare_assignments

# Basic comparison
is_identical = compare_assignments(folder1_path="./example", folder2_path="./workspace")
print(f"Assignments are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_assignments(
    folder1_path="./example",
    folder2_path="./workspace", 
    write_to_file=True
)

# Creates diff_assignments.json with assignments that differ by user_name + project_name + role
```

#### `compare_question_groups`

Compare question_groups folders between two workspace folders.

```python
from label_pizza.compare_utils import compare_question_groups

# Basic comparison
is_identical = compare_question_groups(
  folder1_path="./example", 
  folder2_path="./workspace"
)
print(f"Question groups are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_question_groups(
    folder1_path="./example",
    folder2_path="./workspace", 
    write_to_file=True
)

# Creates diff_question_groups.json comparing all JSON files using title as unique identifier
```

#### `compare_annotations`

Compare annotations folders between two workspace folders.

```python
from label_pizza.compare_utils import compare_annotations

# Basic comparison
is_identical = compare_annotations(folder1_path="./example", folder2_path="./workspace")
print(f"Annotations are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_annotations(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

# Creates diff_annotations.json with annotations that differ by:
# video_uid + project_name + question_group_title + user_name
```

#### `compare_ground_truths`

Compare ground_truths folders between two workspace folders.

```python
from label_pizza.compare_utils import compare_ground_truths

# Basic comparison
is_identical = compare_ground_truths(folder1_path="./example", folder2_path="./workspace")
print(f"Ground truths are identical: {is_identical}")

# Generate detailed diff report
is_identical = compare_ground_truths(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

# Creates diff_ground_truths.json with ground truths that differ by:
# video_uid + project_name + question_group_title + user_name
```

### 3. Understanding Diff Reports

Each diff report contains:

- **identical**: Boolean indicating if data is identical
- **folder1_only**: Items that exist only in the first folder
- **folder2_only**: Items that exist only in the second folder
- **different**: Items that exist in both but have different content
- **summary**: Count statistics and differences found

## Merge Functions

Merge functions allow you to combine data from two Label Pizza workspace folders by choosing which folder's data to keep when the same item exists in both folders.

### Understanding Merge Conflicts

Each data type uses its appropriate unique key for identifying duplicates:

| Data Type               | Unique Key(s)                                                |
| ----------------------- | ------------------------------------------------------------ |
| **videos.json**         | `video_uid`                                                  |
| **users.json**          | `user_id`                                                    |
| **schemas.json**        | `schema_name`                                                |
| **projects.json**       | `project_name`                                               |
| **project_groups.json** | `project_group_name`                                         |
| **assignments.json**    | `user_name + project_name + role` (composite)                |
| **question_groups**     | `title`                                                      |
| **annotations**         | `video_uid + project_name + question_group_title + user_name` (composite) |
| **ground_truths**       | `video_uid + project_name + question_group_title + user_name` (composite) |

### Conflict Resolution Strategy

- **use_first_folder_on_conflict=True**: Use folder1's data when conflicts occur
- **use_first_folder_on_conflict=False**: Use folder2's data when conflicts occur
- **No conflicts**: Items that exist in only one folder are always included

### 1. Complete Workspace Merge

#### `merge_workspace`

Merge all components of two workspace folders in a single operation.

```python
from label_pizza.merge_utils import merge_workspace

# Merge entire workspaces using folder1 (example) as ground truth
merge_report = merge_workspace(
    folder1_path="./example",
    folder2_path="./workspace", 
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True  # Use folder1 (example) data for conflicts
)

# Output: Creates complete merged workspace structure:
# merged_workspace/
# ├── users.json
# ├── videos.json
# ├── schemas.json
# ├── projects.json
# ├── project_groups.json
# ├── assignments.json
# ├── question_groups/
# │   ├── Safety_Assessment.json
# │   └── Quality_Review.json
# ├── annotations/
# │   ├── Safety_Assessment_annotations.json
# │   └── Quality_Review_annotations.json
# └── ground_truths/
#     ├── Safety_Assessment_ground_truths.json
#     └── Quality_Review_ground_truths.json

print(f"Total conflicts resolved: {merge_report['summary']['total_conflicts_resolved']}")
```

### 2. Individual Merge Functions

#### `merge_videos`

Merge videos.json files from two folders.

```python
from label_pizza.merge_utils import merge_videos

# Merge videos from example and workspace, using folder1 (example) as ground truth for conflicts
merge_report = merge_videos(
    folder1_path="./example",
    folder2_path="./workspace", 
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True  # Use folder1 (example) data when conflicts occur
)

print(f"Merged {merge_report['merged_count']} videos")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Merge report structure:
# {
#   "merged_count": 15,           # Total videos in merged file
#   "folder1_count": 10,          # Videos in example
#   "folder2_count": 8,           # Videos in workspace
#   "conflicts_resolved": 3,      # Videos that existed in both with differences
#   "used_folder1_on_conflict": True  # Which folder was used for conflict resolution
# }
```

#### `merge_users`

```python
from label_pizza.merge_utils import merge_users

# Basic merge
merge_report = merge_users(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} users")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates users.json in the output folder
```

#### `merge_schemas`

Merge schemas.json files from two folders.

```python
from label_pizza.merge_utils import merge_schemas

# Basic merge
merge_report = merge_schemas(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} schemas")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates schemas.json in the output folder
```

#### `merge_projects`

Merge projects.json files from two folders.

```python
from label_pizza.merge_utils import merge_projects

# Basic merge
merge_report = merge_projects(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} projects")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates projects.json in the output folder
```

#### `merge_project_groups`

Merge project_groups.json files from two folders.

```python
from label_pizza.merge_utils import merge_project_groups

# Basic merge
merge_report = merge_project_groups(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} project groups")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates project_groups.json in the output folder
```

#### `merge_assignments`

Merge assignments.json files from two folders.

```python
from label_pizza.merge_utils import merge_assignments

# Basic merge
merge_report = merge_assignments(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} assignments")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates assignments.json in the output folder
```

#### `merge_question_groups`

Merge question_groups folders from two folders.

```python
from label_pizza.merge_utils import merge_question_groups

# Basic merge
merge_report = merge_question_groups(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} question groups")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates question_groups/ folder in the output folder with individual JSON files
```

#### `merge_annotations`

Merge annotations folders from two folders.

```python
from label_pizza.merge_utils import merge_annotations

# Basic merge
merge_report = merge_annotations(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} annotations")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates annotations/ folder in the output folder with files grouped by question group
```

#### `merge_ground_truths`

Merge ground_truths folders from two folders.

```python
from label_pizza.merge_utils import merge_ground_truths

# Basic merge
merge_report = merge_ground_truths(
    folder1_path="./example",
    folder2_path="./workspace",
    output_folder="./merged_workspace",
    use_first_folder_on_conflict=True
)

print(f"Merged {merge_report['merged_count']} ground truths")
print(f"Resolved {merge_report['conflicts_resolved']} conflicts")

# Creates ground_truths/ folder in the output folder with files grouped by question group
```

### 3. Understanding Merge Reports

The workspace merge returns detailed statistics for all components:

```python
merge_report = merge_workspace("./example", "./workspace", "./merged", use_first_folder_on_conflict=False)

# Complete merge report structure:
# {
#   "videos": {
#     "merged_count": 15,
#     "folder1_count": 10,  
#     "folder2_count": 8,
#     "conflicts_resolved": 3,
#     "used_folder1_on_conflict": False
#   },
#   "users": {
#     "merged_count": 12,
#     "folder1_count": 8,
#     "folder2_count": 6,
#     "conflicts_resolved": 2,
#     "used_folder1_on_conflict": False
#   },
#   ... // Similar structure for all components
#   "summary": {
#     "total_conflicts_resolved": 15,
#     "used_folder1_on_conflict": False,
#     "merge_completed": True
#   }
# }
```

## Complete Workspace Management Workflow

Here's a complete example showing the export → compare → merge workflow:

### Step 1: Export current workspace

```python
from label_pizza.db import init_database
from label_pizza.export_utils import export_workspace

init_database("DBURL")

# Export current database state
export_workspace(output_folder="./workspace")
print("Current workspace exported to ./workspace")
```

### Step 2: Compare with existing workspace

```python
from label_pizza.compare_utils import compare_workspace

# Compare existing workspace with exported data
is_identical = compare_workspace(
    folder1_path="./example",
    folder2_path="./workspace",
    write_to_file=True
)

if is_identical:
    print("Workspaces are identical - no merge needed")
else:
    print("Differences found - review diff files before merging")
    # Review diff_*.json files to understand differences
```

### Step 3: Merge workspaces if needed

```python
from label_pizza.merge_utils import merge_workspace

# Merge workspaces, using exported data as ground truth for conflicts
merge_report = merge_workspace(
    folder1_path="./example_custom_display",
    folder2_path="./workspace",
    output_folder="./merged_workspace", 
    use_first_folder_on_conflict=True  # Use folder1 (exported) data for conflicts
)

print(f"Merge completed with {merge_report['summary']['total_conflicts_resolved']} conflicts resolved")

# The merged_workspace folder now contains the combined data
# You can use sync_from_folder.py to load this back into the database
```

### Step 4: Sync merged workspace back to database (optional)

```bash
# Load the merged workspace back into the database
python sync_from_folder.py --folder-path ./merged_workspace
```

## Important Notes

### File Naming Conventions

- Question group files use the title with spaces and special characters replaced with underscores
- Annotation and ground truth files append `_annotations` and `_ground_truths` respectively
- All files use UTF-8 encoding and pretty-printed JSON formatting

### Error Handling

- Missing folders or files are treated as empty datasets
- Invalid JSON files are skipped with error messages
- Output directories are created automatically if they don't exist

### Order Dependencies

When working with individual functions, follow proper dependency order:

**Export/Merge order**: videos → users → question groups → schemas → projects → project groups → assignments → annotations/ground truths

**Archive order**: annotations/ground truths → assignments → projects → schemas → question groups → videos

### Performance Considerations

- Full workspace operations can be slow on large datasets
- Use individual functions for selective operations on specific data types
- Consider breaking large workspaces into smaller chunks for better performance

------

[← Back to Workspace Management](workspace_management.md) | [Next → Custom Display](custom_display.md)