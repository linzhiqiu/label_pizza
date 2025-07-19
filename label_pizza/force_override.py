from label_pizza.db import init_database
init_database()
from label_pizza.models import *
from label_pizza.db import SessionLocal
import os
import datetime
import glob
from pathlib import Path
from typing import Optional

# Import backup functionality from init_db
try:
    from label_pizza.backup_restore import DatabaseBackupRestore
    BACKUP_AVAILABLE = True
except ImportError:
    print("⚠️  backup_restore.py not found. Backup functionality disabled.")
    BACKUP_AVAILABLE = False

# Backup configuration
BACKUP_DIR = "./db_backups"
MAX_BACKUPS = 10


def create_backup_if_requested(db_url: str, backup_dir: str = "./db_backups", 
                             backup_file: Optional[str] = None, compress: bool = True) -> Optional[str]:
    """Create a backup before operations if requested"""
    if not BACKUP_AVAILABLE:
        print("❌ Backup functionality not available (backup_restore.py not found)")
        return None
        
    try:
        handler = DatabaseBackupRestore(db_url)
        
        # Create backup directory if it doesn't exist
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True)
        
        # Handle output filename
        if backup_file is None:
            # Auto-generate timestamped filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            extension = ".sql.gz" if compress else ".sql"
            backup_file = f"backup_{timestamp}{extension}"
        
        # If backup_file is just a filename (no path separator), combine with backup_dir
        if not os.path.sep in backup_file and not os.path.isabs(backup_file):
            output_file = str(backup_path / backup_file)
        else:
            output_file = backup_file
        
        print(f"💾 Creating backup: {backup_file}")
        
        success = handler.create_backup(
            output_file=output_file,
            compress=compress,
            schema_only=False
        )
        
        if success:
            print(f"   ✅ Backup created: {output_file}")
            _rotate_backups()
            return output_file
        else:
            print("   ❌ Backup failed")
            return None
            
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return None

def _rotate_backups():
    """Keep only the latest MAX_BACKUPS backups"""
    try:
        backup_pattern = str(Path(BACKUP_DIR) / "backup_*.sql*")
        backup_files = glob.glob(backup_pattern)
        
        if len(backup_files) <= MAX_BACKUPS:
            return
        
        # Sort by modification time (newest first)
        backup_files.sort(key=os.path.getmtime, reverse=True)
        
        # Remove old backups
        files_to_remove = backup_files[MAX_BACKUPS:]
        for old_backup in files_to_remove:
            try:
                os.remove(old_backup)
                print(f"   🗑️  Removed old backup: {os.path.basename(old_backup)}")
            except Exception as e:
                print(f"   ⚠️  Could not remove {old_backup}: {e}")
                
    except Exception as e:
        print(f"⚠️  Backup rotation error: {e}")

def _create_operation_backup(operation_name: str) -> Optional[str]:
    """Create a backup for a specific operation"""
    db_url = os.getenv("DBURL")
    if not db_url:
        print("⚠️  DBURL not found, skipping backup")
        return None
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"backup_before_{operation_name}_{timestamp}.sql.gz"
    
    return create_backup_if_requested(
        db_url=db_url,
        backup_dir=BACKUP_DIR,
        backup_file=backup_filename,
        compress=True
    )


def change_question_text(original_text, new_text, new_display_text=None):
    """Update question text with automatic backup"""
    # Create backup before making changes
    backup_path = _create_operation_backup(f"change_question_{original_text.replace(' ', '_')}")
    
    if new_display_text is None:
        new_display_text = new_text
        
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.text == original_text).first()
        if not question:
            raise ValueError(f"Question '{original_text}' not found")
        if session.query(Question).filter(Question.text == new_text).first():
            raise ValueError(f"Question '{new_text}' already exists")
        
        question.text = new_text
        question.display_text = new_display_text
        session.commit()
        
def update_question_group_titles(group_id: int, new_title: str, new_display_title: str = None):
    """Update QuestionGroup title and display_title with automatic backup"""
    # Create backup before making changes
    backup_path = _create_operation_backup(f"update_qgroup_{group_id}")
    
    if new_display_title is None:
        new_display_title = new_title
    
    with SessionLocal() as session:
        group = session.query(QuestionGroup).filter(QuestionGroup.id == group_id).first()
        if not group:
            raise ValueError(f"QuestionGroup {group_id} not found")
        
        existing = session.query(QuestionGroup).filter(QuestionGroup.title == new_title).first()
        if existing and existing.id != group_id:
            raise ValueError(f"Title '{new_title}' already exists")
        
        group.title = new_title
        group.display_title = new_display_title
        session.commit()


"""
All the check functions that are used to check what would be deleted when running the delete functions.
"""
def check_user_by_id(user_id: int) -> dict:
    """Check what would be deleted for user by ID"""
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            print(f"❌ User with ID {user_id} not found")
            return None
        
        print(f"=== USER {user_id} DELETION PREVIEW ===")
        print(f"User: {user.user_id_str} ({user.email})")
        print(f"Type: {user.user_type}")
        print(f"Created: {user.created_at}")
        print(f"Archived: {user.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  User record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "user_info": {
                "id": user_id,
                "user_id_str": user.user_id_str,
                "email": user.email,
                "user_type": user.user_type,
                "is_archived": user.is_archived
            },
            "total_records": 1
        }


def check_user_by_user_id_str(user_id_str: str) -> dict:
    """Check what would be deleted for user by user_id_str"""
    with SessionLocal() as session:
        user = session.query(User).filter(User.user_id_str == user_id_str).first()
        if not user:
            print(f"❌ User with user_id_str '{user_id_str}' not found")
            return None
        
        return check_user_by_id(user.id)


def check_video_by_id(video_id: int) -> dict:
    """Check what would be deleted for video by ID"""
    with SessionLocal() as session:
        video = session.query(Video).filter(Video.id == video_id).first()
        if not video:
            print(f"❌ Video with ID {video_id} not found")
            return None
        
        print(f"=== VIDEO {video_id} DELETION PREVIEW ===")
        print(f"Video: {video.video_uid}")
        print(f"URL: {video.url}")
        print(f"Created: {video.created_at}")
        print(f"Archived: {video.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Video record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "video_info": {
                "id": video_id,
                "video_uid": video.video_uid,
                "url": video.url,
                "is_archived": video.is_archived
            },
            "total_records": 1
        }


def check_video_by_uid(video_uid: str) -> dict:
    """Check what would be deleted for video by video_uid"""
    with SessionLocal() as session:
        video = session.query(Video).filter(Video.video_uid == video_uid).first()
        if not video:
            print(f"❌ Video with video_uid '{video_uid}' not found")
            return None
        
        return check_video_by_id(video.id)


def check_video_tag_by_video_id(video_id: int) -> dict:
    """Check what would be deleted for video tags by video_id"""
    with SessionLocal() as session:
        tags = session.query(VideoTag).filter(VideoTag.video_id == video_id).all()
        tag_count = len(tags)
        
        print(f"=== VIDEO TAGS FOR VIDEO {video_id} DELETION PREVIEW ===")
        if tag_count == 0:
            print("❌ No video tags found")
            return None
        
        print(f"Video ID: {video_id}")
        print(f"Tags: {[tag.tag for tag in tags]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Video tag records: {tag_count}")
        print(f"  📈 TOTAL RECORDS: {tag_count}")
        
        return {
            "video_id": video_id,
            "tags": [{"tag": tag.tag, "source": tag.tag_source} for tag in tags],
            "total_records": tag_count
        }


def check_question_group_by_id(question_group_id: int) -> dict:
    """Check what would be deleted for question group by ID"""
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.id == question_group_id).first()
        if not qg:
            print(f"❌ QuestionGroup with ID {question_group_id} not found")
            return None
        
        print(f"=== QUESTION GROUP {question_group_id} DELETION PREVIEW ===")
        print(f"Question Group: {qg.title}")
        print(f"Display Title: {qg.display_title}")
        print(f"Reusable: {qg.is_reusable}")
        print(f"Archived: {qg.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Question group record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "question_group_info": {
                "id": question_group_id,
                "title": qg.title,
                "display_title": qg.display_title,
                "is_reusable": qg.is_reusable,
                "is_archived": qg.is_archived
            },
            "total_records": 1
        }


def check_question_group_by_title(title: str) -> dict:
    """Check what would be deleted for question group by title"""
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.title == title).first()
        if not qg:
            print(f"❌ QuestionGroup with title '{title}' not found")
            return None
        
        return check_question_group_by_id(qg.id)


def check_question_by_id(question_id: int) -> dict:
    """Check what would be deleted for question by ID"""
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            print(f"❌ Question with ID {question_id} not found")
            return None
        
        print(f"=== QUESTION {question_id} DELETION PREVIEW ===")
        print(f"Question: {question.text}")
        print(f"Display Text: {question.display_text}")
        print(f"Type: {question.type}")
        print(f"Options: {question.options}")
        print(f"Archived: {question.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Question record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "question_info": {
                "id": question_id,
                "text": question.text,
                "display_text": question.display_text,
                "type": question.type,
                "options": question.options,
                "is_archived": question.is_archived
            },
            "total_records": 1
        }


def check_question_by_text(text: str) -> dict:
    """Check what would be deleted for question by text"""
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.text == text).first()
        if not question:
            print(f"❌ Question with text '{text}' not found")
            return None
        
        return check_question_by_id(question.id)


def check_question_group_question_by_group_id(question_group_id: int) -> dict:
    """Check what would be deleted for QuestionGroupQuestion records by group_id"""
    with SessionLocal() as session:
        records = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id
        ).all()
        
        count = len(records)
        print(f"=== QUESTION GROUP QUESTIONS FOR GROUP {question_group_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No QuestionGroupQuestion records found")
            return None
        
        print(f"Question Group ID: {question_group_id}")
        print(f"Question IDs: {[r.question_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  QuestionGroupQuestion records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "question_group_id": question_group_id,
            "question_ids": [r.question_id for r in records],
            "total_records": count
        }


def check_question_group_question_by_question_id(question_id: int) -> dict:
    """Check what would be deleted for QuestionGroupQuestion records by question_id"""
    with SessionLocal() as session:
        records = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_id == question_id
        ).all()
        
        count = len(records)
        print(f"=== QUESTION GROUP QUESTIONS FOR QUESTION {question_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No QuestionGroupQuestion records found")
            return None
        
        print(f"Question ID: {question_id}")
        print(f"Question Group IDs: {[r.question_group_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  QuestionGroupQuestion records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "question_id": question_id,
            "question_group_ids": [r.question_group_id for r in records],
            "total_records": count
        }


def check_question_group_question_by_both_ids(question_group_id: int, question_id: int) -> dict:
    """Check what would be deleted for specific QuestionGroupQuestion record by both IDs"""
    with SessionLocal() as session:
        qgq = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id,
            QuestionGroupQuestion.question_id == question_id
        ).first()
        
        if not qgq:
            print(f"❌ QuestionGroupQuestion record not found for question_group_id {question_group_id}, question_id {question_id}")
            return None
        
        print(f"=== QUESTION GROUP QUESTION DELETION PREVIEW ===")
        print(f"Question Group ID: {question_group_id}")
        print(f"Question ID: {question_id}")
        print(f"Display Order: {qgq.display_order}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  QuestionGroupQuestion record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "question_group_id": question_group_id,
            "question_id": question_id,
            "display_order": qgq.display_order,
            "total_records": 1
        }


def check_schema_by_id(schema_id: int) -> dict:
    """Check what would be deleted for schema by ID"""
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.id == schema_id).first()
        if not schema:
            print(f"❌ Schema with ID {schema_id} not found")
            return None
        
        print(f"=== SCHEMA {schema_id} DELETION PREVIEW ===")
        print(f"Schema: {schema.name}")
        print(f"Instructions URL: {schema.instructions_url}")
        print(f"Created: {schema.created_at}")
        print(f"Archived: {schema.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Schema record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "schema_info": {
                "id": schema_id,
                "name": schema.name,
                "instructions_url": schema.instructions_url,
                "is_archived": schema.is_archived
            },
            "total_records": 1
        }


def check_schema_by_name(name: str) -> dict:
    """Check what would be deleted for schema by name"""
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.name == name).first()
        if not schema:
            print(f"❌ Schema with name '{name}' not found")
            return None
        
        return check_schema_by_id(schema.id)


def check_project_by_id(project_id: int) -> dict:
    """Check what would be deleted for project by ID"""
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            print(f"❌ Project with ID {project_id} not found")
            return None
        
        print(f"=== PROJECT {project_id} DELETION PREVIEW ===")
        print(f"Project: {project.name}")
        print(f"Schema ID: {project.schema_id}")
        print(f"Description: {project.description}")
        print(f"Created: {project.created_at}")
        print(f"Archived: {project.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Project record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "project_info": {
                "id": project_id,
                "name": project.name,
                "schema_id": project.schema_id,
                "description": project.description,
                "is_archived": project.is_archived
            },
            "total_records": 1
        }


def check_project_by_name(name: str) -> dict:
    """Check what would be deleted for project by name"""
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.name == name).first()
        if not project:
            print(f"❌ Project with name '{name}' not found")
            return None
        
        return check_project_by_id(project.id)


def check_project_video_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for ProjectVideo records by project_id"""
    with SessionLocal() as session:
        records = session.query(ProjectVideo).filter(ProjectVideo.project_id == project_id).all()
        
        count = len(records)
        print(f"=== PROJECT VIDEOS FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectVideo records found")
            return None
        
        print(f"Project ID: {project_id}")
        print(f"Video IDs: {[r.video_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideo records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "video_ids": [r.video_id for r in records],
            "total_records": count
        }


def check_project_video_by_video_id(video_id: int) -> dict:
    """Check what would be deleted for ProjectVideo records by video_id"""
    with SessionLocal() as session:
        records = session.query(ProjectVideo).filter(ProjectVideo.video_id == video_id).all()
        
        count = len(records)
        print(f"=== PROJECT VIDEOS FOR VIDEO {video_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectVideo records found")
            return None
        
        print(f"Video ID: {video_id}")
        print(f"Project IDs: {[r.project_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideo records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "video_id": video_id,
            "project_ids": [r.project_id for r in records],
            "total_records": count
        }


def check_project_video_by_both_ids(project_id: int, video_id: int) -> dict:
    """Check what would be deleted for specific ProjectVideo record by both IDs"""
    with SessionLocal() as session:
        pv = session.query(ProjectVideo).filter(
            ProjectVideo.project_id == project_id,
            ProjectVideo.video_id == video_id
        ).first()
        
        if not pv:
            print(f"❌ ProjectVideo record not found for project_id {project_id}, video_id {video_id}")
            return None
        
        print(f"=== PROJECT VIDEO DELETION PREVIEW ===")
        print(f"Project ID: {project_id}")
        print(f"Video ID: {video_id}")
        print(f"Added: {pv.added_at}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideo record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "project_id": project_id,
            "video_id": video_id,
            "added_at": pv.added_at,
            "total_records": 1
        }


def check_project_user_role_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for ProjectUserRole records by project_id"""
    with SessionLocal() as session:
        records = session.query(ProjectUserRole).filter(ProjectUserRole.project_id == project_id).all()
        
        count = len(records)
        print(f"=== PROJECT USER ROLES FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectUserRole records found")
            return None
        
        print(f"Project ID: {project_id}")
        print(f"User roles:")
        for record in records:
            print(f"  User {record.user_id}: {record.role} (weight: {record.user_weight})")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectUserRole records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "user_roles": [(r.user_id, r.role, r.user_weight) for r in records],
            "total_records": count
        }


def check_project_user_role_by_user_id(user_id: int) -> dict:
    """Check what would be deleted for ProjectUserRole records by user_id"""
    with SessionLocal() as session:
        records = session.query(ProjectUserRole).filter(ProjectUserRole.user_id == user_id).all()
        
        count = len(records)
        print(f"=== PROJECT USER ROLES FOR USER {user_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectUserRole records found")
            return None
        
        print(f"User ID: {user_id}")
        print(f"Project roles:")
        for record in records:
            print(f"  Project {record.project_id}: {record.role} (weight: {record.user_weight})")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectUserRole records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "user_id": user_id,
            "project_roles": [(r.project_id, r.role, r.user_weight) for r in records],
            "total_records": count
        }


def check_project_user_role_by_both_ids(project_id: int, user_id: int) -> dict:
    """Check what would be deleted for ProjectUserRole records by project_id and user_id"""
    with SessionLocal() as session:
        records = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id,
            ProjectUserRole.user_id == user_id
        ).all()
        
        count = len(records)
        if count == 0:
            print(f"❌ No ProjectUserRole records found for project_id {project_id}, user_id {user_id}")
            return None
        
        print(f"=== PROJECT USER ROLES DELETION PREVIEW ===")
        print(f"Project ID: {project_id}")
        print(f"User ID: {user_id}")
        print(f"Roles to be deleted:")
        for record in records:
            print(f"  Role: {record.role}")
            print(f"    Weight: {record.user_weight}")
            print(f"    Assigned: {record.assigned_at}")
            print(f"    Completed: {record.completed_at}")
            print(f"    Archived: {record.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectUserRole records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "user_id": user_id,
            "roles": [
                {
                    "role": r.role,
                    "user_weight": r.user_weight,
                    "assigned_at": r.assigned_at,
                    "completed_at": r.completed_at,
                    "is_archived": r.is_archived
                } for r in records
            ],
            "total_records": count
        }


def check_project_group_by_id(project_group_id: int) -> dict:
    """Check what would be deleted for project group by ID"""
    with SessionLocal() as session:
        pg = session.query(ProjectGroup).filter(ProjectGroup.id == project_group_id).first()
        if not pg:
            print(f"❌ ProjectGroup with ID {project_group_id} not found")
            return None
        
        print(f"=== PROJECT GROUP {project_group_id} DELETION PREVIEW ===")
        print(f"Project Group: {pg.name}")
        print(f"Description: {pg.description}")
        print(f"Created: {pg.created_at}")
        print(f"Archived: {pg.is_archived}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  Project group record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "project_group_info": {
                "id": project_group_id,
                "name": pg.name,
                "description": pg.description,
                "is_archived": pg.is_archived
            },
            "total_records": 1
        }


def check_project_group_by_name(name: str) -> dict:
    """Check what would be deleted for project group by name"""
    with SessionLocal() as session:
        pg = session.query(ProjectGroup).filter(ProjectGroup.name == name).first()
        if not pg:
            print(f"❌ ProjectGroup with name '{name}' not found")
            return None
        
        return check_project_group_by_id(pg.id)


def check_project_group_project_by_group_id(project_group_id: int) -> dict:
    """Check what would be deleted for ProjectGroupProject records by project_group_id"""
    with SessionLocal() as session:
        records = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_group_id == project_group_id
        ).all()
        
        count = len(records)
        print(f"=== PROJECT GROUP PROJECTS FOR GROUP {project_group_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectGroupProject records found")
            return None
        
        print(f"Project Group ID: {project_group_id}")
        print(f"Project IDs: {[r.project_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectGroupProject records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_group_id": project_group_id,
            "project_ids": [r.project_id for r in records],
            "total_records": count
        }


def check_project_group_project_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for ProjectGroupProject records by project_id"""
    with SessionLocal() as session:
        records = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_id == project_id
        ).all()
        
        count = len(records)
        print(f"=== PROJECT GROUP PROJECTS FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectGroupProject records found")
            return None
        
        print(f"Project ID: {project_id}")
        print(f"Project Group IDs: {[r.project_group_id for r in records]}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectGroupProject records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "project_group_ids": [r.project_group_id for r in records],
            "total_records": count
        }


def check_project_group_project_by_both_ids(project_group_id: int, project_id: int) -> dict:
    """Check what would be deleted for specific ProjectGroupProject record by both IDs"""
    with SessionLocal() as session:
        pgp = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_group_id == project_group_id,
            ProjectGroupProject.project_id == project_id
        ).first()
        
        if not pgp:
            print(f"❌ ProjectGroupProject record not found for project_group_id {project_group_id}, project_id {project_id}")
            return None
        
        print(f"=== PROJECT GROUP PROJECT DELETION PREVIEW ===")
        print(f"Project Group ID: {project_group_id}")
        print(f"Project ID: {project_id}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectGroupProject record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "project_group_id": project_group_id,
            "project_id": project_id,
            "total_records": 1
        }


def check_project_video_question_display_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for ProjectVideoQuestionDisplay records by project_id"""
    with SessionLocal() as session:
        records = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        ).all()
        
        count = len(records)
        print(f"=== PROJECT VIDEO QUESTION DISPLAYS FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectVideoQuestionDisplay records found")
            return None
        
        print(f"Project ID: {project_id}")
        print(f"Custom displays:")
        for record in records:
            print(f"  Video {record.video_id}, Question {record.question_id}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideoQuestionDisplay records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "video_question_pairs": [(r.video_id, r.question_id) for r in records],
            "total_records": count
        }


def check_project_video_question_display_by_video_id(video_id: int) -> dict:
    """Check what would be deleted for ProjectVideoQuestionDisplay records by video_id"""
    with SessionLocal() as session:
        records = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.video_id == video_id
        ).all()
        
        count = len(records)
        print(f"=== PROJECT VIDEO QUESTION DISPLAYS FOR VIDEO {video_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectVideoQuestionDisplay records found")
            return None
        
        print(f"Video ID: {video_id}")
        print(f"Custom displays:")
        for record in records:
            print(f"  Project {record.project_id}, Question {record.question_id}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideoQuestionDisplay records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "video_id": video_id,
            "project_question_pairs": [(r.project_id, r.question_id) for r in records],
            "total_records": count
        }


def check_project_video_question_display_by_question_id(question_id: int) -> dict:
    """Check what would be deleted for ProjectVideoQuestionDisplay records by question_id"""
    with SessionLocal() as session:
        records = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.question_id == question_id
        ).all()
        
        count = len(records)
        print(f"=== PROJECT VIDEO QUESTION DISPLAYS FOR QUESTION {question_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ProjectVideoQuestionDisplay records found")
            return None
        
        print(f"Question ID: {question_id}")
        print(f"Custom displays:")
        for record in records:
            print(f"  Project {record.project_id}, Video {record.video_id}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideoQuestionDisplay records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "question_id": question_id,
            "project_video_pairs": [(r.project_id, r.video_id) for r in records],
            "total_records": count
        }


def check_project_video_question_display_by_both_ids(project_id: int, video_id: int, question_id: int) -> dict:
    """Check what would be deleted for specific ProjectVideoQuestionDisplay record by all three IDs"""
    with SessionLocal() as session:
        pvqd = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id,
            ProjectVideoQuestionDisplay.video_id == video_id,
            ProjectVideoQuestionDisplay.question_id == question_id
        ).first()
        
        if not pvqd:
            print(f"❌ ProjectVideoQuestionDisplay record not found for project_id {project_id}, video_id {video_id}, question_id {question_id}")
            return None
        
        print(f"=== PROJECT VIDEO QUESTION DISPLAY DELETION PREVIEW ===")
        print(f"Project ID: {project_id}")
        print(f"Video ID: {video_id}")
        print(f"Question ID: {question_id}")
        print(f"Custom Display Text: {pvqd.custom_display_text}")
        print(f"Custom Option Display Map: {pvqd.custom_option_display_map}")
        print(f"Created: {pvqd.created_at}")
        print(f"Updated: {pvqd.updated_at}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ProjectVideoQuestionDisplay record: 1")
        print(f"  📈 TOTAL RECORDS: 1")
        
        return {
            "project_id": project_id,
            "video_id": video_id,
            "question_id": question_id,
            "custom_display_text": pvqd.custom_display_text,
            "custom_option_display_map": pvqd.custom_option_display_map,
            "created_at": pvqd.created_at,
            "updated_at": pvqd.updated_at,
            "total_records": 1
        }


def check_annotator_answer_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for AnnotatorAnswer records by project_id"""
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id == project_id).count()
        
        print(f"=== ANNOTATOR ANSWERS FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No AnnotatorAnswer records found")
            return None
        
        # Get some sample data
        samples = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.project_id == project_id
        ).limit(5).all()
        
        print(f"Project ID: {project_id}")
        print(f"Total answers: {count}")
        print(f"Sample answers (first 5):")
        for answer in samples:
            print(f"  ID {answer.id}: video {answer.video_id}, question {answer.question_id}, user {answer.user_id}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  AnnotatorAnswer records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "total_records": count
        }
     
def check_annotator_answers_by_video_id(video_id: int) -> dict:
    """Check what would be deleted for AnnotatorAnswer records by video_id"""
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.video_id == video_id).count()
        
        print(f"=== ANNOTATOR ANSWERS FOR VIDEO {video_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No AnnotatorAnswer records found")
            return None
        
        # Get project distribution
        project_counts = session.query(
            AnnotatorAnswer.project_id,
            func.count(AnnotatorAnswer.id).label('count')
        ).filter(
            AnnotatorAnswer.video_id == video_id
        ).group_by(AnnotatorAnswer.project_id).all()
        
        print(f"Video ID: {video_id}")
        print(f"Total answers: {count}")
        print(f"Answers by project:")
        for project_id, proj_count in project_counts:
            print(f"  Project {project_id}: {proj_count} answers")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  AnnotatorAnswer records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "video_id": video_id,
            "total_records": count,
            "project_distribution": dict(project_counts)
        }


def check_annotator_answers_by_user_id(user_id: int) -> dict:
    """Check what would be deleted for AnnotatorAnswer records by user_id"""
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.user_id == user_id).count()
        
        print(f"=== ANNOTATOR ANSWERS FOR USER {user_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No AnnotatorAnswer records found")
            return None
        
        # Get project distribution
        project_counts = session.query(
            AnnotatorAnswer.project_id,
            func.count(AnnotatorAnswer.id).label('count')
        ).filter(
            AnnotatorAnswer.user_id == user_id
        ).group_by(AnnotatorAnswer.project_id).all()
        
        print(f"User ID: {user_id}")
        print(f"Total answers: {count}")
        print(f"Answers by project:")
        for project_id, proj_count in project_counts:
            print(f"  Project {project_id}: {proj_count} answers")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  AnnotatorAnswer records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "user_id": user_id,
            "total_records": count,
            "project_distribution": dict(project_counts)
        }
   

def check_reviewer_ground_truth_by_project_id(project_id: int) -> dict:
    """Check what would be deleted for ReviewerGroundTruth records by project_id"""
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id == project_id).count()
        
        print(f"=== REVIEWER GROUND TRUTH FOR PROJECT {project_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ReviewerGroundTruth records found")
            return None
        
        print(f"Project ID: {project_id}")
        print(f"Total ground truth records: {count}")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ReviewerGroundTruth records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "project_id": project_id,
            "total_records": count
        }

def check_reviewer_ground_truth_by_video_id(video_id: int) -> dict:
    """Check what would be deleted for ReviewerGroundTruth records by video_id"""
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.video_id == video_id).count()
        
        print(f"=== REVIEWER GROUND TRUTH FOR VIDEO {video_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ReviewerGroundTruth records found")
            return None
        
        # Get project distribution
        project_counts = session.query(
            ReviewerGroundTruth.project_id,
            func.count().label('count')
        ).filter(
            ReviewerGroundTruth.video_id == video_id
        ).group_by(ReviewerGroundTruth.project_id).all()
        
        # Get reviewer distribution
        reviewer_counts = session.query(
            ReviewerGroundTruth.reviewer_id,
            func.count().label('count')
        ).filter(
            ReviewerGroundTruth.video_id == video_id
        ).group_by(ReviewerGroundTruth.reviewer_id).all()
        
        print(f"Video ID: {video_id}")
        print(f"Total ground truth records: {count}")
        print(f"Ground truth by project:")
        for project_id, proj_count in project_counts:
            print(f"  Project {project_id}: {proj_count} records")
        print(f"Ground truth by reviewer:")
        for reviewer_id, rev_count in reviewer_counts:
            print(f"  Reviewer {reviewer_id}: {rev_count} records")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ReviewerGroundTruth records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "video_id": video_id,
            "total_records": count,
            "project_distribution": dict(project_counts),
            "reviewer_distribution": dict(reviewer_counts)
        }


def check_reviewer_ground_truth_by_reviewer_id(reviewer_id: int) -> dict:
    """Check what would be deleted for ReviewerGroundTruth records by reviewer_id"""
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.reviewer_id == reviewer_id).count()
        
        print(f"=== REVIEWER GROUND TRUTH FOR REVIEWER {reviewer_id} DELETION PREVIEW ===")
        if count == 0:
            print("❌ No ReviewerGroundTruth records found")
            return None
        
        # Get project distribution
        project_counts = session.query(
            ReviewerGroundTruth.project_id,
            func.count().label('count')
        ).filter(
            ReviewerGroundTruth.reviewer_id == reviewer_id
        ).group_by(ReviewerGroundTruth.project_id).all()
        
        # Get video distribution (top 10)
        video_counts = session.query(
            ReviewerGroundTruth.video_id,
            func.count().label('count')
        ).filter(
            ReviewerGroundTruth.reviewer_id == reviewer_id
        ).group_by(ReviewerGroundTruth.video_id).order_by(func.count().desc()).limit(10).all()
        
        print(f"Reviewer ID: {reviewer_id}")
        print(f"Total ground truth records: {count}")
        print(f"Ground truth by project:")
        for project_id, proj_count in project_counts:
            print(f"  Project {project_id}: {proj_count} records")
        print(f"Ground truth by video (top 10):")
        for video_id, vid_count in video_counts:
            print(f"  Video {video_id}: {vid_count} records")
        print()
        print("📊 RECORDS THAT WOULD BE DELETED:")
        print(f"  ReviewerGroundTruth records: {count}")
        print(f"  📈 TOTAL RECORDS: {count}")
        
        return {
            "reviewer_id": reviewer_id,
            "total_records": count,
            "project_distribution": dict(project_counts),
            "top_video_distribution": dict(video_counts)
        }


"""
All the delete functions that are used to delete the records.
"""

def delete_user_by_id(user_id: int = None, confirm: bool = True) -> bool:
    """Delete user by ID"""
    # First show what would be deleted
    check_result = check_user_by_id(user_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            print(f"❌ User with ID {user_id} not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the user!")
            response = input(f"\nConfirm deletion of user '{user.user_id_str}' (ID: {user_id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_user_{user_id}")
        
        session.delete(user)
        session.commit()
        print(f"✅ Deleted user '{user.user_id_str}' (ID: {user_id})")
        return True


def delete_user_by_user_id_str(user_id_str: str = None, confirm: bool = True) -> bool:
    """Delete user by user_id_str"""
    # Show what would be deleted
    check_result = check_user_by_id(user.id)
    if not check_result:
        return False    

    with SessionLocal() as session:
        user = session.query(User).filter(User.user_id_str == user_id_str).first()
        if not user:
            print(f"❌ User with user_id_str '{user_id_str}' not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the user!")
            response = input(f"\nConfirm deletion of user '{user_id_str}' (ID: {user.id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_user_{user_id_str}")
        
        session.delete(user)
        session.commit()
        print(f"✅ Deleted user '{user_id_str}' (ID: {user.id})")
        return True


# ==================== VIDEO FUNCTIONS ====================

def delete_video_by_id(video_id: int = None, confirm: bool = True) -> bool:
    """Delete video by ID"""
    # First show what would be deleted
    check_result = check_video_by_id(video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        video = session.query(Video).filter(Video.id == video_id).first()
        if not video:
            print(f"❌ Video with ID {video_id} not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the video!")
            response = input(f"\nConfirm deletion of video '{video.video_uid}' (ID: {video_id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_video_{video_id}")
        
        session.delete(video)
        session.commit()
        print(f"✅ Deleted video '{video.video_uid}' (ID: {video_id})")
        return True


def delete_video_by_uid(video_uid: str, confirm: bool = True) -> bool:
    """Delete video by video_uid"""
    # Show what would be deleted
    check_result = check_video_by_id(video.id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        video = session.query(Video).filter(Video.video_uid == video_uid).first()
        if not video:
            print(f"❌ Video with video_uid '{video_uid}' not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the video!")
            response = input(f"\nConfirm deletion of video '{video_uid}' (ID: {video.id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_video_{video_uid}")
        
        session.delete(video)
        session.commit()
        print(f"✅ Deleted video '{video_uid}' (ID: {video.id})")
        return True


# ==================== VIDEO TAG FUNCTIONS ====================

def delete_video_tag_by_video_id(video_id: int = None, confirm: bool = True) -> int:
    """Delete all video tags for a specific video_id"""
    # First show what would be deleted
    check_result = check_video_tag_by_video_id(video_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        tags = session.query(VideoTag).filter(VideoTag.video_id == video_id).all()
        if not tags:
            print(f"❌ No video tags found for video_id {video_id}")
            return 0
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE all video tags for this video!")
            response = input(f"\nConfirm deletion of {len(tags)} video tags for video_id {video_id}? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_video_tags_{video_id}")
        
        count = session.query(VideoTag).filter(VideoTag.video_id == video_id).delete()
        session.commit()
        print(f"✅ Deleted {count} video tags for video_id {video_id}")
        return count


# ==================== QUESTION GROUP FUNCTIONS ====================

def delete_question_group_by_id(question_group_id: int = None, confirm: bool = True) -> bool:
    """Delete question group by ID"""
    # First show what would be deleted
    check_result = check_question_group_by_id(question_group_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.id == question_group_id).first()
        if not qg:
            print(f"❌ QuestionGroup with ID {question_group_id} not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the question group!")
            response = input(f"\nConfirm deletion of question group '{qg.title}' (ID: {question_group_id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_question_group_{question_group_id}")
        
        session.delete(qg)
        session.commit()
        print(f"✅ Deleted question group '{qg.title}' (ID: {question_group_id})")
        return True


def delete_question_group_by_title(title: str = None, confirm: bool = True) -> bool:
    """Delete question group by title"""
    # Show what would be deleted
    check_result = check_question_group_by_id(qg.id)
    if not check_result:
        return False
        
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.title == title).first()
        if not qg:
            print(f"❌ QuestionGroup with title '{title}' not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the question group!")
            response = input(f"\nConfirm deletion of question group '{title}' (ID: {qg.id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_question_group_{title}")
        
        session.delete(qg)
        session.commit()
        print(f"✅ Deleted question group '{title}' (ID: {qg.id})")
        return True


# ==================== QUESTION FUNCTIONS ====================

def delete_question_by_id(question_id: int = None, confirm: bool = True) -> bool:
    """Delete question by ID"""
    # First show what would be deleted
    check_result = check_question_by_id(question_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            print(f"❌ Question with ID {question_id} not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the question!")
            response = input(f"\nConfirm deletion of question '{question.text}' (ID: {question_id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_question_{question_id}")
        
        session.delete(question)
        session.commit()
        print(f"✅ Deleted question '{question.text}' (ID: {question_id})")
        return True


def delete_question_by_text(text: str = None, confirm: bool = True) -> bool:
    """Delete question by text"""
    # Show what would be deleted
    check_result = check_question_by_text(text)
    if not check_result:
        return False
        
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.text == text).first()
        if not question:
            print(f"❌ Question with text '{text}' not found")
            return False
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE the question!")
            response = input(f"\nConfirm deletion of question '{text}' (ID: {question.id})? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_question_{text.replace(' ', '_')}")
        
        session.delete(question)
        session.commit()
        print(f"✅ Deleted question '{text}' (ID: {question.id})")
        return True


# ==================== QUESTION GROUP QUESTION FUNCTIONS ====================

def delete_question_group_question_by_group_id(question_group_id: int, confirm: bool = True) -> int:
    """Delete all QuestionGroupQuestion records for a specific question_group_id"""
    # First show what would be deleted
    check_result = check_question_group_question_by_group_id(question_group_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        count = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id
        ).count()
        
        if count == 0:
            print(f"❌ No QuestionGroupQuestion records found for question_group_id {question_group_id}")
            return 0
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE all question-group relationships!")
            response = input(f"\nConfirm deletion of {count} QuestionGroupQuestion records for question_group_id {question_group_id}? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_qgq_group_{question_group_id}")
        
        deleted_count = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} QuestionGroupQuestion records for question_group_id {question_group_id}")
        return deleted_count


def delete_question_group_question_by_question_id(question_id: int, confirm: bool = True) -> int:
    """Delete all QuestionGroupQuestion records for a specific question_id"""
    # First show what would be deleted
    check_result = check_question_group_question_by_question_id(question_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        count = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_id == question_id
        ).count()
        
        if count == 0:
            print(f"❌ No QuestionGroupQuestion records found for question_id {question_id}")
            return 0
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE all question-group relationships!")
            response = input(f"\nConfirm deletion of {count} QuestionGroupQuestion records for question_id {question_id}? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_qgq_question_{question_id}")
        
        deleted_count = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_id == question_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} QuestionGroupQuestion records for question_id {question_id}")
        return deleted_count


def delete_question_group_question_by_both_ids(question_group_id: int, question_id: int, confirm: bool = True) -> bool:
    """Delete specific QuestionGroupQuestion record by both IDs"""
    # First show what would be deleted
    check_result = check_question_group_question_by_both_ids(question_group_id, question_id)
    if not check_result:
        return 0
    with SessionLocal() as session:
        qgq = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id,
            QuestionGroupQuestion.question_id == question_id
        ).first()
        
        if not qgq:
            print(f"❌ QuestionGroupQuestion record not found for question_group_id {question_group_id}, question_id {question_id}")
            return False
        
        if confirm:
            response = input(f"Delete QuestionGroupQuestion record (group_id: {question_group_id}, question_id: {question_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_qgq_{question_group_id}_{question_id}")
        
        session.delete(qgq)
        session.commit()
        print(f"✅ Deleted QuestionGroupQuestion record (group_id: {question_group_id}, question_id: {question_id})")
        return True


# ==================== SCHEMA FUNCTIONS ====================

def delete_schema_by_id(schema_id: int, confirm: bool = True) -> bool:
    """Delete schema by ID"""
    # First show what would be deleted
    check_result = check_schema_by_id(schema_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.id == schema_id).first()
        if not schema:
            print(f"❌ Schema with ID {schema_id} not found")
            return False
        
        if confirm:
            response = input(f"Delete schema '{schema.name}' (ID: {schema_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_schema_{schema_id}")
        
        session.delete(schema)
        session.commit()
        print(f"✅ Deleted schema '{schema.name}' (ID: {schema_id})")
        return True


def delete_schema_by_name(name: str, confirm: bool = True) -> bool:
    """Delete schema by name"""
    # Show what would be deleted
    check_result = check_schema_by_name(schema.id)
    if not check_result:
        return False
        
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.name == name).first()
        if not schema:
            print(f"❌ Schema with name '{name}' not found")
            return False
        
        if confirm:
            response = input(f"Delete schema '{name}' (ID: {schema.id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_schema_{name}")
        
        session.delete(schema)
        session.commit()
        print(f"✅ Deleted schema '{name}' (ID: {schema.id})")
        return True


# ==================== SCHEMA QUESTION GROUP FUNCTIONS ====================

def delete_schema_question_group_by_schema_id(schema_id: int = None, confirm: bool = True) -> int:
    """Delete all SchemaQuestionGroup records for a specific schema_id"""
    # First show what would be deleted
    check_result = check_schema_question_group_by_schema_id(schema_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        count = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.schema_id == schema_id
        ).count()
        
        if count == 0:
            print(f"❌ No SchemaQuestionGroup records found for schema_id {schema_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} SchemaQuestionGroup records for schema_id {schema_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_sqg_schema_{schema_id}")
        
        deleted_count = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.schema_id == schema_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} SchemaQuestionGroup records for schema_id {schema_id}")
        return deleted_count


def delete_schema_question_group_by_question_group_id(question_group_id: int = None, confirm: bool = True) -> int:
    """Delete all SchemaQuestionGroup records for a specific question_group_id"""
    # First show what would be deleted
    check_result = check_schema_question_group_by_question_group_id(question_group_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        count = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.question_group_id == question_group_id
        ).count()
        
        if count == 0:
            print(f"❌ No SchemaQuestionGroup records found for question_group_id {question_group_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} SchemaQuestionGroup records for question_group_id {question_group_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_sqg_qgroup_{question_group_id}")
        
        deleted_count = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.question_group_id == question_group_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} SchemaQuestionGroup records for question_group_id {question_group_id}")
        return deleted_count


def delete_schema_question_group_by_both_ids(schema_id: int = None, question_group_id: int = None, confirm: bool = True) -> bool:
    """Delete specific SchemaQuestionGroup record by both IDs"""
    # First show what would be deleted
    check_result = check_schema_question_group_by_both_ids(schema_id, question_group_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        sqg = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.schema_id == schema_id,
            SchemaQuestionGroup.question_group_id == question_group_id
        ).first()
        
        if not sqg:
            print(f"❌ SchemaQuestionGroup record not found for schema_id {schema_id}, question_group_id {question_group_id}")
            return False
        
        if confirm:
            response = input(f"Delete SchemaQuestionGroup record (schema_id: {schema_id}, question_group_id: {question_group_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_sqg_{schema_id}_{question_group_id}")
        
        session.delete(sqg)
        session.commit()
        print(f"✅ Deleted SchemaQuestionGroup record (schema_id: {schema_id}, question_group_id: {question_group_id})")
        return True


# ==================== PROJECT FUNCTIONS ====================

def delete_project_by_id(project_id: int = None, confirm: bool = True) -> bool:
    """Delete project by ID"""
    # First show what would be deleted
    check_result = check_project_by_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            print(f"❌ Project with ID {project_id} not found")
            return False
        
        if confirm:
            response = input(f"Delete project '{project.name}' (ID: {project_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_project_{project_id}")
        
        session.delete(project)
        session.commit()
        print(f"✅ Deleted project '{project.name}' (ID: {project_id})")
        return True


def delete_project_by_name(name: str = None, confirm: bool = True) -> bool:
    """Delete project by name"""
    # First show what would be deleted
    check_result = check_project_by_name(name)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.name == name).first()
        if not project:
            print(f"❌ Project with name '{name}' not found")
            return False
        
        if confirm:
            response = input(f"Delete project '{name}' (ID: {project.id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_project_{name}")
        
        session.delete(project)
        session.commit()
        print(f"✅ Deleted project '{name}' (ID: {project.id})")
        return True


# ==================== PROJECT VIDEO FUNCTIONS ====================

def delete_project_video_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectVideo records for a specific project_id"""
    # First show what would be deleted
    check_result = check_project_video_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectVideo).filter(ProjectVideo.project_id == project_id).count()
        
        if count == 0:
            print(f"❌ No ProjectVideo records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectVideo records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_project_videos_{project_id}")
        
        deleted_count = session.query(ProjectVideo).filter(ProjectVideo.project_id == project_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectVideo records for project_id {project_id}")
        return deleted_count


def delete_project_video_by_video_id(video_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectVideo records for a specific video_id"""
    # First show what would be deleted
    check_result = check_project_video_by_video_id(video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectVideo).filter(ProjectVideo.video_id == video_id).count()
        
        if count == 0:
            print(f"❌ No ProjectVideo records found for video_id {video_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectVideo records for video_id {video_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_project_videos_vid_{video_id}")
        
        deleted_count = session.query(ProjectVideo).filter(ProjectVideo.video_id == video_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectVideo records for video_id {video_id}")
        return deleted_count


def delete_project_video_by_both_ids(project_id: int = None, video_id: int = None, confirm: bool = True) -> bool:
    """Delete specific ProjectVideo record by both IDs"""
    # First show what would be deleted
    check_result = check_project_video_by_both_ids(project_id, video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        pv = session.query(ProjectVideo).filter(
            ProjectVideo.project_id == project_id,
            ProjectVideo.video_id == video_id
        ).first()
        
        if not pv:
            print(f"❌ ProjectVideo record not found for project_id {project_id}, video_id {video_id}")
            return False
        
        if confirm:
            response = input(f"Delete ProjectVideo record (project_id: {project_id}, video_id: {video_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_project_video_{project_id}_{video_id}")
        
        session.delete(pv)
        session.commit()
        print(f"✅ Deleted ProjectVideo record (project_id: {project_id}, video_id: {video_id})")
        return True


# ==================== PROJECT USER ROLE FUNCTIONS ====================

def delete_project_user_role_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectUserRole records for a specific project_id"""
    # First show what would be deleted
    check_result = check_project_user_role_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectUserRole).filter(ProjectUserRole.project_id == project_id).count()
        
        if count == 0:
            print(f"❌ No ProjectUserRole records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectUserRole records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_project_user_roles_{project_id}")
        
        deleted_count = session.query(ProjectUserRole).filter(ProjectUserRole.project_id == project_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectUserRole records for project_id {project_id}")
        return deleted_count


def delete_project_user_role_by_user_id(user_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectUserRole records for a specific user_id"""
    # First show what would be deleted
    check_result = check_project_user_role_by_user_id(user_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectUserRole).filter(ProjectUserRole.user_id == user_id).count()
        
        if count == 0:
            print(f"❌ No ProjectUserRole records found for user_id {user_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectUserRole records for user_id {user_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_project_user_roles_user_{user_id}")
        
        deleted_count = session.query(ProjectUserRole).filter(ProjectUserRole.user_id == user_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectUserRole records for user_id {user_id}")
        return deleted_count


def delete_project_user_role_by_both_ids(project_id: int = None, user_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectUserRole records for a specific project_id and user_id combination"""
    # First show what would be deleted
    check_result = check_project_user_role_by_both_ids(project_id, user_id)
    if not check_result:
        return 0
    
    with SessionLocal() as session:
        count = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id,
            ProjectUserRole.user_id == user_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectUserRole records found for project_id {project_id}, user_id {user_id}")
            return 0
        
        if confirm:
            print("⚠️  WARNING: This will PERMANENTLY DELETE all user roles for this project-user combination!")
            response = input(f"\nConfirm deletion of {count} ProjectUserRole records for project_id {project_id}, user_id {user_id}? (DELETE/no): ")
            if response != "DELETE":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_project_user_role_{project_id}_{user_id}")
        
        deleted_count = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id,
            ProjectUserRole.user_id == user_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectUserRole records for project_id {project_id}, user_id {user_id}")
        return deleted_count


# ==================== PROJECT GROUP FUNCTIONS ====================

def delete_project_group_by_id(project_group_id: int = None, confirm: bool = True) -> bool:
    """Delete project group by ID"""
    # First show what would be deleted
    check_result = check_project_group_by_id(project_group_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        pg = session.query(ProjectGroup).filter(ProjectGroup.id == project_group_id).first()
        if not pg:
            print(f"❌ ProjectGroup with ID {project_group_id} not found")
            return False
        
        if confirm:
            response = input(f"Delete project group '{pg.name}' (ID: {project_group_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_project_group_{project_group_id}")
        
        session.delete(pg)
        session.commit()
        print(f"✅ Deleted project group '{pg.name}' (ID: {project_group_id})")
        return True


def delete_project_group_by_name(name: str = None, confirm: bool = True) -> bool:
    """Delete project group by name"""
    # First show what would be deleted
    check_result = check_project_group_by_name(name)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        pg = session.query(ProjectGroup).filter(ProjectGroup.name == name).first()
        if not pg:
            print(f"❌ ProjectGroup with name '{name}' not found")
            return False
        
        if confirm:
            response = input(f"Delete project group '{name}' (ID: {pg.id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_project_group_{name}")
        
        session.delete(pg)
        session.commit()
        print(f"✅ Deleted project group '{name}' (ID: {pg.id})")
        return True


# ==================== PROJECT GROUP PROJECT FUNCTIONS ====================

def delete_project_group_project_by_group_id(project_group_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectGroupProject records for a specific project_group_id"""
    # First show what would be deleted
    check_result = check_project_group_project_by_group_id(project_group_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_group_id == project_group_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectGroupProject records found for project_group_id {project_group_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectGroupProject records for project_group_id {project_group_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_pgp_group_{project_group_id}")
        
        deleted_count = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_group_id == project_group_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectGroupProject records for project_group_id {project_group_id}")
        return deleted_count


def delete_project_group_project_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectGroupProject records for a specific project_id"""
    # First show what would be deleted
    check_result = check_project_group_project_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_id == project_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectGroupProject records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectGroupProject records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_pgp_project_{project_id}")
        
        deleted_count = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_id == project_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectGroupProject records for project_id {project_id}")
        return deleted_count


def delete_project_group_project_by_both_ids(project_group_id: int = None, project_id: int = None, confirm: bool = True) -> bool:
    """Delete specific ProjectGroupProject record by both IDs"""
    # First show what would be deleted
    check_result = check_project_group_project_by_both_ids(project_group_id, project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        pgp = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_group_id == project_group_id,
            ProjectGroupProject.project_id == project_id
        ).first()
        
        if not pgp:
            print(f"❌ ProjectGroupProject record not found for project_group_id {project_group_id}, project_id {project_id}")
            return False
        
        if confirm:
            response = input(f"Delete ProjectGroupProject record (project_group_id: {project_group_id}, project_id: {project_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_pgp_{project_group_id}_{project_id}")
        
        session.delete(pgp)
        session.commit()
        print(f"✅ Deleted ProjectGroupProject record (project_group_id: {project_group_id}, project_id: {project_id})")
        return True


# ==================== PROJECT VIDEO QUESTION DISPLAY FUNCTIONS ====================

def delete_project_video_question_display_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectVideoQuestionDisplay records for a specific project_id"""
    # First show what would be deleted
    check_result = check_project_video_question_display_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectVideoQuestionDisplay records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectVideoQuestionDisplay records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_pvqd_project_{project_id}")
        
        deleted_count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectVideoQuestionDisplay records for project_id {project_id}")
        return deleted_count


def delete_project_video_question_displays_by_video_id(video_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectVideoQuestionDisplay records for a specific video_id"""
    # First show what would be deleted
    check_result = check_project_video_question_display_by_video_id(video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.video_id == video_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectVideoQuestionDisplay records found for video_id {video_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectVideoQuestionDisplay records for video_id {video_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_pvqd_video_{video_id}")
        
        deleted_count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.video_id == video_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectVideoQuestionDisplay records for video_id {video_id}")
        return deleted_count


def delete_project_video_question_displays_by_question_id(question_id: int = None, confirm: bool = True) -> int:
    """Delete all ProjectVideoQuestionDisplay records for a specific question_id"""
    # First show what would be deleted
    check_result = check_project_video_question_display_by_question_id(question_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.question_id == question_id
        ).count()
        
        if count == 0:
            print(f"❌ No ProjectVideoQuestionDisplay records found for question_id {question_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ProjectVideoQuestionDisplay records for question_id {question_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_pvqd_question_{question_id}")
        
        deleted_count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.question_id == question_id
        ).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ProjectVideoQuestionDisplay records for question_id {question_id}")
        return deleted_count


def delete_project_video_question_display_by_ids(project_id: int = None, video_id: int = None, question_id: int = None, confirm: bool = True) -> bool:
    """Delete specific ProjectVideoQuestionDisplay record by all three IDs"""
    # First show what would be deleted
    check_result = check_project_video_question_display_by_both_ids(project_id, video_id, question_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        pvqd = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id,
            ProjectVideoQuestionDisplay.video_id == video_id,
            ProjectVideoQuestionDisplay.question_id == question_id
        ).first()
        
        if not pvqd:
            print(f"❌ ProjectVideoQuestionDisplay record not found for project_id {project_id}, video_id {video_id}, question_id {question_id}")
            return False
        
        if confirm:
            response = input(f"Delete ProjectVideoQuestionDisplay record (project_id: {project_id}, video_id: {video_id}, question_id: {question_id})? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return False
        
        backup_path = _create_operation_backup(f"delete_pvqd_{project_id}_{video_id}_{question_id}")
        
        session.delete(pvqd)
        session.commit()
        print(f"✅ Deleted ProjectVideoQuestionDisplay record (project_id: {project_id}, video_id: {video_id}, question_id: {question_id})")
        return True


# ==================== ANNOTATOR ANSWER FUNCTIONS ====================

def delete_annotator_answer_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all AnnotatorAnswer records for a specific project_id"""
    # First show what would be deleted
    check_result = check_annotator_answer_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id == project_id).count()
        
        if count == 0:
            print(f"❌ No AnnotatorAnswer records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} AnnotatorAnswer records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_annotator_answers_project_{project_id}")
        
        deleted_count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id == project_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} AnnotatorAnswer records for project_id {project_id}")
        return deleted_count


def delete_annotator_answers_by_video_id(video_id: int = None, confirm: bool = True) -> int:
    """Delete all AnnotatorAnswer records for a specific video_id"""
    # First show what would be deleted
    check_result = check_annotator_answers_by_video_id(video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.video_id == video_id).count()
        
        if count == 0:
            print(f"❌ No AnnotatorAnswer records found for video_id {video_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} AnnotatorAnswer records for video_id {video_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_annotator_answers_video_{video_id}")
        
        deleted_count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.video_id == video_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} AnnotatorAnswer records for video_id {video_id}")
        return deleted_count


def delete_annotator_answers_by_user_id(user_id: int = None, confirm: bool = True) -> int:
    """Delete all AnnotatorAnswer records for a specific user_id"""
    # First show what would be deleted
    check_result = check_annotator_answers_by_user_id(user_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.user_id == user_id).count()
        
        if count == 0:
            print(f"❌ No AnnotatorAnswer records found for user_id {user_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} AnnotatorAnswer records for user_id {user_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_annotator_answers_user_{user_id}")
        
        deleted_count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.user_id == user_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} AnnotatorAnswer records for user_id {user_id}")
        return deleted_count


# ==================== REVIEWER GROUND TRUTH FUNCTIONS ====================

def delete_reviewer_ground_truth_by_project_id(project_id: int = None, confirm: bool = True) -> int:
    """Delete all ReviewerGroundTruth records for a specific project_id"""
    # First show what would be deleted
    check_result = check_reviewer_ground_truth_by_project_id(project_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id == project_id).count()
        
        if count == 0:
            print(f"❌ No ReviewerGroundTruth records found for project_id {project_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ReviewerGroundTruth records for project_id {project_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_rgt_project_{project_id}")
        
        deleted_count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id == project_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ReviewerGroundTruth records for project_id {project_id}")
        return deleted_count


def delete_reviewer_ground_truth_by_video_id(video_id: int = None, confirm: bool = True) -> int:
    """Delete all ReviewerGroundTruth records for a specific video_id"""
    # First show what would be deleted
    check_result = check_reviewer_ground_truth_by_video_id(video_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.video_id == video_id).count()
        
        if count == 0:
            print(f"❌ No ReviewerGroundTruth records found for video_id {video_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ReviewerGroundTruth records for video_id {video_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_rgt_video_{video_id}")
        
        deleted_count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.video_id == video_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ReviewerGroundTruth records for video_id {video_id}")
        return deleted_count


def delete_reviewer_ground_truth_by_reviewer_id(reviewer_id: int = None, confirm: bool = True) -> int:
    """Delete all ReviewerGroundTruth records for a specific reviewer_id"""
    # First show what would be deleted
    check_result = check_reviewer_ground_truth_by_reviewer_id(reviewer_id)
    if not check_result:
        return False
    
    with SessionLocal() as session:
        count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.reviewer_id == reviewer_id).count()
        
        if count == 0:
            print(f"❌ No ReviewerGroundTruth records found for reviewer_id {reviewer_id}")
            return 0
        
        if confirm:
            response = input(f"Delete {count} ReviewerGroundTruth records for reviewer_id {reviewer_id}? (yes/no): ")
            if response.lower() != "yes":
                print("❌ Deletion cancelled")
                return 0
        
        backup_path = _create_operation_backup(f"delete_rgt_reviewer_{reviewer_id}")
        
        deleted_count = session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.reviewer_id == reviewer_id).delete()
        session.commit()
        print(f"✅ Deleted {deleted_count} ReviewerGroundTruth records for reviewer_id {reviewer_id}")
        return deleted_count

        
# replace_question(old_id=78, new_id=58)
# change_question_text("Glassy surface reflection?", "Glossy surface reflection?")

# with SessionLocal() as session:
#     # 查看所有约束
#     result = session.execute(text("""
#         SELECT constraint_name 
#         FROM information_schema.table_constraints 
#         WHERE table_name = 'question_groups' 
#         AND constraint_type = 'UNIQUE';
#     """))
#     for row in result:
#         print(f"Found constraint: {row[0]}")

# find_questions_with_none_default()
# update_question_default_option(question_id=6, new_default_option="")
# delete_all_project_data(226)
# delete_all_schema_data(schema_id=6)
# delete_all_question_group_data(question_group_id=15)
# delete_all_question_data(question_id=77)
# change_question_text("Aerial / atmospheric Perspective?", "Aerial / atmospheric perspective?")





