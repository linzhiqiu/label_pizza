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
    from backup_restore import DatabaseBackupRestore
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


def get_schema_question_ids(schema_id: int, session: Session) -> set:
    """
    Helper function to get question IDs in a schema (optimized for our use case)
    """
    # Check if schema exists
    schema = session.query(Schema).filter(Schema.id == schema_id).first()
    if not schema:
        raise ValueError(f"Schema with ID {schema_id} not found")
    
    # Get question IDs more efficiently
    question_ids = session.query(Question.id).join(
        QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id
    ).join(
        SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id
    ).filter(
        SchemaQuestionGroup.schema_id == schema_id
    ).all()
    
    return {q.id for q in question_ids}


def check_project_counts_simple(project_id: int, new_schema_id: int):
    """
    Quick count check using optimized schema question lookup
    """
    with SessionLocal() as session:
        # Check if project exists
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get question sets using helper function
        old_question_ids = get_schema_question_ids(project.schema_id, session)
        new_question_ids = get_schema_question_ids(new_schema_id, session)
        
        removed_question_ids = old_question_ids - new_question_ids
        
        # Count custom displays (all will be deleted in clean slate approach)
        custom_displays_count = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        ).count()
        
        # Count annotator answers (total and to be removed)
        total_answers = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.project_id == project_id
        ).count()
        
        answers_to_remove = 0
        if removed_question_ids:
            answers_to_remove = session.query(AnnotatorAnswer).filter(
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.question_id.in_(removed_question_ids)
            ).count()
        
        # Count ground truth (total and to be removed)
        total_gt = session.query(ReviewerGroundTruth).filter(
            ReviewerGroundTruth.project_id == project_id
        ).count()
        
        gt_to_remove = 0
        if removed_question_ids:
            gt_to_remove = session.query(ReviewerGroundTruth).filter(
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id.in_(removed_question_ids)
            ).count()
        
        return {
            "project_name": project.name,
            "current_schema": project.schema_id,
            "new_schema": new_schema_id,
            "questions_being_removed": len(removed_question_ids),
            "removed_question_ids": list(removed_question_ids),  # 添加具体的问题ID列表
            "custom_displays_to_delete": custom_displays_count,
            "total_answers": total_answers,
            "answers_to_delete": answers_to_remove,
            "total_ground_truth": total_gt,
            "ground_truth_to_delete": gt_to_remove
        }


def change_project_schema(project_id: int, new_schema_id: int):
    """Change project schema and clean up data with automatic backup"""
    # Create backup before making changes
    backup_path = _create_operation_backup(f"change_project_schema_{project_id}")
    
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project {project_id} not found")
        
        # Get removed questions
        from label_pizza.services import SchemaService
        old_questions = {q['id'] for q in SchemaService.get_schema_questions(project.schema_id, session)}
        new_questions = {q['id'] for q in SchemaService.get_schema_questions(new_schema_id, session)}
        removed_questions = old_questions - new_questions
        
        # Delete all custom displays (clean slate)
        session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id).delete()
        
        # Delete data for removed questions
        if removed_questions:
            session.query(AnnotatorAnswer).filter(
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.question_id.in_(removed_questions)).delete()
            session.query(ReviewerGroundTruth).filter(
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id.in_(removed_questions)).delete()
        
        # Update schema and reset completion status
        project.schema_id = new_schema_id
        session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id).update(
            {ProjectUserRole.completed_at: None})
        
        session.commit()
        print(f"✅ Updated project '{project.name}' to schema {new_schema_id}")


def check_project_data_before_delete(project_id: int):
    """
    Args:
        project_id: Project ID
        
    Returns:
        Dictionary with counts of data that will be deleted
    """
    with SessionLocal() as session:
        # Check if project exists
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Count data in each table
        data_counts = {}
        
        # 1. ProjectVideo
        data_counts["project_videos"] = session.query(ProjectVideo).filter(
            ProjectVideo.project_id == project_id
        ).count()
        
        # 2. ProjectUserRole
        data_counts["project_user_roles"] = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id
        ).count()
        
        # 3. ProjectVideoQuestionDisplay
        data_counts["custom_displays"] = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        ).count()
        
        # 4. AnnotatorAnswer
        data_counts["annotator_answers"] = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.project_id == project_id
        ).count()
        
        # 5. ReviewerGroundTruth
        data_counts["ground_truth"] = session.query(ReviewerGroundTruth).filter(
            ReviewerGroundTruth.project_id == project_id
        ).count()
        
        # 6. ProjectGroupProject
        data_counts["project_group_associations"] = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_id == project_id
        ).count()
        
        # Calculate total records
        total_records = sum(data_counts.values())
        
        print(f"=== PROJECT {project_id} DATA OVERVIEW ===")
        print(f"Project: {project.name}")
        print(f"Schema ID: {project.schema_id}")
        print(f"Created: {project.created_at}")
        print(f"Archived: {project.is_archived}")
        print()
        print(f"📊 DATA TO BE DELETED:")
        print(f"  Project videos: {data_counts['project_videos']}")
        print(f"  User roles: {data_counts['project_user_roles']}")
        print(f"  Custom displays: {data_counts['custom_displays']}")
        print(f"  Annotator answers: {data_counts['annotator_answers']}")
        print(f"  Ground truth: {data_counts['ground_truth']}")
        print(f"  Project group associations: {data_counts['project_group_associations']}")
        print(f"  Project record: 1")
        print(f"  📈 TOTAL RECORDS: {total_records + 1}")
        print()
        
        return {
            "project_info": {
                "id": project_id,
                "name": project.name,
                "schema_id": project.schema_id,
                "is_archived": project.is_archived
            },
            "data_counts": data_counts,
            "total_records": total_records + 1
        }


def delete_all_project_data(project_id: int, confirm_delete: bool = True):
    """
    Delete project and all related data
    Args:
        project_id: Project ID
        confirm_delete: Whether to ask for confirmation
        
    Returns:
        Dictionary with deletion results
    """
    # First, check what data exists
    try:
        data_overview = check_project_data_before_delete(project_id)
    except ValueError as e:
        print(f"❌ Error: {e}")
        return None
    
    # Ask for confirmation if needed
    if confirm_delete:
        print("⚠️  WARNING: This will PERMANENTLY DELETE all data for this project!")
        print("   This includes all answers, ground truth, user assignments, and the project itself.")
        response = input(f"\nConfirm deletion of project {project_id} '{data_overview['project_info']['name']}'? (DELETE/no): ")
        if response != "DELETE":
            print("❌ Deletion cancelled")
            return None
    
     # Create backup before deletion
    backup_path = _create_operation_backup(f"delete_project_{project_id}")
    
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project {project_id} not found")
        
        # Delete in dependency order
        answer_ids = [a.id for a in session.query(AnnotatorAnswer.id).filter(AnnotatorAnswer.project_id == project_id)]
        if answer_ids:
            session.query(AnswerReview).filter(AnswerReview.answer_id.in_(answer_ids)).delete()
        
        session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id == project_id).delete()
        session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id == project_id).delete()
        session.query(ProjectVideoQuestionDisplay).filter(ProjectVideoQuestionDisplay.project_id == project_id).delete()
        session.query(ProjectUserRole).filter(ProjectUserRole.project_id == project_id).delete()
        session.query(ProjectVideo).filter(ProjectVideo.project_id == project_id).delete()
        session.query(ProjectGroupProject).filter(ProjectGroupProject.project_id == project_id).delete()
        session.delete(project)
        
        session.commit()
        print(f"✅ Deleted project '{project.name}'")


def check_schema_data_before_delete(schema_id: int):
    """
    Args:
        schema_id: Schema ID
        
    Returns:
        Dictionary with counts of data that will be deleted
    """
    with SessionLocal() as session:
        # Check if schema exists
        schema = session.query(Schema).filter(Schema.id == schema_id).first()
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        # Find all projects using this schema
        projects_using_schema = session.query(Project).filter(
            Project.schema_id == schema_id
        ).all()
        
        project_ids = [p.id for p in projects_using_schema]
        
        data_counts = {}
        
        # 1. Projects using this schema
        data_counts["projects"] = len(project_ids)
        
        if project_ids:
            # 2. Project-related data that will be deleted
            data_counts["project_videos"] = session.query(ProjectVideo).filter(
                ProjectVideo.project_id.in_(project_ids)
            ).count()
            
            data_counts["project_user_roles"] = session.query(ProjectUserRole).filter(
                ProjectUserRole.project_id.in_(project_ids)
            ).count()
            
            data_counts["custom_displays"] = session.query(ProjectVideoQuestionDisplay).filter(
                ProjectVideoQuestionDisplay.project_id.in_(project_ids)
            ).count()
            
            # 3. All annotator answers for projects using this schema
            data_counts["annotator_answers"] = session.query(AnnotatorAnswer).filter(
                AnnotatorAnswer.project_id.in_(project_ids)
            ).count()
            
            # 4. All ground truth for projects using this schema
            data_counts["ground_truth"] = session.query(ReviewerGroundTruth).filter(
                ReviewerGroundTruth.project_id.in_(project_ids)
            ).count()
            
            # 5. Answer reviews for annotator answers
            answer_ids = session.query(AnnotatorAnswer.id).filter(
                AnnotatorAnswer.project_id.in_(project_ids)
            ).subquery()
            
            data_counts["answer_reviews"] = session.query(AnswerReview).filter(
                AnswerReview.answer_id.in_(answer_ids)
            ).count()
            
            # 6. Project group associations
            data_counts["project_group_associations"] = session.query(ProjectGroupProject).filter(
                ProjectGroupProject.project_id.in_(project_ids)
            ).count()
        else:
            # No projects using this schema
            for key in ["project_videos", "project_user_roles", "custom_displays", 
                       "annotator_answers", "ground_truth", "answer_reviews", 
                       "project_group_associations"]:
                data_counts[key] = 0
        
        # 7. Schema-question group relationships
        data_counts["schema_question_groups"] = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.schema_id == schema_id
        ).count()
        
        # Calculate total records
        total_records = sum(data_counts.values()) + 1  # +1 for schema itself
        
        print(f"=== SCHEMA {schema_id} DATA OVERVIEW ===")
        print(f"Schema: {schema.name}")
        print(f"Created: {schema.created_at}")
        print(f"Archived: {schema.is_archived}")
        print()
        print(f"📊 DATA TO BE DELETED:")
        print(f"  Projects using this schema: {data_counts['projects']}")
        print(f"  Project videos: {data_counts['project_videos']}")
        print(f"  User roles: {data_counts['project_user_roles']}")
        print(f"  Custom displays: {data_counts['custom_displays']}")
        print(f"  Annotator answers: {data_counts['annotator_answers']}")
        print(f"  Ground truth: {data_counts['ground_truth']}")
        print(f"  Answer reviews: {data_counts['answer_reviews']}")
        print(f"  Project group associations: {data_counts['project_group_associations']}")
        print(f"  Schema question groups: {data_counts['schema_question_groups']}")
        print(f"  Schema record: 1")
        print(f"  📈 TOTAL RECORDS: {total_records}")
        print()
        
        return {
            "schema_info": {
                "id": schema_id,
                "name": schema.name,
                "is_archived": schema.is_archived
            },
            "projects_using_schema": [{"id": p.id, "name": p.name} for p in projects_using_schema],
            "data_counts": data_counts,
            "total_records": total_records
        }


def delete_all_schema_data(schema_id: int, confirm_delete: bool = True):
    """
    delete all the data related to a schema (including all projects using it)
    Args:
        schema_id: Schema ID
        confirm_delete: Whether to ask for confirmation
        
    Returns:
        Dictionary with deletion results
    """
    # First, check what data exists
    try:
        data_overview = check_schema_data_before_delete(schema_id)
    except ValueError as e:
        print(f"❌ Error: {e}")
        return None
    
    # Ask for confirmation if needed
    if confirm_delete:
        print("⚠️  WARNING: This will PERMANENTLY DELETE the schema and ALL projects using it!")
        print("   This includes all answers, ground truth, user assignments, and projects.")
        projects_list = ', '.join([f"'{p['name']}'" for p in data_overview['projects_using_schema']])
        if projects_list:
            print(f"   Projects to be deleted: {projects_list}")
        
        response = input(f"\nConfirm deletion of schema {schema_id} '{data_overview['schema_info']['name']}'? (DELETE/no): ")
        if response != "DELETE":
            print("❌ Deletion cancelled")
            return None
    
    # Create backup before deletion
    backup_path = _create_operation_backup(f"delete_schema_{schema_id}")
    
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.id == schema_id).first()
        if not schema:
            raise ValueError(f"Schema {schema_id} not found")
        
        project_ids = [p.id for p in session.query(Project).filter(Project.schema_id == schema_id)]
        
        if project_ids:
            answer_ids = [a.id for a in session.query(AnnotatorAnswer.id).filter(AnnotatorAnswer.project_id.in_(project_ids))]
            if answer_ids:
                session.query(AnswerReview).filter(AnswerReview.answer_id.in_(answer_ids)).delete()
            
            session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id.in_(project_ids)).delete()
            session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id.in_(project_ids)).delete()
            session.query(ProjectVideoQuestionDisplay).filter(ProjectVideoQuestionDisplay.project_id.in_(project_ids)).delete()
            session.query(ProjectUserRole).filter(ProjectUserRole.project_id.in_(project_ids)).delete()
            session.query(ProjectVideo).filter(ProjectVideo.project_id.in_(project_ids)).delete()
            session.query(ProjectGroupProject).filter(ProjectGroupProject.project_id.in_(project_ids)).delete()
            session.query(Project).filter(Project.id.in_(project_ids)).delete()
        
        session.query(SchemaQuestionGroup).filter(SchemaQuestionGroup.schema_id == schema_id).delete()
        session.delete(schema)
        
        session.commit()
        print(f"✅ Deleted schema '{schema.name}' and {len(project_ids)} projects")


def check_question_group_data_before_delete(question_group_id: int):
    """
    Args:
        question_group_id: Question Group ID
        
    Returns:
        Dictionary with counts of data that will be deleted
    """
    with SessionLocal() as session:
        # Check if question group exists
        qg = session.query(QuestionGroup).filter(QuestionGroup.id == question_group_id).first()
        if not qg:
            raise ValueError(f"Question Group with ID {question_group_id} not found")
        
        data_counts = {}
        
        # 1. Question group relationships
        data_counts["question_group_questions"] = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id
        ).count()
        
        data_counts["schema_question_groups"] = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.question_group_id == question_group_id
        ).count()
        
        # Calculate total records
        total_records = sum(data_counts.values()) + 1  # +1 for question group itself
        
        print(f"=== QUESTION GROUP {question_group_id} DATA OVERVIEW ===")
        print(f"Question Group: {qg.title}")
        print(f"Display Title: {qg.display_title}")
        print(f"Is Reusable: {qg.is_reusable}")
        print(f"Is Archived: {qg.is_archived}")
        print()
        print(f"📊 DATA TO BE DELETED:")
        print(f"  Question group questions: {data_counts['question_group_questions']}")
        print(f"  Schema question groups: {data_counts['schema_question_groups']}")
        print(f"  Question group record: 1")
        print(f"  📈 TOTAL RECORDS: {total_records}")
        print()
        print("⚠️  NOTE: This will NOT delete Questions, Schemas, or project data.")
        print("   Only the question group and its relationships will be deleted.")
        print()
        
        return {
            "question_group_info": {
                "id": question_group_id,
                "title": qg.title,
                "display_title": qg.display_title,
                "is_reusable": qg.is_reusable,
                "is_archived": qg.is_archived
            },
            "data_counts": data_counts,
            "total_records": total_records
        }


def delete_all_question_group_data(question_group_id: int, confirm_delete: bool = True):
    """
    Args:
        question_group_id: Question Group ID
        confirm_delete: Whether to ask for confirmation
        
    Returns:
        Dictionary with deletion results
    """
    # First, check what data exists
    try:
        data_overview = check_question_group_data_before_delete(question_group_id)
    except ValueError as e:
        print(f"❌ Error: {e}")
        return None
    
    # Ask for confirmation if needed
    if confirm_delete:
        print("⚠️  WARNING: This will PERMANENTLY DELETE the question group and its relationships!")
        print("   This will NOT delete Questions, Schemas, or project data.")
        print("   Only the question group itself and its relationship records will be deleted.")
        
        response = input(f"\nConfirm deletion of question group {question_group_id} '{data_overview['question_group_info']['title']}'? (DELETE/no): ")
        if response != "DELETE":
            print("❌ Deletion cancelled")
            return None
    
    # Create backup before deletion
    backup_path = _create_operation_backup(f"delete_qgroup_{question_group_id}")
    
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.id == question_group_id).first()
        if not qg:
            raise ValueError(f"QuestionGroup {question_group_id} not found")
        
        session.query(SchemaQuestionGroup).filter(SchemaQuestionGroup.question_group_id == question_group_id).delete()
        session.query(QuestionGroupQuestion).filter(QuestionGroupQuestion.question_group_id == question_group_id).delete()
        session.delete(qg)
        
        session.commit()
        print(f"✅ Deleted question group '{qg.title}'")


def check_question_data_before_delete(question_id: int):
    """
    Args:
        question_id: Question ID
        
    Returns:
        Dictionary with counts of data that will be deleted
    """
    with SessionLocal() as session:
        # Check if question exists
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
        
        data_counts = {}
        
        # 1. Direct question relationships
        data_counts["question_group_questions"] = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_id == question_id
        ).count()
        
        # 2. All answers to this question
        data_counts["annotator_answers"] = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.question_id == question_id
        ).count()
        
        # 3. All ground truth for this question
        data_counts["ground_truth"] = session.query(ReviewerGroundTruth).filter(
            ReviewerGroundTruth.question_id == question_id
        ).count()
        
        # 4. Custom displays for this question
        data_counts["custom_displays"] = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.question_id == question_id
        ).count()
        
        # 5. Answer reviews for answers to this question
        answer_ids = session.query(AnnotatorAnswer.id).filter(
            AnnotatorAnswer.question_id == question_id
        ).subquery()
        
        data_counts["answer_reviews"] = session.query(AnswerReview).filter(
            AnswerReview.answer_id.in_(answer_ids)
        ).count()
        
        # Calculate total records
        total_records = sum(data_counts.values()) + 1  # +1 for question itself
        
        print(f"=== QUESTION {question_id} DATA OVERVIEW ===")
        print(f"Question: {question.text}")
        print(f"Display Text: {question.display_text}")
        print(f"Type: {question.type}")
        print(f"Is Archived: {question.is_archived}")
        print()
        print(f"📊 DATA TO BE DELETED:")
        print(f"  Question group questions: {data_counts['question_group_questions']}")
        print(f"  Annotator answers: {data_counts['annotator_answers']}")
        print(f"  Ground truth: {data_counts['ground_truth']}")
        print(f"  Custom displays: {data_counts['custom_displays']}")
        print(f"  Answer reviews: {data_counts['answer_reviews']}")
        print(f"  Question record: 1")
        print(f"  📈 TOTAL RECORDS: {total_records}")
        print()
        
        return {
            "question_info": {
                "id": question_id,
                "text": question.text,
                "display_text": question.display_text,
                "type": question.type,
                "is_archived": question.is_archived
            },
            "data_counts": data_counts,
            "total_records": total_records
        }


def delete_all_question_data(question_id: int, confirm_delete: bool = True):
    """Delete question and all related data"""
    # First, check what data exists
    try:
        data_overview = check_question_data_before_delete(question_id)
    except ValueError as e:
        print(f"❌ Error: {e}")
        return None
    
    # Ask for confirmation if needed
    if confirm_delete:
        print("⚠️  WARNING: This will PERMANENTLY DELETE the question and all related data!")
        print("   This includes all answers to this question across all projects.")
        
        response = input(f"\nConfirm deletion of question {question_id}? (DELETE/no): ")
        if response != "DELETE":
            print("❌ Deletion cancelled")
            return None
        
    # Create backup before deletion
    backup_path = _create_operation_backup(f"delete_qgroup_{question_id}")
        
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            raise ValueError(f"Question {question_id} not found")
        
        answer_ids = [a.id for a in session.query(AnnotatorAnswer.id).filter(AnnotatorAnswer.question_id == question_id)]
        if answer_ids:
            session.query(AnswerReview).filter(AnswerReview.answer_id.in_(answer_ids)).delete()
        
        session.query(AnnotatorAnswer).filter(AnnotatorAnswer.question_id == question_id).delete()
        session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.question_id == question_id).delete()
        session.query(ProjectVideoQuestionDisplay).filter(ProjectVideoQuestionDisplay.question_id == question_id).delete()
        session.query(QuestionGroupQuestion).filter(QuestionGroupQuestion.question_id == question_id).delete()
        session.delete(question)
        
        session.commit()
        print(f"✅ Deleted question '{question.text}'")


# 便捷函数
def quick_delete_schema(schema_id: int):
    """快速删除 schema (无确认提示)"""
    return delete_all_schema_data(schema_id, confirm_delete=False)


def safe_delete_schema(schema_id: int):
    """安全删除 schema (带确认提示)"""
    return delete_all_schema_data(schema_id, confirm_delete=True)


def preview_schema_deletion(schema_id: int):
    """预览 schema 删除 (只查看，不删除)"""
    try:
        return check_schema_data_before_delete(schema_id)
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def quick_delete_question_group(question_group_id: int):
    """快速删除 question group (无确认提示)"""
    return delete_all_question_group_data(question_group_id, confirm_delete=False)


def safe_delete_question_group(question_group_id: int):
    """安全删除 question group (带确认提示)"""
    return delete_all_question_group_data(question_group_id, confirm_delete=True)


def preview_question_group_deletion(question_group_id: int):
    """预览 question group 删除 (只查看，不删除)"""
    try:
        return check_question_group_data_before_delete(question_group_id)
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def quick_delete_question(question_id: int):
    """快速删除 question (无确认提示)"""
    return delete_all_question_data(question_id, confirm_delete=False)


def safe_delete_question(question_id: int):
    """安全删除 question (带确认提示)"""
    return delete_all_question_data(question_id, confirm_delete=True)


def preview_question_deletion(question_id: int):
    """预览 question 删除 (只查看，不删除)"""
    try:
        return check_question_data_before_delete(question_id)
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def find_questions_with_none_default():
    """
    只返回 default_option 为 None 的问题
    
    Returns:
        List of questions with None default_option
    """
    with SessionLocal() as session:
        questions_with_none = session.query(Question).filter(
            Question.default_option.is_(None)
        ).all()
        
        print(f"=== QUESTIONS WITH default_option = None ===")
        print(f"Found {len(questions_with_none)} questions")
        print()
        
        result = []
        for question in questions_with_none:
            q_info = {
                "id": question.id,
                "text": question.text,
                "display_text": question.display_text,
                "type": question.type,
                "options": question.options,
                "display_values": question.display_values,
                "is_archived": question.is_archived
            }
            result.append(q_info)
            
            archived_str = " [ARCHIVED]" if question.is_archived else ""
            print(f"ID {question.id}: {question.text}{archived_str}")
            print(f"  Type: {question.type}")
            if question.options:
                print(f"  Options: {question.options}")
            if question.display_values:
                print(f"  Display Values: {question.display_values}")
            print()
        
        return result


def update_question_default_option(question_id: int, new_default_option: str):
    """
    
    Args:
        question_id: Question ID
        new_default_option: New default option value
        
    Returns:
        Boolean indicating success
    """
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            print(f"❌ Question with ID {question_id} not found")
            return False
        
        old_default = question.default_option
        question.default_option = new_default_option
        
        try:
            session.commit()
            print(f"✅ Updated question {question_id} default_option:")
            print(f"  From: {old_default}")
            print(f"  To: {new_default_option}")
            return True
        except Exception as e:
            session.rollback()
            print(f"❌ Failed to update question {question_id}: {e}")
            return False
from sqlalchemy import text


def update_project_schema(project_name: str, new_schema_id: int):
    from label_pizza.services import ProjectService
    """
    Update project's schema and clear existing annotation data.
    
    Args:
        project_name: Name of the project to update
        new_schema_id: ID of the new schema to assign
    """
    with SessionLocal() as session:
        # Get project by name
        project = ProjectService.get_project_by_name(project_name, session)
        
        # Check if the new schema exists and is not archived
        new_schema = session.query(Schema).filter(Schema.id == new_schema_id).first()
        if not new_schema:
            raise ValueError(f"Schema with id {new_schema_id} not found")
        if new_schema.is_archived:
            raise ValueError(f"Schema with id {new_schema_id} is archived")
        
        # Clear existing annotation data
        session.query(AnnotatorAnswer).filter(AnnotatorAnswer.project_id == project.id).delete()
        session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.project_id == project.id).delete()
        
        # Update schema
        project.schema_id = new_schema_id
        
        session.commit()
        print(f"✅ Updated Project '{project_name}': schema_id changed to {new_schema_id}")

# for i in range(6, 44):
#     update_project_schema(project_name=f"Subject Lighting {i}", new_schema_id=11)

def check_question_78():
    """Check all data for question_id = 78"""
    question_id = 78
    
    with SessionLocal() as session:
        # Check if question exists
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            print(f"❌ Question {question_id} not found!")
            return
        
        print(f"Question {question_id}: {question.text}")
        
        # Count all related data
        counts = {
            "question_groups": session.query(QuestionGroupQuestion).filter(QuestionGroupQuestion.question_id == question_id).count(),
            "annotator_answers": session.query(AnnotatorAnswer).filter(AnnotatorAnswer.question_id == question_id).count(),
            "ground_truth": session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.question_id == question_id).count(),
            "custom_displays": session.query(ProjectVideoQuestionDisplay).filter(ProjectVideoQuestionDisplay.question_id == question_id).count()
        }
        
        total = sum(counts.values())
        print(f"📊 Related records: {counts} | Total: {total}")
        
        return counts


def get_question_78_details():
    """Get detailed data for question_id = 78"""
    question_id = 78
    
    with SessionLocal() as session:
        # Question groups
        qg_questions = session.query(QuestionGroupQuestion).filter(QuestionGroupQuestion.question_id == question_id).all()
        print(f"Question Groups: {[qgq.question_group_id for qgq in qg_questions]}")
        
        # Projects using this question
        project_ids = session.query(AnnotatorAnswer.project_id).filter(AnnotatorAnswer.question_id == question_id).distinct().all()
        project_ids = [p[0] for p in project_ids]
        print(f"Projects with answers: {project_ids}")
        
        # Answers by project
        for pid in project_ids[:3]:  # Show first 3 projects
            count = session.query(AnnotatorAnswer).filter(AnnotatorAnswer.question_id == question_id, AnnotatorAnswer.project_id == pid).count()
            print(f"  Project {pid}: {count} answers")


# Quick SQL queries
def sql_for_question_78():
    """Generate SQL queries for question_id = 78"""
    q_id = 78
    
    queries = [
        f"SELECT * FROM questions WHERE id = {q_id};",
        f"SELECT COUNT(*) FROM question_group_questions WHERE question_id = {q_id};",
        f"SELECT COUNT(*) FROM annotator_answers WHERE question_id = {q_id};",
        f"SELECT COUNT(*) FROM reviewer_ground_truth WHERE question_id = {q_id};",
        f"SELECT COUNT(*) FROM project_video_question_displays WHERE question_id = {q_id};"
    ]
    
    for query in queries:
        print(query)

def replace_question(old_id: int, new_id: int):
    """Replace all question_id old_id with new_id and delete original question"""
    with SessionLocal() as session:
        # Check if both questions exist
        old_question = session.query(Question).filter(Question.id == old_id).first()
        if not old_question:
            raise ValueError(f"Question with id {old_id} not found in database")
        
        new_question = session.query(Question).filter(Question.id == new_id).first()
        if not new_question:
            raise ValueError(f"Question with id {new_id} not found in database")
        
        # Update all tables with old_id to new_id
        session.query(QuestionGroupQuestion).filter(QuestionGroupQuestion.question_id == old_id).update({QuestionGroupQuestion.question_id: new_id})
        session.query(AnnotatorAnswer).filter(AnnotatorAnswer.question_id == old_id).update({AnnotatorAnswer.question_id: new_id})
        session.query(ReviewerGroundTruth).filter(ReviewerGroundTruth.question_id == old_id).update({ReviewerGroundTruth.question_id: new_id})
        session.query(ProjectVideoQuestionDisplay).filter(ProjectVideoQuestionDisplay.question_id == old_id).update({ProjectVideoQuestionDisplay.question_id: new_id})
        
        # Delete old question
        session.query(Question).filter(Question.id == old_id).delete()
        
        session.commit()
        print(f"✅ Replaced question {old_id} with {new_id} and deleted original")
        
# replace_question(old_id=78, new_id=58)
change_question_text("Glassy surface reflection?", "Glossy surface reflection?")

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