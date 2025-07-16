from label_pizza.db import init_database
init_database()
from label_pizza.models import *
from label_pizza.db import SessionLocal


def change_question_text(original_text, new_text):
    with SessionLocal() as session:
        # Check if the question exists
        question = session.query(Question).filter(Question.text == original_text).first()
        if not question:
            raise ValueError(f"Question '{original_text}' not found in database")
        # Check if the new text already exists
        if session.query(Question).filter(Question.text == new_text).first():
            raise ValueError(f"Question '{new_text}' already exists in database")
        question.text = new_text
        session.commit()
        
def update_question_group_titles(group_id: int, new_title: str, new_display_title: str = None):
    """
    Update title and display_title for a specific QuestionGroup
    
    Args:
        group_id: ID of the question group to update
        new_title: New title value
        new_display_title: New display_title value (defaults to new_title if not provided)
    """
    if new_display_title is None:
        new_display_title = new_title
    
    with SessionLocal() as session:
        # Check if the question group exists
        group = session.query(QuestionGroup).filter(QuestionGroup.id == group_id).first()
        if not group:
            raise ValueError(f"QuestionGroup with id {group_id} not found")
        
        # Check if the new title already exists (but allow if it's the same group)
        existing_group = session.query(QuestionGroup).filter(QuestionGroup.title == new_title).first()
        if existing_group and existing_group.id != group_id:
            raise ValueError(f"QuestionGroup with title '{new_title}' already exists in database")
        
        # Update the title and display_title
        group.title = new_title
        group.display_title = new_display_title
        
        session.commit()
        print(f"✅ Updated QuestionGroup {group_id}: title='{new_title}', display_title='{new_display_title}'")


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


def change_project_schema_simple(project_id: int, new_schema_id: int):
    """
    Simplified schema change using optimized question lookup
    """
    with SessionLocal() as session:
        # Check if project exists
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        print(f"Changing project '{project.name}' from schema {project.schema_id} to {new_schema_id}")
        
        # Get question sets using helper function
        old_question_ids = get_schema_question_ids(project.schema_id, session)
        new_question_ids = get_schema_question_ids(new_schema_id, session)
        
        removed_question_ids = old_question_ids - new_question_ids
        
        print(f"  Questions in old schema: {len(old_question_ids)}")
        print(f"  Questions in new schema: {len(new_question_ids)}")
        print(f"  Questions to be removed: {len(removed_question_ids)}")
        
        # 1. Delete ALL custom displays for this project (clean slate approach)
        # 这是关键！ProjectVideoQuestionDisplay 表的处理
        custom_displays = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        )
        deleted_displays = custom_displays.count()
        
        if deleted_displays > 0:
            print(f"  Deleting {deleted_displays} custom displays (clean slate approach)...")
            custom_displays.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_displays} custom displays")
        else:
            print(f"  No custom displays to delete")
        
        # 2. Delete answers for removed questions
        deleted_answers = 0
        if removed_question_ids:
            answers_to_delete = session.query(AnnotatorAnswer).filter(
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.question_id.in_(removed_question_ids)
            )
            deleted_answers = answers_to_delete.count()
            
            if deleted_answers > 0:
                print(f"  Deleting {deleted_answers} annotator answers for removed questions...")
                answers_to_delete.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_answers} annotator answers")
        
        # 3. Delete ground truth for removed questions
        deleted_gt = 0
        if removed_question_ids:
            gt_to_delete = session.query(ReviewerGroundTruth).filter(
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id.in_(removed_question_ids)
            )
            deleted_gt = gt_to_delete.count()
            
            if deleted_gt > 0:
                print(f"  Deleting {deleted_gt} ground truth answers for removed questions...")
                gt_to_delete.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_gt} ground truth answers")
        
        # 4. Update the schema
        old_schema_id = project.schema_id
        project.schema_id = new_schema_id
        print(f"  ✅ Updated project schema: {old_schema_id} → {new_schema_id}")
        
        # 5. Reset completion status for all users
        user_roles = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id
        )
        completed_users = user_roles.filter(
            ProjectUserRole.completed_at.isnot(None)
        ).count()
        
        if completed_users > 0:
            user_roles.update(
                {ProjectUserRole.completed_at: None},
                synchronize_session=False
            )
            print(f"  ✅ Reset completion status for {completed_users} users")
        else:
            print(f"  No completion status to reset")
        
        session.commit()
        print(f"🎉 Schema change completed successfully!")
        
        return {
            "success": True,
            "project_id": project_id,
            "project_name": project.name,
            "old_schema_id": old_schema_id,
            "new_schema_id": new_schema_id,
            "deleted_data": {
                "custom_displays": deleted_displays,
                "answers": deleted_answers,
                "ground_truth": deleted_gt,
                "reset_completions": completed_users
            }
        }


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
    
    with SessionLocal() as session:
        project = session.query(Project).filter(Project.id == project_id).first()
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        deleted_counts = {}
        
        print(f"🗑️  Starting deletion of project {project_id} '{project.name}'...")
        
        # Delete in reverse dependency order to avoid foreign key conflicts
        
        # 1. Delete AnswerReview (references annotator_answers.id)
        # Need to find answer reviews for this project's annotator answers
        answer_ids = session.query(AnnotatorAnswer.id).filter(
            AnnotatorAnswer.project_id == project_id
        ).subquery()
        
        answer_reviews = session.query(AnswerReview).filter(
            AnswerReview.answer_id.in_(answer_ids)
        )
        deleted_counts["answer_reviews"] = answer_reviews.count()
        if deleted_counts["answer_reviews"] > 0:
            answer_reviews.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['answer_reviews']} answer reviews")
        
        # 2. Delete AnnotatorAnswer
        annotator_answers = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.project_id == project_id
        )
        deleted_counts["annotator_answers"] = annotator_answers.count()
        if deleted_counts["annotator_answers"] > 0:
            annotator_answers.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['annotator_answers']} annotator answers")
        
        # 3. Delete ReviewerGroundTruth
        ground_truth = session.query(ReviewerGroundTruth).filter(
            ReviewerGroundTruth.project_id == project_id
        )
        deleted_counts["ground_truth"] = ground_truth.count()
        if deleted_counts["ground_truth"] > 0:
            ground_truth.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['ground_truth']} ground truth answers")
        
        # 4. Delete ProjectVideoQuestionDisplay
        custom_displays = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.project_id == project_id
        )
        deleted_counts["custom_displays"] = custom_displays.count()
        if deleted_counts["custom_displays"] > 0:
            custom_displays.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['custom_displays']} custom displays")
        
        # 5. Delete ProjectUserRole
        user_roles = session.query(ProjectUserRole).filter(
            ProjectUserRole.project_id == project_id
        )
        deleted_counts["project_user_roles"] = user_roles.count()
        if deleted_counts["project_user_roles"] > 0:
            user_roles.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['project_user_roles']} user role assignments")
        
        # 6. Delete ProjectVideo
        project_videos = session.query(ProjectVideo).filter(
            ProjectVideo.project_id == project_id
        )
        deleted_counts["project_videos"] = project_videos.count()
        if deleted_counts["project_videos"] > 0:
            project_videos.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['project_videos']} project-video associations")
        
        # 7. Delete ProjectGroupProject
        project_group_assocs = session.query(ProjectGroupProject).filter(
            ProjectGroupProject.project_id == project_id
        )
        deleted_counts["project_group_associations"] = project_group_assocs.count()
        if deleted_counts["project_group_associations"] > 0:
            project_group_assocs.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['project_group_associations']} project group associations")
        
        # 8. Finally, delete the Project itself
        project_name = project.name
        session.delete(project)
        deleted_counts["project"] = 1
        print(f"  ✅ Deleted project record")
        
        # Commit all deletions
        session.commit()
        
        total_deleted = sum(deleted_counts.values())
        print(f"🎉 Successfully deleted project '{project_name}' and all related data!")
        print(f"   Total records deleted: {total_deleted}")
        
        return {
            "success": True,
            "project_id": project_id,
            "project_name": project_name,
            "deleted_counts": deleted_counts,
            "total_deleted": total_deleted
        }


def check_schema_data_before_delete(schema_id: int):
    """
    检查 schema 相关数据 - 删除前预览
    
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
    删除 schema 的所有相关数据 (包括使用该 schema 的所有项目)
    
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
    
    with SessionLocal() as session:
        schema = session.query(Schema).filter(Schema.id == schema_id).first()
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        # Get all projects using this schema
        projects_using_schema = session.query(Project).filter(
            Project.schema_id == schema_id
        ).all()
        
        project_ids = [p.id for p in projects_using_schema]
        
        deleted_counts = {}
        
        print(f"🗑️  Starting deletion of schema {schema_id} '{schema.name}'...")
        
        # Delete all project-related data first (same order as project deletion)
        if project_ids:
            # 1. Delete answer reviews
            answer_ids = session.query(AnnotatorAnswer.id).filter(
                AnnotatorAnswer.project_id.in_(project_ids)
            ).subquery()
            
            answer_reviews = session.query(AnswerReview).filter(
                AnswerReview.answer_id.in_(answer_ids)
            )
            deleted_counts["answer_reviews"] = answer_reviews.count()
            if deleted_counts["answer_reviews"] > 0:
                answer_reviews.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['answer_reviews']} answer reviews")
            
            # 2. Delete annotator answers
            annotator_answers = session.query(AnnotatorAnswer).filter(
                AnnotatorAnswer.project_id.in_(project_ids)
            )
            deleted_counts["annotator_answers"] = annotator_answers.count()
            if deleted_counts["annotator_answers"] > 0:
                annotator_answers.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['annotator_answers']} annotator answers")
            
            # 3. Delete ground truth
            ground_truth = session.query(ReviewerGroundTruth).filter(
                ReviewerGroundTruth.project_id.in_(project_ids)
            )
            deleted_counts["ground_truth"] = ground_truth.count()
            if deleted_counts["ground_truth"] > 0:
                ground_truth.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['ground_truth']} ground truth answers")
            
            # 4. Delete custom displays
            custom_displays = session.query(ProjectVideoQuestionDisplay).filter(
                ProjectVideoQuestionDisplay.project_id.in_(project_ids)
            )
            deleted_counts["custom_displays"] = custom_displays.count()
            if deleted_counts["custom_displays"] > 0:
                custom_displays.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['custom_displays']} custom displays")
            
            # 5. Delete project user roles
            user_roles = session.query(ProjectUserRole).filter(
                ProjectUserRole.project_id.in_(project_ids)
            )
            deleted_counts["project_user_roles"] = user_roles.count()
            if deleted_counts["project_user_roles"] > 0:
                user_roles.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['project_user_roles']} user role assignments")
            
            # 6. Delete project videos
            project_videos = session.query(ProjectVideo).filter(
                ProjectVideo.project_id.in_(project_ids)
            )
            deleted_counts["project_videos"] = project_videos.count()
            if deleted_counts["project_videos"] > 0:
                project_videos.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['project_videos']} project-video associations")
            
            # 7. Delete project group associations
            project_group_assocs = session.query(ProjectGroupProject).filter(
                ProjectGroupProject.project_id.in_(project_ids)
            )
            deleted_counts["project_group_associations"] = project_group_assocs.count()
            if deleted_counts["project_group_associations"] > 0:
                project_group_assocs.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['project_group_associations']} project group associations")
            
            # 8. Delete projects
            projects = session.query(Project).filter(
                Project.id.in_(project_ids)
            )
            deleted_counts["projects"] = projects.count()
            if deleted_counts["projects"] > 0:
                projects.delete(synchronize_session=False)
                print(f"  ✅ Deleted {deleted_counts['projects']} projects")
        else:
            # No projects to delete
            for key in ["answer_reviews", "annotator_answers", "ground_truth", 
                       "custom_displays", "project_user_roles", "project_videos",
                       "project_group_associations", "projects"]:
                deleted_counts[key] = 0
        
        # 9. Delete schema-question group relationships
        schema_qgs = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.schema_id == schema_id
        )
        deleted_counts["schema_question_groups"] = schema_qgs.count()
        if deleted_counts["schema_question_groups"] > 0:
            schema_qgs.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['schema_question_groups']} schema question groups")
        
        # 10. Finally, delete the schema itself
        schema_name = schema.name
        session.delete(schema)
        deleted_counts["schema"] = 1
        print(f"  ✅ Deleted schema record")
        
        # Commit all deletions
        session.commit()
        
        total_deleted = sum(deleted_counts.values())
        print(f"🎉 Successfully deleted schema '{schema_name}' and all related data!")
        print(f"   Total records deleted: {total_deleted}")
        
        return {
            "success": True,
            "schema_id": schema_id,
            "schema_name": schema_name,
            "deleted_counts": deleted_counts,
            "total_deleted": total_deleted
        }


def check_question_group_data_before_delete(question_group_id: int):
    """
    检查 question group 相关数据 - 删除前预览
    
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
    删除 question group 的所有相关数据
    注意：这只删除 question group 和其关系表，不删除 questions 或 schemas
    
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
    
    with SessionLocal() as session:
        qg = session.query(QuestionGroup).filter(QuestionGroup.id == question_group_id).first()
        if not qg:
            raise ValueError(f"Question Group with ID {question_group_id} not found")
        
        deleted_counts = {}
        
        print(f"🗑️  Starting deletion of question group {question_group_id} '{qg.title}'...")
        
        # 1. Delete schema-question group relationships
        schema_qgs = session.query(SchemaQuestionGroup).filter(
            SchemaQuestionGroup.question_group_id == question_group_id
        )
        deleted_counts["schema_question_groups"] = schema_qgs.count()
        if deleted_counts["schema_question_groups"] > 0:
            schema_qgs.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['schema_question_groups']} schema question groups")
        
        # 2. Delete question group-question relationships
        qg_questions = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_group_id == question_group_id
        )
        deleted_counts["question_group_questions"] = qg_questions.count()
        if deleted_counts["question_group_questions"] > 0:
            qg_questions.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['question_group_questions']} question group questions")
        
        # 3. Finally, delete the question group itself
        qg_title = qg.title
        session.delete(qg)
        deleted_counts["question_group"] = 1
        print(f"  ✅ Deleted question group record")
        
        # Commit all deletions
        session.commit()
        
        total_deleted = sum(deleted_counts.values())
        print(f"🎉 Successfully deleted question group '{qg_title}' and all related data!")
        print(f"   Total records deleted: {total_deleted}")
        
        return {
            "success": True,
            "question_group_id": question_group_id,
            "question_group_title": qg_title,
            "deleted_counts": deleted_counts,
            "total_deleted": total_deleted
        }


def check_question_data_before_delete(question_id: int):
    """
    检查 question 相关数据 - 删除前预览
    
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
    """
    删除 question 的所有相关数据 (包括 question 本身)
    
    Args:
        question_id: Question ID
        confirm_delete: Whether to ask for confirmation
        
    Returns:
        Dictionary with deletion results
    """
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
    
    with SessionLocal() as session:
        question = session.query(Question).filter(Question.id == question_id).first()
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
        
        deleted_counts = {}
        
        print(f"🗑️  Starting deletion of question {question_id}...")
        
        # Delete in dependency order
        
        # 1. Delete answer reviews
        answer_ids = session.query(AnnotatorAnswer.id).filter(
            AnnotatorAnswer.question_id == question_id
        ).subquery()
        
        answer_reviews = session.query(AnswerReview).filter(
            AnswerReview.answer_id.in_(answer_ids)
        )
        deleted_counts["answer_reviews"] = answer_reviews.count()
        if deleted_counts["answer_reviews"] > 0:
            answer_reviews.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['answer_reviews']} answer reviews")
        
        # 2. Delete annotator answers
        annotator_answers = session.query(AnnotatorAnswer).filter(
            AnnotatorAnswer.question_id == question_id
        )
        deleted_counts["annotator_answers"] = annotator_answers.count()
        if deleted_counts["annotator_answers"] > 0:
            annotator_answers.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['annotator_answers']} annotator answers")
        
        # 3. Delete ground truth
        ground_truth = session.query(ReviewerGroundTruth).filter(
            ReviewerGroundTruth.question_id == question_id
        )
        deleted_counts["ground_truth"] = ground_truth.count()
        if deleted_counts["ground_truth"] > 0:
            ground_truth.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['ground_truth']} ground truth answers")
        
        # 4. Delete custom displays
        custom_displays = session.query(ProjectVideoQuestionDisplay).filter(
            ProjectVideoQuestionDisplay.question_id == question_id
        )
        deleted_counts["custom_displays"] = custom_displays.count()
        if deleted_counts["custom_displays"] > 0:
            custom_displays.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['custom_displays']} custom displays")
        
        # 5. Delete question group-question relationships
        qg_questions = session.query(QuestionGroupQuestion).filter(
            QuestionGroupQuestion.question_id == question_id
        )
        deleted_counts["question_group_questions"] = qg_questions.count()
        if deleted_counts["question_group_questions"] > 0:
            qg_questions.delete(synchronize_session=False)
            print(f"  ✅ Deleted {deleted_counts['question_group_questions']} question group questions")
        
        # 6. Finally, delete the question itself
        question_text = question.text
        session.delete(question)
        deleted_counts["question"] = 1
        print(f"  ✅ Deleted question record")
        
        # Commit all deletions
        session.commit()
        
        total_deleted = sum(deleted_counts.values())
        print(f"🎉 Successfully deleted question '{question_text}' and all related data!")
        print(f"   Total records deleted: {total_deleted}")
        
        return {
            "success": True,
            "question_id": question_id,
            "question_text": question_text,
            "deleted_counts": deleted_counts,
            "total_deleted": total_deleted
        }


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


# 批量删除函数
def delete_multiple_schemas(schema_ids: list, confirm_each: bool = False):
    """
    批量删除多个 schemas
    
    Args:
        schema_ids: List of schema IDs
        confirm_each: Whether to confirm each deletion individually
        
    Returns:
        Dictionary with results for each schema
    """
    if not schema_ids:
        print("❌ No schema IDs provided")
        return {}
    
    print(f"📋 Preparing to delete {len(schema_ids)} schemas...")
    
    # Preview all schemas first
    total_records = 0
    valid_schemas = []
    
    for schema_id in schema_ids:
        try:
            overview = check_schema_data_before_delete(schema_id)
            valid_schemas.append(schema_id)
            total_records += overview["total_records"]
            print("---")
        except Exception as e:
            print(f"❌ Schema {schema_id}: {e}")
            print("---")
    
    if not valid_schemas:
        print("❌ No valid schemas to delete")
        return {}
    
    print(f"📊 SUMMARY: {len(valid_schemas)} valid schemas, {total_records} total records")
    
    # Global confirmation for batch
    if not confirm_each:
        response = input(f"\nConfirm batch deletion of {len(valid_schemas)} schemas? (DELETE_ALL/no): ")
        if response != "DELETE_ALL":
            print("❌ Batch deletion cancelled")
            return {}
    
    # Delete each schema
    results = {}
    for i, schema_id in enumerate(valid_schemas, 1):
        print(f"\n🗑️  Deleting schema {i}/{len(valid_schemas)}: {schema_id}")
        try:
            result = delete_all_schema_data(schema_id, confirm_delete=confirm_each)
            results[schema_id] = result
        except Exception as e:
            print(f"❌ Failed to delete schema {schema_id}: {e}")
            results[schema_id] = {"success": False, "error": str(e)}
    
    # Summary
    successful = sum(1 for r in results.values() if r and r.get("success"))
    failed = len(results) - successful
    
    print(f"\n🎉 Batch schema deletion completed!")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    
    return results


def delete_multiple_question_groups(question_group_ids: list, confirm_each: bool = False):
    """
    批量删除多个 question groups
    
    Args:
        question_group_ids: List of question group IDs
        confirm_each: Whether to confirm each deletion individually
        
    Returns:
        Dictionary with results for each question group
    """
    if not question_group_ids:
        print("❌ No question group IDs provided")
        return {}
    
    print(f"📋 Preparing to delete {len(question_group_ids)} question groups...")
    
    # Preview all question groups first
    total_records = 0
    valid_qgs = []
    
    for qg_id in question_group_ids:
        try:
            overview = check_question_group_data_before_delete(qg_id)
            valid_qgs.append(qg_id)
            total_records += overview["total_records"]
            print("---")
        except Exception as e:
            print(f"❌ Question Group {qg_id}: {e}")
            print("---")
    
    if not valid_qgs:
        print("❌ No valid question groups to delete")
        return {}
    
    print(f"📊 SUMMARY: {len(valid_qgs)} valid question groups, {total_records} total records")
    
    # Global confirmation for batch
    if not confirm_each:
        response = input(f"\nConfirm batch deletion of {len(valid_qgs)} question groups? (DELETE_ALL/no): ")
        if response != "DELETE_ALL":
            print("❌ Batch deletion cancelled")
            return {}
    
    # Delete each question group
    results = {}
    for i, qg_id in enumerate(valid_qgs, 1):
        print(f"\n🗑️  Deleting question group {i}/{len(valid_qgs)}: {qg_id}")
        try:
            result = delete_all_question_group_data(qg_id, confirm_delete=confirm_each)
            results[qg_id] = result
        except Exception as e:
            print(f"❌ Failed to delete question group {qg_id}: {e}")
            results[qg_id] = {"success": False, "error": str(e)}
    
    # Summary
    successful = sum(1 for r in results.values() if r and r.get("success"))
    failed = len(results) - successful
    
    print(f"\n🎉 Batch question group deletion completed!")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    
    return results


def delete_multiple_questions(question_ids: list, confirm_each: bool = False):
    """
    批量删除多个 questions
    
    Args:
        question_ids: List of question IDs
        confirm_each: Whether to confirm each deletion individually
        
    Returns:
        Dictionary with results for each question
    """
    if not question_ids:
        print("❌ No question IDs provided")
        return {}
    
    print(f"📋 Preparing to delete {len(question_ids)} questions...")
    
    # Preview all questions first
    total_records = 0
    valid_questions = []
    
    for question_id in question_ids:
        try:
            overview = check_question_data_before_delete(question_id)
            valid_questions.append(question_id)
            total_records += overview["total_records"]
            print("---")
        except Exception as e:
            print(f"❌ Question {question_id}: {e}")
            print("---")
    
    if not valid_questions:
        print("❌ No valid questions to delete")
        return {}
    
    print(f"📊 SUMMARY: {len(valid_questions)} valid questions, {total_records} total records")
    
    # Global confirmation for batch
    if not confirm_each:
        response = input(f"\nConfirm batch deletion of {len(valid_questions)} questions? (DELETE_ALL/no): ")
        if response != "DELETE_ALL":
            print("❌ Batch deletion cancelled")
            return {}
    
    # Delete each question
    results = {}
    for i, question_id in enumerate(valid_questions, 1):
        print(f"\n🗑️  Deleting question {i}/{len(valid_questions)}: {question_id}")
        try:
            result = delete_all_question_data(question_id, confirm_delete=confirm_each)
            results[question_id] = result
        except Exception as e:
            print(f"❌ Failed to delete question {question_id}: {e}")
            results[question_id] = {"success": False, "error": str(e)}
    
    # Summary
    successful = sum(1 for r in results.values() if r and r.get("success"))
    failed = len(results) - successful
    
    print(f"\n🎉 Batch question deletion completed!")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    
    return results


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

for i in range(6, 44):
    update_project_schema(project_name=f"Subject Lighting {i}", new_schema_id=11)



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