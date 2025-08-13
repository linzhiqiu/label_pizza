from sqlalchemy import select, insert, update, func, delete, exists, join, distinct, and_, or_, case, text
from sqlalchemy.orm import Session, selectinload, joinedload, contains_eager  
import label_pizza.db
from typing import List, Optional, Dict, Any, Tuple
import pandas as pd
import json
import os
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, AnnotatorAnswer, ReviewerGroundTruth, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup, ProjectGroup, ProjectGroupProject,
    ProjectVideoQuestionDisplay
)


def export_users(output_path: str = None):
    # Query all users
    with label_pizza.db.SessionLocal() as session:
        stmt = select(User)
        users = session.execute(stmt).scalars().all()

        # Convert to desired format
        users_data = []
        for user in users:
            users_data.append({
                "user_id": user.user_id_str,
                "email": user.email,
                "password": user.password_hash,
                "user_type": user.user_type,
                "is_active": not user.is_archived
            })

        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(users_data, f, indent=2, ensure_ascii=False)

        print(f"Exported {len(users_data)} users to users.json")
    

def export_videos(output_path: str = None):
    with label_pizza.db.SessionLocal() as session:
        # Query all videos
        stmt = select(Video)
        videos = session.execute(stmt).scalars().all()
        
        # Convert to desired format
        videos_data = []
        for video in videos:
            videos_data.append({
                "video_uid": video.video_uid,
                "url": video.url,
                "metadata": video.video_metadata,
                "is_active": not video.is_archived
            })
        
        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(videos_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(videos_data)} videos to {output_path}")

def export_question_groups(output_folder: str = None):
    """Export all question groups to separate JSON files"""
    with label_pizza.db.SessionLocal() as session:
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Query all question groups
        stmt = select(QuestionGroup)
        question_groups = session.execute(stmt).scalars().all()
        
        for qg in question_groups:
            # Get questions for this group ordered by display_order
            stmt_questions = (
                select(Question, QuestionGroupQuestion.display_order)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == qg.id)
                .order_by(QuestionGroupQuestion.display_order)
            )
            questions_data = session.execute(stmt_questions).all()
            
            # Build question group data
            qg_data = {
                "title": qg.title,
                "display_title": qg.display_title,
                "description": qg.description,
                "is_reusable": qg.is_reusable,
                "is_auto_submit": qg.is_auto_submit,
                "verification_function": qg.verification_function,
                "questions": []
            }
            
            # Add questions
            for question, _ in questions_data:
                q_data = {
                    "qtype": question.type,
                    "text": question.text,
                    "display_text": question.display_text
                }
                
                if question.type == "single":
                    q_data["options"] = question.options
                    q_data["display_values"] = question.display_values
                    q_data["option_weights"] = question.option_weights
                    q_data["default_option"] = question.default_option
                else:  # description type
                        if question.default_option is not None:
                            q_data["default_option"] = question.default_option
                
                qg_data["questions"].append(q_data)
            
            # Write to file
            filename = f"{qg.title.replace(' ', '_').replace('/', '_')}.json"
            filepath = os.path.join(output_folder, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(qg_data, f, indent=4, ensure_ascii=False)
        
        print(f"Exported {len(question_groups)} question groups to {output_folder}")


def export_schemas(output_path: str = None):
    """Export all schemas to JSON format"""
    with label_pizza.db.SessionLocal() as session:
        # Query all schemas
        stmt = select(Schema)
        schemas = session.execute(stmt).scalars().all()
        
        schemas_data = []
        for schema in schemas:
            # Get question groups for this schema ordered by display_order
            stmt_qgroups = (
                select(QuestionGroup, SchemaQuestionGroup.display_order)
                .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == schema.id)
                .order_by(SchemaQuestionGroup.display_order)
            )
            qgroups_data = session.execute(stmt_qgroups).all()
            
            # Build schema data
            schema_data = {
                "schema_name": schema.name,
                "instructions_url": schema.instructions_url or "",
                "has_custom_display": schema.has_custom_display,
                "is_active": not schema.is_archived,
                "question_group_names": [qgroup.title for qgroup, _ in qgroups_data]
            }
            schemas_data.append(schema_data)
        
        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schemas_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(schemas_data)} schemas to {output_path}")


def export_projects(output_path: str = None):
    """Export all projects to JSON format"""
    with label_pizza.db.SessionLocal() as session:
        # Query all projects
        stmt = select(Project)
        projects = session.execute(stmt).scalars().all()
        
        projects_data = []
        for project in projects:
            # Get schema name
            schema_stmt = select(Schema.name).where(Schema.id == project.schema_id)
            schema_name = session.execute(schema_stmt).scalar_one()
            
            # Get videos for this project
            videos_stmt = (
                select(Video.video_uid, Video.id)
                .join(ProjectVideo, Video.id == ProjectVideo.video_id)
                .where(ProjectVideo.project_id == project.id)
                .order_by(Video.video_uid)
            )
            video_results = session.execute(videos_stmt).all()
            
            # Check if this project has any custom question displays
            custom_display_stmt = (
                select(ProjectVideoQuestionDisplay)
                .where(ProjectVideoQuestionDisplay.project_id == project.id)
            )
            has_custom_displays = session.execute(custom_display_stmt).first() is not None
            
            if has_custom_displays:
                # Build detailed structure with custom questions
                videos_data = []
                
                for video_uid, video_id in video_results:
                    # Get custom displays for this video in this project
                    custom_displays_stmt = (
                        select(ProjectVideoQuestionDisplay, Question.text, Question.type, Question.options, Question.display_values)
                        .join(Question, ProjectVideoQuestionDisplay.question_id == Question.id)
                        .where(ProjectVideoQuestionDisplay.project_id == project.id)
                        .where(ProjectVideoQuestionDisplay.video_id == video_id)
                    )
                    custom_displays = session.execute(custom_displays_stmt).all()
                    
                    if custom_displays:
                        questions_data = []
                        
                        for custom_display, question_text, question_type, options, display_values in custom_displays:
                            question_data = {
                                "question_text": question_text
                            }
                            
                            # Add custom question text if available
                            if custom_display.custom_display_text:
                                question_data["custom_question"] = custom_display.custom_display_text
                            
                            # Add custom options if available and question is single choice
                            if question_type == "single" and custom_display.custom_option_display_map:
                                custom_options = {}
                                for option_key, display_text in custom_display.custom_option_display_map.items():
                                    custom_options[f"Option {option_key}"] = display_text
                                question_data["custom_option"] = custom_options
                            
                            questions_data.append(question_data)
                        
                        video_data = {
                            "video_uid": video_uid,
                            "questions": questions_data
                        }
                        videos_data.append(video_data)
                
                # Build project data with detailed video structure
                project_data = {
                    "project_name": project.name,
                    "schema_name": schema_name,
                    "description": project.description or "",
                    "is_active": not project.is_archived,
                    "videos": videos_data
                }
            else:
                # Build simple structure with just video UIDs (original format)
                video_uids = [video_uid for video_uid, _ in video_results]
                project_data = {
                    "project_name": project.name,
                    "description": project.description or "",
                    "schema_name": schema_name,
                    "videos": video_uids,
                    "is_active": not project.is_archived
                }
            
            projects_data.append(project_data)
        
        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(projects_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(projects_data)} projects to {output_path}")
    
def export_project_groups(output_path: str = None):
    """Export all project groups to JSON format"""
    with label_pizza.db.SessionLocal() as session:
        # Query all project groups
        stmt = select(ProjectGroup)
        project_groups = session.execute(stmt).scalars().all()
        
        project_groups_data = []
        for pg in project_groups:
            # Get projects for this project group
            projects_stmt = (
                select(Project.name)
                .join(ProjectGroupProject, Project.id == ProjectGroupProject.project_id)
                .where(ProjectGroupProject.project_group_id == pg.id)
                .order_by(Project.name)
            )
            project_names = session.execute(projects_stmt).scalars().all()
            
            # Build project group data
            pg_data = {
                "project_group_name": pg.name,
                "description": pg.description or "",
                "projects": list(project_names)
            }
            project_groups_data.append(pg_data)
        
        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(project_groups_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(project_groups_data)} project groups to {output_path}")
    


def export_assignments(output_path: str = None):
    """Export all project user role assignments to JSON format"""
    with label_pizza.db.SessionLocal() as session:
        # Define role hierarchy (higher number = higher authority)
        ROLE_HIERARCHY = {
            "annotator": 1,
            "reviewer": 2,
            "admin": 3
        }
        
        # Query all project user roles with user and project names
        stmt = (
            select(User.user_id_str, Project.name, ProjectUserRole.role, 
                ProjectUserRole.user_weight, ProjectUserRole.is_archived, User.id, Project.id)
            .join(User, ProjectUserRole.user_id == User.id)
            .join(Project, ProjectUserRole.project_id == Project.id)
            .order_by(User.user_id_str, Project.name)
        )
        all_assignments = session.execute(stmt).all()
        
        # Group assignments by (user_id, project_id) and find highest role
        user_project_assignments = {}
        admin_users = set()
        
        for user_name, project_name, role, user_weight, is_archived, user_id, project_id in all_assignments:
            # Track admin users to skip them entirely
            if role == "admin":
                admin_users.add(user_id)
                continue
                
            key = (user_id, project_id)
            assignment_data = {
                "user_name": user_name,
                "project_name": project_name,
                "role": role,
                "user_weight": user_weight or 1.0,
                "is_active": not is_archived
            }
            
            if key not in user_project_assignments:
                user_project_assignments[key] = assignment_data
            else:
                # Compare roles and keep the highest authority one
                current_role = user_project_assignments[key]["role"]
                current_priority = ROLE_HIERARCHY.get(current_role, 0)
                new_priority = ROLE_HIERARCHY.get(role, 0)
                
                if new_priority > current_priority:
                    user_project_assignments[key] = assignment_data
                elif new_priority == current_priority:
                    # If same role priority, prefer active assignments
                    if not is_archived and not user_project_assignments[key]["is_active"]:
                        user_project_assignments[key] = assignment_data
        
        # Filter out any assignments from admin users
        final_assignments = []
        for (user_id, project_id), assignment_data in user_project_assignments.items():
            if user_id not in admin_users:
                final_assignments.append(assignment_data)
        
        # Sort by user_name and project_name for consistent output
        final_assignments.sort(key=lambda x: (x["user_name"], x["project_name"]))
        
        # Write to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(final_assignments, f, indent=2, ensure_ascii=False)
        
        print(f"Exported {len(final_assignments)} assignments to {output_path}")
    

def export_annotations(output_folder: str = None):
    """Export all annotations grouped by question group to separate JSON files"""
    with label_pizza.db.SessionLocal() as session:
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Get all question groups
        stmt_qgroups = select(QuestionGroup)
        question_groups = session.execute(stmt_qgroups).scalars().all()
        
        for qg in question_groups:
            annotations_data = []
            
            # Get questions in this question group
            stmt_questions = (
                select(Question.id, Question.text)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == qg.id)
                .order_by(QuestionGroupQuestion.display_order)
            )
            questions = session.execute(stmt_questions).all()
            question_ids = [q.id for q in questions]
            question_texts = {q.id: q.text for q in questions}
            
            # Get annotator answers for these questions
            stmt_annotations = (
                select(AnnotatorAnswer, User.user_id_str, Project.name, Video.video_uid)
                .join(User, AnnotatorAnswer.user_id == User.id)
                .join(Project, AnnotatorAnswer.project_id == Project.id)
                .join(Video, AnnotatorAnswer.video_id == Video.id)
                .where(AnnotatorAnswer.question_id.in_(question_ids))
                .order_by(Project.name, User.user_id_str, Video.video_uid)
            )
            annotator_results = session.execute(stmt_annotations).all()
            
            # Group answers by (project, user, video)
            grouped_answers = {}
            
            # Process annotator answers
            for answer, user_name, project_name, video_uid in annotator_results:
                key = (project_name, user_name, video_uid, False)  # False = not ground truth
                if key not in grouped_answers:
                    grouped_answers[key] = {}
                
                # Store both answer value and confidence score
                answer_data = {
                    "value": answer.answer_value
                }
                
                # Only include confidence_score if it's not None
                if answer.confidence_score is not None:
                    answer_data["confidence_score"] = answer.confidence_score
                
                grouped_answers[key][answer.question_id] = answer_data
            
            # Convert to output format
            for (project_name, user_name, video_uid, is_gt), answers in grouped_answers.items():
                # Build answers and confidence_scores dictionaries separately
                formatted_answers = {}
                confidence_scores = {}
                has_confidence_scores = False
                
                for q_id, answer_data in answers.items():
                    question_text = question_texts[q_id]
                    formatted_answers[question_text] = answer_data["value"]
                    
                    if "confidence_score" in answer_data:
                        confidence_scores[question_text] = answer_data["confidence_score"]
                        has_confidence_scores = True
                
                annotation_data = {
                    "question_group_title": qg.title,
                    "project_name": project_name,
                    "user_name": user_name,
                    "video_uid": video_uid,
                    "answers": formatted_answers,
                    "is_ground_truth": is_gt
                }
                
                # Only add confidence_scores if there are any
                if has_confidence_scores:
                    annotation_data["confidence_scores"] = confidence_scores
                
                annotations_data.append(annotation_data)
            
            # Write to file
            filename = f"{qg.title.replace(' ', '_').replace('/', '_')}_annotations.json"
            filepath = os.path.join(output_folder, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(annotations_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported annotations for {len(question_groups)} question groups to {output_folder}")


def export_ground_truths(output_folder: str = None):
    """Export all ground truth annotations grouped by question group to separate JSON files"""
    with label_pizza.db.SessionLocal() as session:
        # Create output folder if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Get all question groups
        stmt_qgroups = select(QuestionGroup)
        question_groups = session.execute(stmt_qgroups).scalars().all()
        
        for qg in question_groups:
            ground_truths_data = []
            
            # Get questions in this question group
            stmt_questions = (
                select(Question.id, Question.text)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == qg.id)
                .order_by(QuestionGroupQuestion.display_order)
            )
            questions = session.execute(stmt_questions).all()
            question_ids = [q.id for q in questions]
            question_texts = {q.id: q.text for q in questions}
            
            # Get ground truth answers for these questions
            stmt_gt = (
                select(ReviewerGroundTruth, User.user_id_str, Project.name, Video.video_uid)
                .join(User, ReviewerGroundTruth.reviewer_id == User.id)
                .join(Project, ReviewerGroundTruth.project_id == Project.id)
                .join(Video, ReviewerGroundTruth.video_id == Video.id)
                .where(ReviewerGroundTruth.question_id.in_(question_ids))
                .order_by(Project.name, User.user_id_str, Video.video_uid)
            )
            gt_results = session.execute(stmt_gt).all()
            
            # Group ground truth answers by (project, user, video)
            grouped_gts = {}
            
            for gt, user_name, project_name, video_uid in gt_results:
                key = (project_name, user_name, video_uid)
                if key not in grouped_gts:
                    grouped_gts[key] = {}
                grouped_gts[key][gt.question_id] = gt.answer_value
            
            # Convert to output format
            for (project_name, user_name, video_uid), answers in grouped_gts.items():
                gt_data = {
                    "question_group_title": qg.title,
                    "project_name": project_name,
                    "user_name": user_name,
                    "video_uid": video_uid,
                    "answers": {question_texts[q_id]: answer for q_id, answer in answers.items()},
                    "is_ground_truth": True
                }
                ground_truths_data.append(gt_data)
            
            # Write to file
            filename = f"{qg.title.replace(' ', '_').replace('/', '_')}_ground_truths.json"
            filepath = os.path.join(output_folder, filename)
            
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(ground_truths_data, f, indent=2, ensure_ascii=False)
        
        print(f"Exported ground truths for {len(question_groups)} question groups to {output_folder}")


def export_workspace(output_folder: str = None):
    os.makedirs(output_folder, exist_ok=True)
    video_path = os.path.join(output_folder, 'videos.json')
    export_videos(video_path)
    user_path = os.path.join(output_folder, 'users.json')
    export_users(user_path)
    question_group_folder = os.path.join(output_folder, 'question_groups')
    os.makedirs(question_group_folder, exist_ok=True)
    export_question_groups(question_group_folder)
    schema_path = os.path.join(output_folder, 'schemas.json')
    export_schemas(schema_path)
    project_path = os.path.join(output_folder, 'projects.json')
    export_projects(project_path)
    project_group_path = os.path.join(output_folder, 'project_groups.json')
    export_project_groups(project_group_path)
    assignment_path = os.path.join(output_folder, 'assignments.json')
    export_assignments(assignment_path)
    annotations_folder = os.path.join(output_folder, 'annotations')
    os.makedirs(annotations_folder, exist_ok=True)
    export_annotations(annotations_folder)
    ground_truths_folder = os.path.join(output_folder, 'ground_truths')
    os.makedirs(ground_truths_folder, exist_ok=True)
    export_ground_truths(ground_truths_folder)
