# services.py
from sqlalchemy import select, insert, update, func, delete, exists
from sqlalchemy.orm import Session
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, AnnotatorAnswer, ReviewerGroundTruth, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup, ProjectGroup, ProjectGroupProject
)
from typing import List, Optional, Dict, Any
import pandas as pd
from datetime import datetime, timezone
import hashlib
import os
from dotenv import load_dotenv
import importlib.util
import sys
from pathlib import Path

load_dotenv()

# Import verify module
verify_path = Path(__file__).parent / "verify.py"
spec = importlib.util.spec_from_file_location("verify", verify_path)
verify = importlib.util.module_from_spec(spec)
sys.modules["verify"] = verify
spec.loader.exec_module(verify)


class VideoService:
    @staticmethod
    def get_video_by_uid(video_uid: str, session: Session) -> Optional[Video]:
        """Get a video by its UID.
        
        Args:
            video_uid: The UID of the video
            session: Database session
            
        Returns:
            Video object if found, None otherwise
        """
        return session.scalar(select(Video).where(Video.video_uid == video_uid))

    @staticmethod
    def get_video_url(video_id: int, session: Session) -> str:
        """Get a video's URL by its ID.
        
        Args:
            video_id: The ID of the video
            session: Database session
            
        Returns:
            The video's URL
            
        Raises:
            ValueError: If video not found
        """
        video = session.get(Video, video_id)
        if not video:
            raise ValueError(f"Video with ID {video_id} not found")
        return video.url

    @staticmethod
    def get_video_metadata(video_id: int, session: Session) -> dict:
        """Get a video's metadata by its ID.
        
        Args:
            video_id: The ID of the video
            session: Database session
            
        Returns:
            The video's metadata dictionary
            
        Raises:
            ValueError: If video not found
        """
        video = session.get(Video, video_id)
        if not video:
            raise ValueError(f"Video with ID {video_id} not found")
        return video.video_metadata

    @staticmethod
    def archive_video(video_id: int, session: Session) -> None:
        """Archive a video by its ID.
        
        Args:
            video_id: The ID of the video to archive
            session: Database session
            
        Raises:
            ValueError: If video not found
        """
        video = session.get(Video, video_id)
        if not video:
            raise ValueError(f"Video with ID {video_id} not found")
        video.is_archived = True
        session.commit()

    @staticmethod
    def get_all_videos(session: Session) -> pd.DataFrame:
        """Get all videos.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing videos with columns:
            - Video UID: Unique identifier for the video
            - URL: Video URL
            - Archived: Whether the video is archived
        """
        videos = session.scalars(select(Video)).all()
        
        return pd.DataFrame([
            {
                "Video UID": v.video_uid,
                "URL": v.url,
                "Archived": v.is_archived
            }
            for v in videos
        ])

    @staticmethod
    def get_videos_with_project_status(session: Session) -> pd.DataFrame:
        """Get all videos with their project assignments and ground truth status.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing videos with columns:
            - Video UID: Unique identifier for the video
            - URL: Video URL
            - Projects: Comma-separated list of project names and their ground truth status
        """
        rows = []
        for v in session.scalars(select(Video).where(Video.is_archived == False)).all():
            # Get all non-archived projects this video belongs to
            projects = session.scalars(
                select(Project)
                .join(ProjectVideo, Project.id == ProjectVideo.project_id)
                .where(
                    ProjectVideo.video_id == v.id,
                    Project.is_archived == False
                )
            ).all()
            
            # Skip videos that only belong to archived projects
            if not projects:
                continue
            
            # For each project, check if video has complete ground truth
            project_status = []
            for p in projects:
                # Get total questions in schema through question groups
                total_questions = session.scalar(
                    select(func.count())
                    .select_from(Question)
                    .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                    .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                    .where(SchemaQuestionGroup.schema_id == p.schema_id)
                )
                
                # Get ground truth answers for this video in this project
                gt_answers = session.scalar(
                    select(func.count())
                    .select_from(ReviewerGroundTruth)
                    .where(
                        ReviewerGroundTruth.video_id == v.id,
                        ReviewerGroundTruth.project_id == p.id
                    )
                )
                
                # Get any annotator answers for this video in this project
                any_answers = session.scalar(
                    select(func.count())
                    .select_from(AnnotatorAnswer)
                    .where(
                        AnnotatorAnswer.video_id == v.id,
                        AnnotatorAnswer.project_id == p.id
                    )
                )
                
                # Determine status based on answers
                if total_questions == 0:
                    status = "No questions"
                elif gt_answers == total_questions:
                    status = "✓"
                else:
                    status = "✗"
                project_status.append(f"{p.name}: {status}")
            
            rows.append({
                "Video UID": v.video_uid,
                "URL": v.url,
                "Projects": ", ".join(project_status) if project_status else "No projects",
            })
        return pd.DataFrame(rows)

    @staticmethod
    def add_video(url: str, session: Session, metadata: dict = None) -> None:
        """Add a new video to the database.
        
        Args:
            url: The URL of the video
            session: Database session
            metadata: Optional dictionary containing video metadata
            
        Raises:
            ValueError: If URL is invalid, video already exists, or metadata is invalid
        """
        if not url.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        
        # Extract filename and check for extension
        filename = url.split("/")[-1]
        if not filename or "." not in filename:
            raise ValueError("URL must end with a filename with extension")
        
        if len(filename) > 255:
            raise ValueError("Video UID is too long")
        
        # Validate metadata type - must be None or a dictionary
        if metadata is not None:
            if not isinstance(metadata, dict):
                raise ValueError("Metadata must be a dictionary")
            if not metadata:
                raise ValueError("Metadata must be a non-empty dictionary")
            
            # Validate metadata value types if metadata is provided
            for key, value in metadata.items():
                if not isinstance(value, (str, int, float, bool, list, dict)):
                    raise ValueError(f"Invalid metadata value type for key '{key}': {type(value)}")
                if isinstance(value, list):
                    # Validate list elements
                    for item in value:
                        if not isinstance(item, (str, int, float, bool, dict)):
                            raise ValueError(f"Invalid list element type in metadata key '{key}': {type(item)}")
                elif isinstance(value, dict):
                    # Validate nested dictionary values
                    for k, v in value.items():
                        if not isinstance(v, (str, int, float, bool, list, dict)):
                            raise ValueError(f"Invalid nested metadata value type for key '{key}.{k}': {type(v)}")
        
        # Check if video already exists (case-sensitive check)
        existing = session.scalar(
            select(Video).where(Video.video_uid == filename)
        )
        if existing:
            raise ValueError(f"Video with UID '{filename}' already exists")
        
        # Create video
        video = Video(
            video_uid=filename,
            url=url,
            video_metadata=metadata or {}
        )
        session.add(video)
        session.commit()

class ProjectService:
    @staticmethod
    def get_project_by_name(name: str, session: Session) -> Optional[Project]:
        """Get a project by its name.
        
        Args:
            name: The name of the project
            session: Database session
            
        Returns:
            Project object if found, raises ValueError otherwise
        """
        project = session.scalar(select(Project).where(Project.name == name))
        if not project:
            raise ValueError(f"Project with name '{name}' not found")
        return project

    @staticmethod
    def get_all_projects(session: Session) -> pd.DataFrame:
        """Get all non-archived projects with their video counts and ground truth percentages."""
        rows = []
        for p in session.scalars(select(Project).where(Project.is_archived == False)).all():
            # Get schema
            schema = session.get(Schema, p.schema_id)
            if schema.is_archived:
                continue
            
            # Count videos in project
            video_count = session.scalar(
                select(func.count())
                .select_from(ProjectVideo)
                .where(ProjectVideo.project_id == p.id)
            )
            
            # Get total questions in schema
            total_questions = session.scalar(
                select(func.count())
                .select_from(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == p.schema_id)
            )
            
            # Get ground truth answers
            gt_answers = session.scalar(
                select(func.count())
                .select_from(ReviewerGroundTruth)
                .where(
                    ReviewerGroundTruth.project_id == p.id
                )
            )
            
            # Calculate percentage
            gt_percentage = (gt_answers / total_questions * 100) if total_questions > 0 else 0.0
            
            rows.append({
                "ID": p.id,
                "Name": p.name,
                "Videos": video_count,
                "Schema ID": p.schema_id,
                "GT %": gt_percentage
            })
        return pd.DataFrame(rows)

    @staticmethod
    def create_project(name: str, schema_id: int, video_ids: List[int], session: Session) -> None:
        """Create a new project and assign all admin users to it.
        
        Args:
            name: Project name
            schema_id: ID of the schema to use
            video_ids: List of video IDs to include in the project
            session: Database session
            
        Raises:
            ValueError: If schema or any video is archived
        """
        # Check if schema exists and is not archived
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        if schema.is_archived:
            raise ValueError(f"Schema with ID {schema_id} is archived")
        
        # Check if project name already exists
        existing_project = session.scalar(select(Project).where(Project.name == name))
        if existing_project:
            raise ValueError(f"Project with name '{name}' already exists")
        
        # Create project
        project = Project(name=name, schema_id=schema_id)
        session.add(project)
        session.flush()  # Get the project ID
        
        # Add videos to project
        for vid in video_ids:
            # Check if video exists and is not archived
            video = session.get(Video, vid)
            if not video:
                raise ValueError(f"Video with ID {vid} not found")
            if video.is_archived:
                raise ValueError(f"Video with ID {vid} is archived")
            
            session.add(ProjectVideo(project_id=project.id, video_id=vid))
        
        # Assign all admin users to the project
        admin_users = session.scalars(
            select(User).where(User.user_type == "admin", User.is_archived == False)
        ).all()
        
        for admin in admin_users:
            ProjectService.add_user_to_project(admin.id, project.id, "admin", session)
        
        session.commit()

    @staticmethod
    def get_video_ids_by_uids(video_uids: List[str], session: Session) -> List[int]:
        """Get video IDs from their UIDs.
        
        Args:
            video_uids: List of video UIDs
            session: Database session
            
        Returns:
            List of video IDs
        """
        return session.scalars(select(Video.id).where(Video.video_uid.in_(video_uids))).all()

    @staticmethod
    def archive_project(project_id: int, session: Session) -> None:
        """Archive a project and block new answers.
        
        Args:
            project_id: The ID of the project to archive
            session: Database session
            
        Raises:
            ValueError: If project not found
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        project.is_archived = True
        session.commit()

    @staticmethod
    def progress(project_id: int, session: Session) -> dict:
        """Get project progress statistics.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Dictionary containing:
            - total_videos: Number of videos in project
            - total_questions: Number of questions in schema
            - total_answers: Total number of annotator answers submitted
            - ground_truth_answers: Number of ground truth answers
            - completion_percentage: Percentage of questions with ground truth answers
            
        Raises:
            ValueError: If project not found
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get total videos in project
        total_videos = session.scalar(
            select(func.count())
            .select_from(ProjectVideo)
            .where(ProjectVideo.project_id == project_id)
        )
        
        # Get total questions in schema through question groups
        total_questions = session.scalar(
            select(func.count())
            .select_from(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == project.schema_id)
        )
        
        # Get total annotator answers
        total_answers = session.scalar(
            select(func.count())
            .select_from(AnnotatorAnswer)
            .where(AnnotatorAnswer.project_id == project_id)
        )
        
        # Get ground truth answers
        ground_truth_answers = session.scalar(
            select(func.count())
            .select_from(ReviewerGroundTruth)
            .where(ReviewerGroundTruth.project_id == project_id)
        )
        
        # Calculate completion percentage
        total_possible_answers = total_videos * total_questions
        completion_percentage = round(
            (ground_truth_answers / total_possible_answers * 100) if total_possible_answers > 0 else 0,
            2
        )
        
        return {
            "total_videos": total_videos,
            "total_questions": total_questions,
            "total_answers": total_answers,
            "ground_truth_answers": ground_truth_answers,
            "completion_percentage": completion_percentage
        }

    @staticmethod
    def get_project_by_id(project_id: int, session: Session) -> Optional[Project]:
        """Get a project by its ID.
        
        Args:
            project_id: The ID of the project
            session: Database session
        
        Returns:
            Project object if found, raises ValueError otherwise
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        return project

    @staticmethod
    def add_user_to_project(project_id: int, user_id: int, role: str, session: Session) -> None:
        """Add a user to a project with the specified role.
        
        Args:
            project_id: The ID of the project
            user_id: The ID of the user
            role: The role to assign ('annotator', 'reviewer', 'admin', or 'model')
            session: Database session
            
        Raises:
            ValueError: If project or user not found, or if role is invalid
        """
        # Validate project and user
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        if project.is_archived:
            raise ValueError(f"Project with ID {project_id} is archived")
        
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if user.is_archived:
            raise ValueError(f"User with ID {user_id} is archived")
            
        # Validate role
        if role not in ["annotator", "reviewer", "admin", "model"]:
            raise ValueError(f"Invalid role: {role}")
            
        # For admin role, verify user is a global admin
        if role == "admin" and user.user_type != "admin":
            raise ValueError(f"User {user_id} must be a global admin to be assigned admin role")
        
        if user.user_type == "admin" and role != "admin":
            raise ValueError(f"User {user_id} must not be a global admin to be assigned a non-admin role")
        
        # For model role, can only be assigned to model users
        if role == "model" and user.user_type != "model":
            raise ValueError(f"User {user_id} must be a model to be assigned model role")
        
        if user.user_type == "model" and role != "model":
            raise ValueError(f"User {user_id} must not be a model to be assigned a non-model role")
            
        # Archive any existing roles for this user in this project
        session.execute(
            update(ProjectUserRole)
            .where(
                ProjectUserRole.project_id == project_id,
                ProjectUserRole.user_id == user_id
            )
            .values(is_archived=True)
        )
        
        def ensure_role(role_type: str) -> None:
            """Helper function to ensure a role exists and is active."""
            existing = session.scalar(
                select(ProjectUserRole).where(
                    ProjectUserRole.project_id == project_id,
                    ProjectUserRole.user_id == user_id,
                    ProjectUserRole.role == role_type
                )
            )
            if existing:
                existing.is_archived = False
            else:
                session.add(ProjectUserRole(
                    project_id=project_id,
                    user_id=user_id,
                    role=role_type
                ))
        
        # Add roles based on the requested role
        if role == "annotator":
            ensure_role("annotator")
        elif role == "reviewer":
            # Reviewers get both annotator and reviewer roles
            ensure_role("annotator")
            ensure_role("reviewer")
        elif role == "model":
            ensure_role("model")
        elif role == "admin":
            # Admins get all three roles
            ensure_role("annotator")
            ensure_role("reviewer")
            ensure_role("admin")
        else:
            raise ValueError(f"Invalid role: {role}")
            
        session.commit()

class SchemaService:
    @staticmethod
    def get_all_schemas(session: Session) -> pd.DataFrame:
        """Get all schemas with their question groups.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing schemas with columns:
            - ID: Schema ID
            - Name: Schema name
            - Rules: Schema rules JSON
            - Question Groups: List of question groups in schema
        """
        schemas = session.scalars(select(Schema)).all()
        rows = []
        for s in schemas:
            # Get question groups for this schema
            groups = session.scalars(
                select(QuestionGroup)
                .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == s.id)
            ).all()
            
            rows.append({
                "ID": s.id,
                "Name": s.name,
                "Question Groups": ", ".join(g.title for g in groups) if groups else "No groups"
            })
        return pd.DataFrame(rows)

    @staticmethod
    def get_schema_questions(schema_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a schema through its question groups.
        
        Args:
            schema_id: The ID of the schema
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Group: Question group name
            - Type: Question type
            - Options: Available options for single-choice questions
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        # Get questions through question groups
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == schema_id)
        ).all()
        
        return pd.DataFrame([
            {
                "ID": q.id,
                "Text": q.text,
                "Group": session.scalar(
                    select(QuestionGroup.title)
                    .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                    .where(QuestionGroupQuestion.question_id == q.id)
                ),
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else ""
            }
            for q in questions
        ])

    @staticmethod
    def get_schema_id_by_name(name: str, session: Session) -> int:
        """Get schema ID by name.
        
        Args:
            name: Schema name
            session: Database session
            
        Returns:
            Schema ID
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.scalar(select(Schema).where(Schema.name == name))
        if not schema:
            raise ValueError(f"Schema '{name}' not found")
        return schema.id

    @staticmethod
    def create_schema(name: str, question_group_ids: List[int], session: Session) -> Schema:
        """Create a new schema with its question groups.
        
        Args:
            name: Schema name
            question_group_ids: List of question group IDs in desired order
            session: Database session
            
        Returns:
            Created schema
            
        Raises:
            ValueError: If schema with same name exists or validation fails
        """
        # Check if schema with same name exists
        existing = session.scalar(select(Schema).where(Schema.name == name))
        if existing:
            raise ValueError(f"Schema with name '{name}' already exists")
            
        # Create schema
        schema = Schema(name=name)
        session.add(schema)
        session.flush()  # Get schema ID
        
        # Add question groups
        for i, group_id in enumerate(question_group_ids):
            # Check if group exists
            group = session.get(QuestionGroup, group_id)
            if not group:
                raise ValueError(f"Question group with ID {group_id} not found")
            if group.is_archived:
                raise ValueError(f"Question group with ID {group_id} is archived")
                
            # Check if non-reusable group is already used in another schema
            if not group.is_reusable:
                existing_schema = session.scalar(
                    select(Schema)
                    .join(SchemaQuestionGroup, Schema.id == SchemaQuestionGroup.schema_id)
                    .where(SchemaQuestionGroup.question_group_id == group_id)
                )
                if existing_schema:
                    raise ValueError(f"Question group {group.title} is not reusable and is already used in schema {existing_schema.name}")
            
            # Add group to schema
            sqg = SchemaQuestionGroup(
                schema_id=schema.id,
                question_group_id=group_id,
                display_order=i
            )
            session.add(sqg)
            
        session.commit()
        return schema

    @staticmethod
    def archive_schema(schema_id: int, session: Session) -> None:
        """Archive a schema and prevent its use in new projects.
        
        Args:
            schema_id: The ID of the schema to archive
            session: Database session
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        schema.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_schema(schema_id: int, session: Session) -> None:
        """Unarchive a schema to allow its use in new projects.
        
        Args:
            schema_id: The ID of the schema to unarchive
            session: Database session
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        schema.is_archived = False
        session.commit()

    @staticmethod
    def get_question_group_order(schema_id: int, session: Session) -> List[int]:
        """Get the ordered list of question group IDs in a schema.
        
        Args:
            schema_id: Schema ID
            session: Database session
            
        Returns:
            List of question group IDs in display order
            
        Raises:
            ValueError: If schema not found
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        # Get all question groups in schema ordered by display_order
        assignments = session.scalars(
            select(SchemaQuestionGroup)
            .where(SchemaQuestionGroup.schema_id == schema_id)
            .order_by(SchemaQuestionGroup.display_order)
        ).all()
        
        return [a.question_group_id for a in assignments]

    @staticmethod
    def update_question_group_order(schema_id: int, group_ids: List[int], session: Session) -> None:
        """Update the order of question groups in a schema.
        
        Args:
            schema_id: Schema ID
            group_ids: List of question group IDs in desired order
            session: Database session
            
        Raises:
            ValueError: If schema not found, or if any group not in schema
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        # Get all current assignments
        assignments = session.scalars(
            select(SchemaQuestionGroup)
            .where(SchemaQuestionGroup.schema_id == schema_id)
        ).all()
        
        # Create lookup for assignments
        assignment_map = {a.question_group_id: a for a in assignments}
        
        # Validate all groups exist in schema
        for group_id in group_ids:
            if group_id not in assignment_map:
                raise ValueError(f"Question group {group_id} not in schema {schema_id}")

        current_ids = set(assignment_map.keys())
        new_ids = set(group_ids)
        if current_ids != new_ids:
            missing = current_ids - new_ids
            extra = new_ids - current_ids
            error_msg = f"New group_ids must be a permutation of the current group IDs in schema {schema_id}"
            if missing:
                error_msg += f". Missing groups: {list(missing)}"
            if extra:
                error_msg += f". Extra groups: {list(extra)}"
            raise ValueError(error_msg)
        
        # Update orders based on list position
        for i, group_id in enumerate(group_ids):
            assignment_map[group_id].display_order = i
            
        session.commit()

    @staticmethod
    def get_schema_by_name(name: str, session: Session) -> Schema:
        """Get a schema by its name.
        
        Args:
            name: Schema name
            session: Database session
            
        Returns:
            Schema object
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.scalar(select(Schema).where(Schema.name == name))
        if not schema:
            raise ValueError("Schema not found")
        return schema

    @staticmethod
    def get_schema_by_id(schema_id: int, session: Session) -> Schema:
        """Get a schema by its ID.
        
        Args:
            schema_id: Schema ID
            session: Database session
            
        Returns:
            Schema object
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        return schema

    @staticmethod
    def get_schema_question_groups(schema_id: int, session: Session) -> pd.DataFrame:
        """Get all question groups in a schema.
        
        Args:
            schema_id: The ID of the schema
            session: Database session
            
        Returns:
            DataFrame containing question groups with columns:
            - ID: Question group ID
            - Title: Question group title
            - Description: Question group description
            - Reusable: Whether the group is reusable
            - Archived: Whether the group is archived
            - Display Order: Order in which the group appears in the schema
            - Question Count: Number of questions in the group
            
        Raises:
            ValueError: If schema not found
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
            
        # Get all question groups in schema ordered by display_order
        groups = session.scalars(
            select(QuestionGroup)
            .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == schema_id)
            .order_by(SchemaQuestionGroup.display_order)
        ).all()
        
        # Get question counts for each group
        rows = []
        for group in groups:
            # Count questions in this group
            question_count = session.scalar(
                select(func.count())
                .select_from(QuestionGroupQuestion)
                .where(QuestionGroupQuestion.question_group_id == group.id)
            )
            
            # Get display order
            display_order = session.scalar(
                select(SchemaQuestionGroup.display_order)
                .where(
                    SchemaQuestionGroup.schema_id == schema_id,
                    SchemaQuestionGroup.question_group_id == group.id
                )
            )
            
            rows.append({
                "ID": group.id,
                "Title": group.title,
                "Description": group.description,
                "Reusable": group.is_reusable,
                "Archived": group.is_archived,
                "Display Order": display_order,
                "Question Count": question_count
            })
            
        return pd.DataFrame(rows)

class QuestionService:
    @staticmethod
    def get_all_questions(session: Session) -> pd.DataFrame:
        """Get all questions with their group information.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Type: Question type
            - Group: Question group name
            - Options: Available options for single-choice questions
            - Default: Default option for single-choice questions
            - Archived: Whether the question is archived
        """
        qs = session.scalars(select(Question)).all()
        return pd.DataFrame([
            {
                "ID": q.id, 
                "Text": q.text, 
                "Type": q.type,
                "Group": session.scalar(
                    select(QuestionGroup.title)
                    .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                    .where(QuestionGroupQuestion.question_id == q.id)
                ),
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            } for q in qs
        ])

    @staticmethod
    def add_question(text: str, qtype: str, options: Optional[List[str]], default: Optional[str], 
                    session: Session, display_values: Optional[List[str]] = None) -> Question:
        """Add a new question.
        
        Args:
            text: Question text
            qtype: Question type ('single' or 'description')
            options: List of options for single-choice questions
            default: Default option for single-choice questions
            session: Database session
            display_values: Optional list of display text for options. For single-type questions, if not provided, uses options as display values.
            
        Returns:
            Created question
            
        Raises:
            ValueError: If question text already exists or validation fails
        """
        # Check if question text already exists
        existing = session.scalar(select(Question).where(Question.text == text))
        if existing:
            raise ValueError(f"Question with text '{text}' already exists")
            
        # Validate default option for single-choice questions
        if qtype == "single":
            if not options:
                raise ValueError("Single-choice questions must have options")
            if default and default not in options:
                raise ValueError(f"Default option '{default}' must be one of the available options: {', '.join(options)}")
            
            # For single-type questions, display_values must be provided or default to options
            if display_values:
                if len(display_values) != len(options):
                    raise ValueError("Number of display values must match number of options")
            else:
                display_values = options  # Use options as display values if not provided
        else:
            # For description-type questions, display_values should be None
            display_values = None

        # Create question
        q = Question(
            text=text, 
            type=qtype, 
            options=options, 
            display_values=display_values,
            default_option=default
        )
        session.add(q)
        session.commit()
        return q

    @staticmethod
    def get_question_by_text(text: str, session: Session) -> Question:
        """Get a question by its text.
        
        Args:
            text: Question text
            session: Database session
            
        Returns:
            Question object if found
            
        Raises:
            ValueError: If question not found
        """
        question = session.scalar(select(Question).where(Question.text == text))
        if not question:
            raise ValueError(f"Question with text '{text}' not found")
        return question

    @staticmethod
    def edit_question(question_id: int, new_text: str, new_opts: Optional[List[str]], new_default: Optional[str],
                     session: Session, new_display_values: Optional[List[str]] = None) -> None:
        """Edit an existing question.
        
        Args:
            question_id: Current question ID
            new_text: New question text
            new_opts: New options for single-choice questions. Must include all existing options.
            new_default: New default option for single-choice questions
            session: Database session
            new_display_values: Optional new display values for options. For single-type questions, if not provided, maintains existing display values or uses options.
            
        Raises:
            ValueError: If question not found or validation fails
        """
        # Get question
        q = QuestionService.get_question_by_id(question_id, session)

        # Check if question is archived
        if q.is_archived:
            raise ValueError(f"Question with ID {question_id} is archived")
        
        # Check if new text would conflict
        if new_text != q.text:
            existing = session.scalar(select(Question).where(Question.text == new_text))
            if existing:
                raise ValueError(f"Question with text '{new_text}' already exists")
        
        # For single-choice questions, validate options and display values
        if q.type == "single":
            if not new_opts:
                raise ValueError("Cannot change question type")
            if new_default and new_default not in new_opts:
                raise ValueError(f"Default option '{new_default}' must be one of the available options: {', '.join(new_opts)}")
            
            # Validate that all existing options are included in new options
            missing_opts = set(q.options) - set(new_opts)
            if missing_opts:
                raise ValueError(f"Cannot remove existing options: {', '.join(missing_opts)}")
            
            # For single-type questions, ensure we have display values
            if new_display_values:
                if len(new_display_values) != len(new_opts):
                    raise ValueError("Number of display values must match number of options")
            else:
                # If no new display values provided, maintain existing mapping for unchanged options
                new_display_values = []
                for opt in new_opts:
                    if opt in q.options:
                        idx = q.options.index(opt)
                        new_display_values.append(q.display_values[idx])
                    else:
                        new_display_values.append(opt)
        else:  # description type
            if new_opts is not None or new_default is not None or new_display_values is not None:
                raise ValueError("Cannot change question type")
                
        # Update question
        q.text = new_text
        q.options = new_opts
        q.display_values = new_display_values
        q.default_option = new_default
        session.commit()

    @staticmethod
    def archive_question(question_id: int, session: Session) -> None:
        """Archive a question.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Raises:
            ValueError: If question not found
        """
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_question(question_id: int, session: Session) -> None:
        """Unarchive a question.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Raises:
            ValueError: If question not found
        """
        q = session.get(Question, question_id)
        if not q:
            raise ValueError(f"Question with ID {question_id} not found")
        q.is_archived = False
        session.commit()

    @staticmethod
    def get_question_by_id(question_id: int, session: Session) -> Question:
        """Get a question by its ID.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Returns:
            Question object if found
            
        Raises:
            ValueError: If question not found
        """
        question = session.get(Question, question_id)
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
        return question

class AuthService:
    @staticmethod
    def get_user_by_id(user_id: str, session: Session) -> Optional[User]:
        """Get a user by their ID string.
        
        Args:
            user_id: The ID string of the user
            session: Database session
            
        Returns:
            User object if found, raises ValueError otherwise
        """
        user = session.scalar(select(User).where(User.user_id_str == user_id))
        if not user:
            raise ValueError(f"User with ID '{user_id}' not found")
        return user
    
    @staticmethod
    def get_user_by_name(user_name: str, session: Session) -> Optional[User]:
        """Get a user by their name.
        
        Args:
            user_name: The name of the user
            session: Database session
            
        Returns:
            User object if found, raises ValueError otherwise
        """
        user = session.scalar(select(User).where(User.user_id_str == user_name))
        if not user:
            raise ValueError(f"User with name '{user_name}' not found")
        return user

    @staticmethod
    def get_user_by_email(email: str, session: Session) -> Optional[User]:
        """Get a user by their email.
        
        Args:
            email: The email of the user
            session: Database session
            
        Returns:
            User object if found, raises ValueError otherwise
            
        Raises:
            ValueError: If user not found
        """
        user = session.scalar(select(User).where(User.email == email))
        if not user:
            raise ValueError(f"User with email '{email}' not found")
        return user

    @staticmethod
    def authenticate(email: str, pwd: str, role: str, session: Session) -> Optional[dict]:
        """Authenticate a user.
        
        Args:
            email: User's email
            pwd: User's password
            role: Required role
            session: Database session
            
        Returns:
            Dictionary containing user info if authenticated, None otherwise
        """
        u = session.scalar(select(User).where(
            User.email == email, 
            User.password_hash == pwd, 
            User.is_archived == False
        ))
        if not u:
            return None
        if role != "admin" and u.user_type != role:
            return None
        return {"id": u.id, "name": u.user_id_str, "role": u.user_type}

    @staticmethod
    def seed_admin(session: Session) -> None:
        """Create hard‑coded admin if not present."""
        if not session.scalar(select(User).where(User.email == "zhiqiulin98@gmail.com")):
            session.add(User(
                user_id_str="admin", 
                email="zhiqiulin98@gmail.com",
                password_hash="zhiqiulin98", 
                user_type="admin", 
                is_archived=False
            ))
            session.commit()

    @staticmethod
    def get_all_users(session: Session) -> pd.DataFrame:
        """Get all users in a DataFrame format."""
        users = session.scalars(select(User)).all()
        return pd.DataFrame([
            {
                "ID": u.id,
                "User ID": u.user_id_str,
                "Email": u.email,
                "Password Hash": u.password_hash,
                "Role": u.user_type,
                "Archived": u.is_archived,
                "Created At": u.created_at
            } for u in users
        ])

    @staticmethod
    def get_users_by_type(user_type: str, session: Session) -> List[User]:
        """Get all users of a specific type.
        
        Args:
            user_type: The type of users to get ('human', 'model', or 'admin')
            session: Database session
            
        Returns:
            List of User objects of the specified type
            
        Raises:
            ValueError: If user_type is invalid
        """
        if user_type not in ["human", "model", "admin"]:
            raise ValueError(f"Invalid user type: {user_type}")
            
        return session.scalars(
            select(User).where(User.user_type == user_type)
        ).all()

    @staticmethod
    def update_user_id(user_id: int, new_user_id: str, session: Session) -> None:
        """Update a user's ID."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Check if new user ID already exists
        existing = session.scalar(
            select(User).where(User.user_id_str == new_user_id)
        )
        if existing and existing.id != user_id:
            raise ValueError(f"User ID '{new_user_id}' already exists")
        
        user.user_id_str = new_user_id
        session.commit()

    @staticmethod
    def update_user_email(user_id: int, new_email: str, session: Session) -> None:
        """Update a user's email."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Model users cannot have emails
        if user.user_type == "model":
            raise ValueError("Model users cannot have emails")
        
        # Email is required for human and admin users
        if not new_email:
            raise ValueError("Email is required for human and admin users")
        
        # Check if new email already exists
        existing = session.scalar(
            select(User).where(User.email == new_email)
        )
        if existing and existing.id != user_id:
            raise ValueError(f"Email '{new_email}' already exists")
        
        user.email = new_email
        session.commit()

    @staticmethod
    def update_user_password(user_id: int, new_password: str, session: Session) -> None:
        """Update a user's password."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        user.password_hash = new_password  # Note: In production, this should be hashed
        session.commit()

    @staticmethod
    def update_user_role(user_id: int, new_role: str, session: Session) -> None:
        """Update a user's role and handle admin project assignments."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if new_role not in ["human", "model", "admin"]:
            raise ValueError(f"Invalid role: {new_role}")

        # Cannot change from human/admin to model
        if user.user_type == "human" or user.user_type == "admin":
            if new_role == "model":
                raise ValueError("Cannot change from human/admin to model")
        
        # Cannot change from model to human/admin
        if user.user_type == "model":
            if new_role == "human" or new_role == "admin":
                raise ValueError("Cannot change from model to human/admin")
        
        # If changing from admin to human, remove all project roles
        if user.user_type == "admin" and new_role == "human":
            # Get all non-archived project assignments for this user
            assignments = session.scalars(
                select(ProjectUserRole)
                .where(
                    ProjectUserRole.user_id == user_id,
                    ProjectUserRole.is_archived == False
                )
            ).all()
            
            # Archive each assignment
            for assignment in assignments:
                assignment.is_archived = True
        
        # If changing to admin role, assign to all projects
        if new_role == "admin" and user.user_type != "admin":
            user.user_type = new_role
            session.commit()
            # Get all non-archived projects
            projects = session.scalars(
                select(Project).where(Project.is_archived == False)
            ).all()
            
            # Assign user as admin to each project
            for project in projects:
                ProjectService.add_user_to_project(user_id, project.id, "admin", session)
        else:
            user.user_type = new_role
            session.commit()

    @staticmethod
    def toggle_user_archived(user_id: int, session: Session) -> None:
        """Toggle a user's archived status."""
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        user.is_archived = not user.is_archived
        session.commit()

    @staticmethod
    def get_project_assignments(session: Session) -> pd.DataFrame:
        """Get all project assignments in a DataFrame format."""
        assignments = session.scalars(
            select(ProjectUserRole)
            .join(Project, ProjectUserRole.project_id == Project.id)
            .join(User, ProjectUserRole.user_id == User.id)
            .where(ProjectUserRole.is_archived == False)
        ).all()
        
        return pd.DataFrame([
            {
                "Project ID": a.project_id,
                "Project Name": session.get(Project, a.project_id).name,
                "User ID": a.user_id,
                "User Name": session.get(User, a.user_id).user_id_str,
                "Role": a.role,
                "Archived": a.is_archived,
                "Assigned At": a.assigned_at,
                "Completed At": a.completed_at
            }
            for a in assignments
        ])

    @staticmethod
    def create_user(user_id: str, email: str, password_hash: str, user_type: str, session: Session, is_archived: bool = False) -> User:
        """Create a new user with validation."""
        if user_type not in ["human", "model", "admin"]:
            raise ValueError("Invalid user type. Must be one of: human, model, admin")
        
        # For model users, email should be None
        if user_type == "model":
            if email:
                raise ValueError("Model users cannot have emails")
        elif not email:
            raise ValueError("Email is required for human and admin users")
        
        # Check if user already exists
        existing_user = session.scalar(
            select(User).where(
                (User.user_id_str == user_id) | 
                (User.email == email)
            )
        )
        if existing_user:
            raise ValueError(f"User with ID '{user_id}' or email '{email}' already exists")
        
        user = User(
            user_id_str=user_id,
            email=email,
            password_hash=password_hash,
            user_type=user_type,
            is_archived=is_archived
        )
        session.add(user)
        session.flush()  # Get user ID
        
        # If user is admin, assign to all existing projects
        if user_type == "admin" and not is_archived:
            projects = session.scalars(
                select(Project).where(Project.is_archived == False)
            ).all()
            
            for project in projects:
                ProjectService.add_user_to_project(user.id, project.id, "admin", session)
        
        session.commit()
        return user

    @staticmethod
    def assign_user_to_project(user_id: int, project_id: int, role: str, session: Session) -> None:
        """Assign a user to a project with role validation and admin privileges."""
        if role not in ["annotator", "reviewer", "admin", "model"]:
            raise ValueError("Invalid role. Must be one of: annotator, reviewer, admin, model")
        
        # Get user and project
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if user.is_archived:
            raise ValueError(f"User with ID {user_id} is archived")
        
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        if project.is_archived:
            raise ValueError(f"Project with ID {project_id} is archived")
        
        # If user is an admin, they automatically get reviewer role
        if user.user_type == "admin" and role != "admin":
            role = "reviewer"
        
        # Check if assignment already exists (including archived ones)
        existing = session.scalar(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id
            )
        )
        
        if existing:
            existing.role = role
            existing.is_archived = False  # Unarchive if it was archived
        else:
            assignment = ProjectUserRole(
                project_id=project_id,
                user_id=user_id,
                role=role,
                is_archived=False
            )
            session.add(assignment)
        
        session.commit()

    @staticmethod
    def remove_user_from_project(user_id: int, project_id: int, session: Session) -> None:
        """Remove a user's assignment from a project."""
        assignment = session.scalar(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id
            )
        )
        
        if not assignment:
            raise ValueError(f"No assignment found for user {user_id} in project {project_id}")
        
        # Instead of deleting, mark as archived
        assignment.is_archived = True
        session.commit()

    @staticmethod
    def bulk_assign_users_to_project(user_ids: List[int], project_id: int, role: str, session: Session) -> None:
        """Assign multiple users to a project with the same role."""
        for user_id in user_ids:
            try:
                AuthService.assign_user_to_project(user_id, project_id, role, session)
            except ValueError as e:
                # Log error but continue with other assignments
                print(f"Error assigning user {user_id}: {str(e)}")

    @staticmethod
    def bulk_remove_users_from_project(user_ids: List[int], project_id: int, session: Session) -> None:
        """Remove multiple users from a project."""
        # Instead of deleting, mark as archived
        session.execute(
            update(ProjectUserRole)
            .where(
                ProjectUserRole.user_id.in_(user_ids),
                ProjectUserRole.project_id == project_id
            )
            .values(is_archived=True)
        )
        session.commit()

    @staticmethod
    def archive_user_from_project(user_id: int, project_id: int, session: Session) -> None:
        """Archive a user's assignment from a project.
        
        Args:
            user_id: The ID of the user
            project_id: The ID of the project
            session: Database session
            
        Raises:
            ValueError: If no assignments found
        """
        # Get all role assignments for this user in this project
        assignments = session.scalars(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id,
                ProjectUserRole.is_archived == False
            )
        ).all()
        
        if not assignments:
            return
        
        # Archive all role assignments
        for assignment in assignments:
            assignment.is_archived = True
        
        session.commit()

class QuestionGroupService:
    @staticmethod
    def get_all_groups(session: Session) -> pd.DataFrame:
        """Get all question groups with their questions and schema usage.
        
        Args:
            session: Database session
            
        Returns:
            DataFrame containing groups with columns:
            - ID: Group ID
            - Name: Group name
            - Description: Group description
            - Questions: List of questions in the group
            - Reusable: Whether the group is reusable
            - Archived: Whether the group is archived
            - Question Count: Number of questions
            - Archived Questions: Number of archived questions
            - Used in Schemas: List of schemas using this group
        """
        groups = session.scalars(select(QuestionGroup)).all()
        rows = []
        for g in groups:
            # Get all questions in this group
            questions = session.scalars(
                select(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == g.id)
            ).all()
            
            # Get all schemas using this group
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestionGroup, Schema.id == SchemaQuestionGroup.schema_id)
                .where(SchemaQuestionGroup.question_group_id == g.id)
                .distinct()
            ).all()
            
            # Format questions as a list of strings
            question_list = []
            for q in questions:
                q_str = f"- {q.text} ({q.type})"
                if q.type == "single" and q.options:
                    q_str += f" [Options: {', '.join(q.options)}"
                    if q.default_option:
                        q_str += f", Default: {q.default_option}"
                    q_str += "]"
                if q.is_archived:
                    q_str += " [ARCHIVED]"
                question_list.append(q_str)
            
            rows.append({
                "ID": g.id,
                "Name": g.title,
                "Description": g.description,
                "Questions": "\n".join(question_list) if question_list else "No questions",
                "Reusable": g.is_reusable,
                "Archived": g.is_archived,
                "Question Count": len(questions),
                "Archived Questions": sum(1 for q in questions if q.is_archived),
                "Used in Schemas": ", ".join(s.name for s in schemas) if schemas else "None"
            })
        return pd.DataFrame(rows)

    @staticmethod
    def get_group_questions(group_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text
            - Type: Question type
            - Options: Available options for single-choice questions
            - Default: Default option for single-choice questions
            - Archived: Whether the question is archived
        """
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == group_id)
        ).all()
        
        return pd.DataFrame([
            {
                "ID": q.id,
                "Text": q.text,
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            }
            for q in questions
        ])

    @staticmethod
    def get_group_details(group_id: int, session: Session) -> dict:
        """Get details of a question group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            Dictionary containing group details
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        return {
            "title": group.title,
            "description": group.description,
            "is_reusable": group.is_reusable,
            "is_archived": group.is_archived
        }

    @staticmethod
    def create_group(
        title: str,
        description: str,
        is_reusable: bool,
        question_ids: List[int],
        verification_function: Optional[str],
        session: Session
    ) -> QuestionGroup:
        """Create a new question group.
        
        Args:
            title: Group title
            description: Group description
            is_reusable: Whether group can be used in multiple schemas
            question_ids: List of question IDs in desired order
            verification_function: Optional name of verification function from verify.py
            session: Database session
            
        Returns:
            Created QuestionGroup
            
        Raises:
            ValueError: If title already exists or validation fails
        """
        # Validate title
        if not title or not title.strip():
            raise ValueError("Title is required")
            
        # Validate questions
        if not question_ids:
            raise ValueError("Question group must contain at least one question")
            
        # Check if title already exists
        existing = session.scalar(
            select(QuestionGroup).where(
                QuestionGroup.title == title
            )
        )
        if existing:
            raise ValueError(f"Question group with title '{title}' already exists")
        
        # Validate verification function if provided
        if verification_function:
            if not hasattr(verify, verification_function):
                raise ValueError(f"Verification function '{verification_function}' not found in verify.py")
                
        # Create group
        group = QuestionGroup(
            title=title,
            description=description,
            is_reusable=is_reusable,
            verification_function=verification_function
        )
        session.add(group)
        session.flush()  # Get the group ID
        
        # Validate and add questions
        for i, question_id in enumerate(question_ids):
            # Check if question exists and isn't archived
            question = session.scalar(select(Question).where(Question.id == question_id))
            if not question:
                raise ValueError(f"Question with ID {question_id} not found")
            if question.is_archived:
                raise ValueError(f"Question with ID {question_id} is archived")
                
            # Add question to group
            session.add(QuestionGroupQuestion(
                question_group_id=group.id,
                question_id=question_id,
                display_order=i
            ))
            
        session.commit()
        return group

    @staticmethod
    def get_group_by_name(name: str, session: Session) -> Optional[QuestionGroup]:
        """Get a question group by its name.
        
        Args:
            name: Group name
            session: Database session
            
        Returns:
            Question group if found, None otherwise
        """
        group = session.scalar(select(QuestionGroup).where(QuestionGroup.title == name))
        if not group:
            raise ValueError(f"Question group with title '{name}' not found")
        return group

    @staticmethod
    def get_group_by_id(group_id: int, session: Session) -> Optional[QuestionGroup]:
        """Get a question group by its ID.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            Question group if found, None otherwise
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        return group

    @staticmethod
    def edit_group(group_id: int, new_title: str, new_description: str, is_reusable: bool, session: Session) -> None:
        """Edit a question group.
        
        Args:
            group_id: Group ID
            new_title: New group title
            new_description: New group description
            is_reusable: Whether the group is reusable
            session: Database session
            
        Raises:
            ValueError: If group not found or validation fails
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        # If making a group non-reusable, check if it's used in multiple schemas
        if not is_reusable and group.is_reusable:
            schemas = session.scalars(
                select(Schema)
                .join(SchemaQuestionGroup, Schema.id == SchemaQuestionGroup.schema_id)
                .where(SchemaQuestionGroup.question_group_id == group_id)
                .distinct()
            ).all()
            
            if len(schemas) > 1:
                raise ValueError(
                    f"Cannot make group non-reusable as it is used in multiple schemas: "
                    f"{', '.join(s.name for s in schemas)}"
                )
        
        # Check if new title conflicts with existing group
        if new_title != group.title:
            existing = session.scalar(
                select(QuestionGroup).where(QuestionGroup.title == new_title)
            )
            if existing:
                raise ValueError(f"Question group with title '{new_title}' already exists")
        
        group.title = new_title
        group.description = new_description
        group.is_reusable = is_reusable
        session.commit()

    @staticmethod
    def archive_group(group_id: int, session: Session) -> None:
        """Archive a question group and its questions.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = True
        # Also archive all questions in this group
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == group_id)
        ).all()
        for q in questions:
            q.is_archived = True
        session.commit()

    @staticmethod
    def unarchive_group(group_id: int, session: Session) -> None:
        """Unarchive a question group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        group.is_archived = False
        session.commit()

    @staticmethod
    def get_question_order(group_id: int, session: Session) -> List[int]:
        """Get the ordered list of question IDs in a group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            List of question IDs in display order
            
        Raises:
            ValueError: If group not found
        """
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
            
        # Get all questions in group ordered by display_order
        assignments = session.scalars(
            select(QuestionGroupQuestion)
            .where(QuestionGroupQuestion.question_group_id == group_id)
            .order_by(QuestionGroupQuestion.display_order)
        ).all()
        
        return [a.question_id for a in assignments]

    @staticmethod
    def update_question_order(group_id: int, question_ids: List[int], session: Session) -> None:
        """Update the order of questions in a group.
        
        Args:
            group_id: Group ID
            question_ids: List of question IDs in desired order
            session: Database session
            
        Raises:
            ValueError: If group not found, or if any question not in group
        """
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
            
        # Get all current assignments
        assignments = session.scalars(
            select(QuestionGroupQuestion)
            .where(QuestionGroupQuestion.question_group_id == group_id)
        ).all()
        
        # Create lookup for assignments
        assignment_map = {a.question_id: a for a in assignments}
        
        # Validate all questions exist in group
        for question_id in question_ids:
            if question_id not in assignment_map:
                raise ValueError(f"Question {question_id} not in group {group_id}")
        
        # Update orders based on list position
        for i, question_id in enumerate(question_ids):
            assignment_map[question_id].display_order = i
            
        session.commit()

class BaseAnswerService:
    """Base class with shared functionality for answer submission services."""
    
    @staticmethod
    def _validate_project_and_user(project_id: int, user_id: int, session: Session) -> tuple[Project, User]:
        """Validate project and user exist and are active.
        
        Args:
            project_id: The ID of the project
            user_id: The ID of the user
            session: Database session
            
        Returns:
            Tuple of (Project, User) objects
            
        Raises:
            ValueError: If validation fails
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        if project.is_archived:
            raise ValueError("Project is archived")
            
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        if user.is_archived:
            raise ValueError("User is archived")
            
        return project, user

    @staticmethod
    def _validate_user_role(user_id: int, project_id: int, required_role: str, session: Session) -> None:
        """Validate user has required role in project.
        
        Args:
            user_id: The ID of the user
            project_id: The ID of the project
            required_role: Required role ('annotator', 'reviewer', or 'admin')
            session: Database session
            
        Raises:
            ValueError: If validation fails
        """
        # Get all non-archived roles for the user in this project
        user_roles = session.scalars(
            select(ProjectUserRole).where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id,
                ProjectUserRole.is_archived == False
            )
        ).all()
        
        # Define role hierarchy
        role_hierarchy = {
            'annotator': ['annotator', 'reviewer', 'admin'],
            'reviewer': ['reviewer', 'admin'],
            'admin': ['admin']
        }
        
        # Check if user has any role that satisfies the requirement
        if not user_roles or not any(role.role in role_hierarchy[required_role] for role in user_roles):
            raise ValueError(f"User {user_id} does not have {required_role} role in project {project_id}")

    @staticmethod
    def _validate_question_group(
        question_group_id: int,
        session: Session
    ) -> tuple[QuestionGroup, list[Question]]:
        """Validate question group and get its questions.
        
        Args:
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            Tuple of (QuestionGroup, list[Question])
            
        Raises:
            ValueError: If validation fails
        """
        group = session.get(QuestionGroup, question_group_id)
        if not group:
            raise ValueError(f"Question group with ID {question_group_id} not found")
        if group.is_archived:
            raise ValueError(f"Question group with ID {question_group_id} is archived")
            
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == question_group_id)
        ).all()
        
        return group, questions

    @staticmethod
    def _validate_answers_match_questions(
        answers: Dict[str, str],
        questions: list[Question]
    ) -> None:
        """Validate that answers match questions in group.
        
        Args:
            answers: Dictionary mapping question text to answer value
            questions: List of questions
            
        Raises:
            ValueError: If validation fails
        """
        question_texts = {q.text for q in questions}
        if set(answers.keys()) != question_texts:
            missing = question_texts - set(answers.keys())
            extra = set(answers.keys()) - question_texts
            raise ValueError(
                f"Answers do not match questions in group. "
                f"Missing: {missing}. Extra: {extra}"
            )

    @staticmethod
    def _run_verification(
        group: QuestionGroup,
        answers: Dict[str, str]
    ) -> None:
        """Run verification function if specified.
        
        Args:
            group: Question group
            answers: Dictionary mapping question text to answer value
            
        Raises:
            ValueError: If verification fails
        """
        if group.verification_function:
            verify_func = getattr(verify, group.verification_function, None)
            if not verify_func:
                raise ValueError(f"Verification function '{group.verification_function}' not found in verify.py")
            try:
                verify_func(answers)
            except ValueError as e:
                raise ValueError(f"Answer verification failed: {str(e)}")

    @staticmethod
    def _validate_answer_value(question: Question, answer_value: str) -> None:
        """Validate answer value matches question type and options.
        
        Args:
            question: Question object
            answer_value: Answer value to validate
            
        Raises:
            ValueError: If validation fails
        """
        if question.type == "single":
            if not question.options:
                raise ValueError(f"Question '{question.text}' has no options defined")
            if answer_value not in question.options:
                raise ValueError(
                    f"Answer value '{answer_value}' not in options for '{question.text}': "
                    f"{', '.join(question.options)}"
                )
        elif question.type == "description":
            if not isinstance(answer_value, str):
                raise ValueError(f"Description answer for '{question.text}' must be a string")

    @staticmethod
    def _check_and_update_completion(
        user_id: int,
        project_id: int,
        session: Session
    ) -> float:
        """Check if user has completed all questions in project and update completion timestamp.
        
        Args:
            user_id: The ID of the user
            project_id: The ID of the project
            session: Database session
            
        Returns:
            float: Completion percentage (0-100)
        """
        # Get total non-archived questions in project's schema
        total_questions = session.scalar(
            select(func.count())
            .select_from(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .join(Project, SchemaQuestionGroup.schema_id == Project.schema_id)
            .where(
                Project.id == project_id,
                Question.is_archived == False
            )
        )
        
        # Get total non-archived videos in project
        total_videos = session.scalar(
            select(func.count())
            .select_from(ProjectVideo)
            .join(Video, ProjectVideo.video_id == Video.id)
            .where(
                ProjectVideo.project_id == project_id,
                Video.is_archived == False
            )
        )
        
        # Get user's role
        user_role = session.scalar(
            select(ProjectUserRole)
            .where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.project_id == project_id,
                ProjectUserRole.is_archived == False
            )
        )
        
        if not user_role:
            return 0.0
            
        # Get total answers submitted by user
        if user_role.role == "annotator":
            # For annotators, count their own answers for non-archived questions
            total_answers = session.scalar(
                select(func.count())
                .select_from(AnnotatorAnswer)
                .join(Question, AnnotatorAnswer.question_id == Question.id)
                .where(
                    AnnotatorAnswer.user_id == user_id,
                    AnnotatorAnswer.project_id == project_id,
                    Question.is_archived == False
                )
            )
            
            # Update completion timestamp if all questions are answered
            expected_answers = total_questions * total_videos
            completion_percentage = min((total_answers / expected_answers * 100) if expected_answers > 0 else 0.0, 100.0)
            
            if total_answers >= expected_answers:
                user_role.completed_at = datetime.now(timezone.utc)
            else:
                user_role.completed_at = None
                
        else:  # reviewer
            # For reviewers, count total ground truth answers in project for non-archived questions
            total_answers = session.scalar(
                select(func.count())
                .select_from(ReviewerGroundTruth)
                .join(Question, ReviewerGroundTruth.question_id == Question.id)
                .where(
                    ReviewerGroundTruth.project_id == project_id,
                    Question.is_archived == False
                )
            )
            
            # Calculate completion percentage
            expected_answers = total_questions * total_videos
            completion_percentage = min((total_answers / expected_answers * 100) if expected_answers > 0 else 0.0, 100.0)
            
            # If all questions are answered, update completion timestamp for all reviewers
            # Get all reviewer roles for this project
            reviewer_roles = session.scalars(
                select(ProjectUserRole)
                .where(
                    ProjectUserRole.project_id == project_id,
                    ProjectUserRole.role == "reviewer",
                )
            ).all()
            if total_answers >= expected_answers:
                
                # Update completion timestamp for all reviewers
                for role in reviewer_roles:
                    role.completed_at = datetime.now(timezone.utc)
            else:
                # Reset completion timestamp for all reviewers            
                for role in reviewer_roles:
                    role.completed_at = None
            
        session.commit()
        return completion_percentage

class AnnotatorService(BaseAnswerService):
    @staticmethod
    def submit_answer_to_question_group(
        video_id: int,
        project_id: int,
        user_id: int,
        question_group_id: int,
        answers: Dict[str, str],  # Maps question text to answer value
        session: Session,
        confidence_scores: Optional[Dict[str, float]] = None,
        notes: Optional[Dict[str, str]] = None
    ) -> None:
        """Submit answers for all questions in a question group.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            user_id: The ID of the user submitting the answers
            question_group_id: The ID of the question group
            answers: Dictionary mapping question text to answer value
            session: Database session
            confidence_scores: Optional dictionary mapping question text to confidence score
            notes: Optional dictionary mapping question text to notes
            
        Raises:
            ValueError: If validation fails or verification fails
        """
        # Validate project and user
        project, user = AnnotatorService._validate_project_and_user(project_id, user_id, session)
        
        # Validate user role
        AnnotatorService._validate_user_role(user_id, project_id, "annotator", session)
            
        # Validate question group and get questions
        group, questions = AnnotatorService._validate_question_group(question_group_id, session)
        
        # Validate answers match questions
        AnnotatorService._validate_answers_match_questions(answers, questions)
            
        # Run verification if specified
        AnnotatorService._run_verification(group, answers)
            
        # Submit each answer
        for question in questions:
            answer_value = answers[question.text]
            confidence_score = confidence_scores.get(question.text) if confidence_scores else None
            # If confidence score is not None, check if it's float
            if confidence_score is not None:
                if not isinstance(confidence_score, float):
                    raise ValueError(f"Confidence score for question '{question.text}' must be a float")
            note = notes.get(question.text) if notes else None
            
            # Validate answer value
            AnnotatorService._validate_answer_value(question, answer_value)
            
            # Check for existing answer
            existing = session.scalar(
                select(AnnotatorAnswer).where(
                    AnnotatorAnswer.video_id == video_id,
                    AnnotatorAnswer.question_id == question.id,
                    AnnotatorAnswer.user_id == user_id,
                    AnnotatorAnswer.project_id == project_id
                )
            )
        
            if existing:
                # Update existing answer
                existing.answer_value = answer_value
                existing.modified_at = datetime.now(timezone.utc)
                existing.confidence_score = confidence_score
                existing.notes = note
            else:
                # Create new answer
                answer = AnnotatorAnswer(
                    video_id=video_id,
                    question_id=question.id,
                    project_id=project_id,
                    user_id=user_id,
                    answer_type=question.type,
                    answer_value=answer_value,
                    confidence_score=confidence_score,
                    notes=note
                )
                session.add(answer)
        session.commit()
        
        # Check and update completion status
        AnnotatorService._check_and_update_completion(user_id, project_id, session)

    @staticmethod
    def get_answers(video_id: int, project_id: int, session: Session) -> pd.DataFrame:
        """Get all answers for a video in a project.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing answers with columns:
            - Question ID
            - User ID
            - Answer ID
            - Answer Value
            - Created At
            - Modified At
            - Confidence Score
            - Notes
        """
        answers = session.scalars(
            select(AnnotatorAnswer)
            .where(
                AnnotatorAnswer.video_id == video_id,
                AnnotatorAnswer.project_id == project_id
            )
        ).all()
        
        return pd.DataFrame([
            {
                "Question ID": a.question_id,
                "User ID": a.user_id,
                "Answer ID": a.id,
                "Answer Value": a.answer_value,
                "Confidence Score": a.confidence_score,
                "Created At": a.created_at,
                "Modified At": a.modified_at,
                "Notes": a.notes
            }
            for a in answers
        ])

    @staticmethod
    def get_question_answers(question_id: int, project_id: int, session: Session) -> pd.DataFrame:
        """Get all answers for a question in a project.
        
        Args:
            question_id: The ID of the question
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing answers
        """
        answers = session.scalars(
            select(AnnotatorAnswer)
            .where(
                AnnotatorAnswer.question_id == question_id,
                AnnotatorAnswer.project_id == project_id
            )
        ).all()
        
        return pd.DataFrame([
            {
                "Video ID": a.video_id,
                "User ID": a.user_id,
                "Answer Value": a.answer_value,
                "Confidence Score": a.confidence_score,
                "Created At": a.created_at,
                "Modified At": a.modified_at,
                "Notes": a.notes
            }
            for a in answers
        ])

class GroundTruthService(BaseAnswerService):
    @staticmethod
    def submit_ground_truth_to_question_group(
        video_id: int,
        project_id: int,
        reviewer_id: int,
        question_group_id: int,
        answers: Dict[str, str],  # Maps question text to answer value
        session: Session,
        confidence_scores: Optional[Dict[str, float]] = None,
        notes: Optional[Dict[str, str]] = None
    ) -> None:
        """Submit ground truth answers for all questions in a question group.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            reviewer_id: The ID of the reviewer
            question_group_id: The ID of the question group
            answers: Dictionary mapping question text to answer value
            session: Database session
            confidence_scores: Optional dictionary mapping question text to confidence score
            notes: Optional dictionary mapping question text to notes
            
        Raises:
            ValueError: If validation fails or verification fails
        """
        # Validate project and reviewer
        project, reviewer = GroundTruthService._validate_project_and_user(project_id, reviewer_id, session)
        
        # Validate reviewer role
        GroundTruthService._validate_user_role(reviewer_id, project_id, "reviewer", session)
            
        # Validate question group and get questions
        group, questions = GroundTruthService._validate_question_group(question_group_id, session)
        
        # Validate answers match questions
        GroundTruthService._validate_answers_match_questions(answers, questions)
            
        # Run verification if specified
        GroundTruthService._run_verification(group, answers)
            
        # Submit each ground truth answer
        for question in questions:
            answer_value = answers[question.text]
            confidence_score = confidence_scores.get(question.text) if confidence_scores else None
            # If confidence score is not None, check if it's float
            if confidence_score is not None:
                if not isinstance(confidence_score, float):
                    raise ValueError(f"Confidence score for question '{question.text}' must be a float")
            note = notes.get(question.text) if notes else None
            
            # Validate answer value
            GroundTruthService._validate_answer_value(question, answer_value)
            
            # Check for existing ground truth
            existing = session.get(ReviewerGroundTruth, (video_id, question.id, project_id))
        
            if existing:
                # Update existing ground truth
                existing.answer_value = answer_value
                existing.answer_type = question.type
                existing.confidence_score = confidence_score
                existing.notes = note
                existing.modified_at = datetime.now(timezone.utc)
            else:
                # Create new ground truth
                gt = ReviewerGroundTruth(
                    video_id=video_id,
                    question_id=question.id,
                    project_id=project_id,
                    reviewer_id=reviewer_id,
                    answer_type=question.type,
                    answer_value=answer_value,
                    original_answer_value=answer_value,
                    confidence_score=confidence_score,
                    notes=note
                )
                session.add(gt)
            
        session.commit()
        
        # Check and update completion status
        GroundTruthService._check_and_update_completion(reviewer_id, project_id, session)

    @staticmethod
    def get_ground_truth(video_id: int, project_id: int, session: Session) -> pd.DataFrame:
        """Get ground truth answers for a video in a project.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            session: Database session
            
        Returns:
            DataFrame containing ground truth answers
        """
        gts = session.scalars(
            select(ReviewerGroundTruth)
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id
            )
        ).all()
        
        return pd.DataFrame([
            {
                "Question ID": gt.question_id,
                "Answer Value": gt.answer_value,
                "Original Value": gt.original_answer_value,
                "Reviewer ID": gt.reviewer_id,
                "Modified At": gt.modified_at,
                "Modified By Admin": gt.modified_by_admin_id,
                "Modified By Admin At": gt.modified_by_admin_at,
                "Confidence Score": gt.confidence_score,
                "Created At": gt.created_at,
                "Notes": gt.notes
            }
            for gt in gts
        ])

    @staticmethod
    def get_reviewer_accuracy(reviewer_id: int, project_id: int, session: Session) -> float:
        """Calculate reviewer accuracy based on admin modifications.
        
        Args:
            reviewer_id: The ID of the reviewer
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Accuracy percentage (0-100)
        """
        # Get all ground truth answers by this reviewer
        gts = session.scalars(
            select(ReviewerGroundTruth)
            .where(
                ReviewerGroundTruth.reviewer_id == reviewer_id,
                ReviewerGroundTruth.project_id == project_id
            )
        ).all()
        
        if not gts:
            return 0.0
            
        # Count answers that were modified by admin
        modified = sum(1 for gt in gts if gt.modified_by_admin_id is not None)
        total = len(gts)
        
        return ((total - modified) / total * 100) if total > 0 else 0.0

    @staticmethod
    def get_annotator_accuracy(project_id: int, question_id: int, session: Session) -> pd.DataFrame:
        """Calculate annotator accuracy for a specific question in a project.
        
        Args:
            project_id: The ID of the project
            question_id: The ID of the question
            session: Database session
            
        Returns:
            DataFrame with columns:
            - Video ID: The video ID
            - User ID: The annotator's user ID
            - Correct: 1 if answer matches ground truth or is approved, 0 otherwise
            
        Raises:
            ValueError: If not all videos have ground truth or if description answers are not all reviewed
        """
        # Get question type
        question = session.get(Question, question_id)
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
            
        # Get all videos in project
        project_videos = session.scalars(
            select(ProjectVideo.video_id)
            .where(ProjectVideo.project_id == project_id)
        ).all()
        
        if not project_videos:
            raise ValueError(f"No videos found in project {project_id}")
            
        # Check if all videos have ground truth
        missing_gt = session.scalars(
            select(ReviewerGroundTruth.video_id)
            .where(
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id == question_id,
                ReviewerGroundTruth.video_id.in_(project_videos)
            )
        ).all()
        
        if len(missing_gt) != len(project_videos):
            raise ValueError(f"Not all videos have ground truth answers for question {question_id}")
            
        # Get all annotator answers for this question
        answers = session.scalars(
            select(AnnotatorAnswer)
            .where(
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.question_id == question_id
            )
        ).all()
        
        if not answers:
            raise ValueError(f"No annotator answers found for question {question_id}")
            
        # For single-choice questions, compare directly with ground truth
        if question.type == "single":
            results = []
            for answer in answers:
                gt = session.get(ReviewerGroundTruth, (answer.video_id, question_id, project_id))
                correct = 1 if answer.answer_value == gt.answer_value else 0
                results.append({
                    "Video ID": answer.video_id,
                    "User ID": answer.user_id,
                    "Correct": correct
                })
            return pd.DataFrame(results)
            
        # For description questions, check reviews
        else:
            results = []
            for answer in answers:
                review = session.scalar(
                    select(AnswerReview)
                    .where(
                        AnswerReview.answer_id == answer.id,
                        AnswerReview.status != "pending"
                    )
                )
                
                if not review:
                    raise ValueError(f"Answer {answer.id} has not been reviewed")
                    
                correct = 1 if review.status == "approved" else 0
                results.append({
                    "Video ID": answer.video_id,
                    "User ID": answer.user_id,
                    "Correct": correct
                })
            return pd.DataFrame(results)

    @staticmethod
    def override_ground_truth_to_question_group(
        video_id: int,
        project_id: int,
        question_group_id: int,
        admin_id: int,
        answers: Dict[str, str],  # Maps question text to answer value
        session: Session
    ) -> None:
        """Override ground truth answers for all questions in a question group (admin only).
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_group_id: The ID of the question group
            admin_id: The ID of the admin
            answers: Dictionary mapping question text to answer value
            session: Database session
            
        Raises:
            ValueError: If validation fails
        """
        # Validate project and admin
        project, admin = GroundTruthService._validate_project_and_user(project_id, admin_id, session)
        
        # Validate project admin role
        GroundTruthService._validate_user_role(admin_id, project_id, "admin", session)
            
        # Validate question group and get questions
        group, questions = GroundTruthService._validate_question_group(question_group_id, session)
        
        # Validate answers match questions
        GroundTruthService._validate_answers_match_questions(answers, questions)
            
        # Run verification if specified
        GroundTruthService._run_verification(group, answers)
            
        # Override each ground truth answer
        for question in questions:
            answer_value = answers[question.text]
            
            # Validate answer value
            GroundTruthService._validate_answer_value(question, answer_value)
            
            # Get ground truth
            gt = session.get(ReviewerGroundTruth, (video_id, question.id, project_id))
            if not gt:
                raise ValueError(f"No ground truth found for video {video_id}, question {question.id}, project {project_id}")
            
            # Only update if answer value actually changes
            if gt.answer_value != answer_value:
                gt.answer_value = answer_value
                gt.modified_by_admin_id = admin_id
                gt.modified_by_admin_at = datetime.now(timezone.utc)
        
        session.commit()

    @staticmethod
    def submit_answer_review(
        answer_id: int,
        reviewer_id: int,
        status: str,  # "approved"/"rejected"/"pending"
        session: Session,
        comment: Optional[str] = None  # Optional comment for the review
    ) -> None:
        """Submit a review for a description-type answer.
        
        Args:
            answer_id: The ID of the answer to review
            reviewer_id: The ID of the reviewer
            status: Review status ("approved"/"rejected"/"pending")
            session: Database session
            comment: Optional review comment
            
        Raises:
            ValueError: If validation fails
        """
        # Get the answer
        answer = session.get(AnnotatorAnswer, answer_id)
        if not answer:
            raise ValueError(f"Answer with ID {answer_id} not found")
            
        # Get the question
        question = session.get(Question, answer.question_id)
        if not question:
            raise ValueError(f"Question with ID {answer.question_id} not found")
            
        # Verify question is description type
        if question.type != "description":
            raise ValueError(f"Question '{question.text}' is not a description type question")
            
        # Validate reviewer role
        GroundTruthService._validate_user_role(reviewer_id, answer.project_id, "reviewer", session)
        
        # Validate review status
        valid_statuses = {"approved", "rejected", "pending"}
        if status not in valid_statuses:
            raise ValueError(f"Invalid review status: {status}. Must be one of {valid_statuses}")
        
        # Create or update review
        review = session.scalar(
            select(AnswerReview)
            .where(AnswerReview.answer_id == answer_id)
        )
        
        if review:
            # Update existing review
            review.status = status
            review.comment = comment
            review.reviewer_id = reviewer_id  # Update reviewer in case it's different
            review.reviewed_at = datetime.now(timezone.utc)
        else:
            # Create new review
            review = AnswerReview(
                answer_id=answer_id,
                reviewer_id=reviewer_id,
                status=status,
                comment=comment
            )
            session.add(review)
            
        session.commit()

    @staticmethod
    def get_answer_review(answer_id: int, session: Session) -> Optional[dict]:
        """Get the review for a specific answer.
        
        Args:
            answer_id: The ID of the answer
            session: Database session
            
        Returns:
            Dictionary containing the review with keys:
            - status: The review status ("approved"/"rejected"/"pending")
            - comment: The review comment
            - reviewer_id: The ID of the reviewer
            - reviewed_at: When the review was made
            Returns None if no review exists
        """
        review = session.scalar(
            select(AnswerReview)
            .where(AnswerReview.answer_id == answer_id)
        )
        
        if not review:
            return None
            
        return {
            "status": review.status,
            "comment": review.comment,
            "reviewer_id": review.reviewer_id,
            "reviewed_at": review.reviewed_at
        }

class ProjectGroupService:
    @staticmethod
    def create_project_group(name: str, description: str, project_ids: list[int] | None, session: Session) -> ProjectGroup:
        """Create a new project group with optional list of project IDs, enforcing uniqueness constraints."""
        # Check for unique name
        existing = session.scalar(select(ProjectGroup).where(ProjectGroup.name == name))
        if existing:
            raise ValueError(f"Project group with name '{name}' already exists")
        group = ProjectGroup(
            name=name,
            description=description,
        )
        session.add(group)
        session.flush()  # get group.id
        if project_ids:
            ProjectGroupService._validate_project_group_uniqueness(project_ids, session)
            for pid in project_ids:
                session.add(ProjectGroupProject(project_group_id=group.id, project_id=pid))
        session.commit()
        return group

    @staticmethod
    def edit_project_group(group_id: int, name: str | None, description: str | None, add_project_ids: list[int] | None, remove_project_ids: list[int] | None, session: Session) -> ProjectGroup:
        """Edit group name/description, add/remove projects, enforcing uniqueness constraints when adding."""
        group = session.get(ProjectGroup, group_id)
        if not group:
            raise ValueError(f"Project group with ID {group_id} not found")
        if name:
            # Check for unique name
            existing = session.scalar(select(ProjectGroup).where(ProjectGroup.name == name, ProjectGroup.id != group_id))
            if existing and existing.id != group_id:
                raise ValueError(f"Project group with name '{name}' already exists")
            group.name = name
        if description:
            group.description = description
        if add_project_ids:
            # Get current project IDs
            current_ids = set(row.project_id for row in session.scalars(select(ProjectGroupProject).where(ProjectGroupProject.project_group_id == group_id)).all())
            new_ids = set(add_project_ids)
            all_ids = list(current_ids | new_ids)
            ProjectGroupService._validate_project_group_uniqueness(all_ids, session)
            for pid in new_ids - current_ids:
                session.add(ProjectGroupProject(project_group_id=group_id, project_id=pid))
        if remove_project_ids:
            for pid in remove_project_ids:
                row = session.scalar(select(ProjectGroupProject).where(ProjectGroupProject.project_group_id == group_id, ProjectGroupProject.project_id == pid))
                if row:
                    session.delete(row)
        session.commit()
        return group

    @staticmethod
    def get_project_group_by_id(group_id: int, session: Session):
        group = session.get(ProjectGroup, group_id)
        if not group:
            raise ValueError(f"Project group with ID {group_id} not found")
        projects = session.scalars(
            select(Project).join(ProjectGroupProject, Project.id == ProjectGroupProject.project_id)
            .where(ProjectGroupProject.project_group_id == group_id)
        ).all()
        return {"group": group, "projects": projects}
    
    @staticmethod
    def get_project_group_by_name(name: str, session: Session):
        """Get a project group by name."""
        group = session.scalar(select(ProjectGroup).where(ProjectGroup.name == name))
        if not group:
            raise ValueError(f"Project group with name '{name}' not found")
        return group

    @staticmethod
    def list_project_groups(session: Session):
        groups = session.scalars(select(ProjectGroup)).all()
        return groups

    @staticmethod
    def _validate_project_group_uniqueness(project_ids: list[int], session: Session):
        # For every pair of projects, check uniqueness constraint
        projects = [session.get(Project, pid) for pid in project_ids]
        # Get schema questions for each project
        project_questions = {}
        project_videos = {}
        for p in projects:
            if not p:
                raise ValueError(f"Project with ID {p.id if p else None} not found")
            # Get all questions in schema
            qids = set(session.scalars(
                select(Question.id)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == p.schema_id)
            ).all())
            vids = set(session.scalars(
                select(ProjectVideo.video_id)
                .where(ProjectVideo.project_id == p.id)
            ).all())
            # Only consider non-archived videos
            vids = set(
                v for v in vids if not session.get(Video, v).is_archived
            )
            project_questions[p.id] = qids
            project_videos[p.id] = vids
        # Check all pairs
        n = len(projects)
        for i in range(n):
            for j in range(i+1, n):
                q_overlap = project_questions[projects[i].id] & project_questions[projects[j].id]
                if not q_overlap:
                    continue  # No conflict
                v_overlap = project_videos[projects[i].id] & project_videos[projects[j].id]
                if v_overlap:
                    raise ValueError(f"Projects {projects[i].id} and {projects[j].id} have overlapping questions and videos: {v_overlap}")
