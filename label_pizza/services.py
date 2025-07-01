from sqlalchemy import select, insert, update, func, delete, exists, join, distinct, and_, or_, case, text
from sqlalchemy.orm import Session, selectinload, joinedload, contains_eager  
from sqlalchemy.sql import literal_column
from typing import List, Optional, Dict, Any, Tuple
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, AnnotatorAnswer, ReviewerGroundTruth, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup, ProjectGroup, ProjectGroupProject,
    ProjectVideoQuestionDisplay
)
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
# verify_path = Path(__file__).parent / "verify.py"
# spec = importlib.util.spec_from_file_location("verify", verify_path)
# verify = importlib.util.module_from_spec(spec)
# sys.modules["verify"] = verify
# spec.loader.exec_module(verify)
from label_pizza import verify


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
                "ID": v.id,
                "Video UID": v.video_uid,
                "URL": v.url,
                "Created At": v.created_at,
                "Updated At": v.updated_at,
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
    def verify_add_video(url: str, session: Session, metadata: dict = None) -> None:
        """Verify parameters for adding a new video.

        Args:
            url: The URL of the video
            session: Database session
            metadata: Optional dictionary containing video metadata

        Raises:
            ValueError: If URL is invalid, metadata is invalid, or video already exists
        """
        # Validate URL format
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
        filename = url.split("/")[-1]
        existing = VideoService.get_video_by_uid(filename, session)
        if existing:
            raise ValueError(f"Video with UID '{filename}' already exists")

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
        # Verify input parameters and get the filename
        VideoService.verify_add_video(url, session, metadata)

        filename = url.split("/")[-1]
        # Create video
        video = Video(
            video_uid=filename,
            url=url,
            video_metadata=metadata or {}
        )
        session.add(video)
        session.commit()

    @staticmethod
    def get_project_videos(project_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all non-archived videos in a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            List of video dictionaries containing: id, uid, url, metadata
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        videos = session.scalars(
            select(Video)
            .join(ProjectVideo, Video.id == ProjectVideo.video_id)
            .where(
                ProjectVideo.project_id == project_id,
                Video.is_archived == False
            )
        ).all()
        
        return [{
            "id": v.id,
            "uid": v.video_uid,
            "url": v.url,
            "metadata": v.video_metadata
        } for v in videos]

    @staticmethod
    def verify_update_video(video_uid: str, new_url: str, new_metadata: dict, session: Session) -> None:
        """Verify parameters for updating a video.

        Args:
            video_uid: Video UID to update
            new_url: New video URL
            new_metadata: New metadata dictionary
            session: Database session

        Raises:
            ValueError: If video not found or validation fails
        """
        # Check if video exists
        video = VideoService.get_video_by_uid(video_uid=video_uid, session=session)
        if not video:
            raise ValueError(f"Video with UID '{video_uid}' not found")

        # Validate new URL if provided
        if new_url and new_url != video.url:
            if not new_url.startswith(("http://", "https://")):
                raise ValueError("URL must start with http:// or https://")

            # ---- new check ----
            # Strip any query string, then compare the filename to the UID
            filename = new_url.split("/")[-1]
            if filename != video_uid:
                raise ValueError(
                    f"URL filename '{filename}' must exactly match the video UID '{video_uid}'"
                )
            # -------------------

        # Validate new metadata if provided
        if new_metadata is not None:
            if not isinstance(new_metadata, dict):
                raise ValueError("Metadata must be a dictionary")
            if not new_metadata:
                raise ValueError("Metadata must be a non-empty dictionary")

            # Validate metadata value types
            for key, value in new_metadata.items():
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

    @staticmethod
    def update_video(video_uid: str, new_url: str, new_metadata: dict, session: Session) -> None:
        """Update video URL and metadata.

        Args:
            video_uid: Video UID to update
            new_url: New video URL
            new_metadata: New metadata dictionary
            session: Database session

        Raises:
            ValueError: If video not found or validation fails
        """
        # Verify parameters (raises ValueError if invalid)
        VideoService.verify_update_video(video_uid, new_url, new_metadata, session)

        # Get the video object for updating
        video = VideoService.get_video_by_uid(video_uid=video_uid, session=session)

        # Update URL if provided and different
        if new_url and new_url != video.url:
            video.url = new_url

        # Update metadata if provided
        if new_metadata is not None:
            video.video_metadata = new_metadata

        video.updated_at = datetime.now(timezone.utc)
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
        
        # Single optimized query using subqueries and joins
        query = select(
            Project.id,
            Project.name,
            Project.schema_id,
            # Count videos in project
            func.coalesce(
                select(func.count(ProjectVideo.video_id))
                .select_from(ProjectVideo)
                .where(ProjectVideo.project_id == Project.id)
                .scalar_subquery(), 0
            ).label('video_count'),
            # Count total questions in schema
            func.coalesce(
                select(func.count(Question.id))
                .select_from(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                .where(SchemaQuestionGroup.schema_id == Project.schema_id)
                .scalar_subquery(), 0
            ).label('total_questions'),
            # Count ground truth answers
            func.coalesce(
                select(func.count(ReviewerGroundTruth.project_id))
                .select_from(ReviewerGroundTruth)
                .where(ReviewerGroundTruth.project_id == Project.id)
                .scalar_subquery(), 0
            ).label('gt_answers')
        ).select_from(
            Project
        ).join(
            Schema, Project.schema_id == Schema.id
        ).where(
            and_(Project.is_archived == False, Schema.is_archived == False)
        )
        
        result = session.execute(query).all()
        
        rows = []
        for row in result:
            # Calculate GT percentage
            total_possible = row.video_count * row.total_questions
            gt_percentage = (row.gt_answers / total_possible * 100) if total_possible > 0 else 0.0
            
            rows.append({
                "ID": row.id,
                "Name": row.name,
                "Videos": row.video_count,
                "Schema ID": row.schema_id,
                "GT %": gt_percentage
            })
        
        return pd.DataFrame(rows)

    @staticmethod
    def get_all_projects_old(session: Session) -> pd.DataFrame:
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
        
        # Assign all admin users to the project using KEYWORD ARGUMENTS
        admin_users = session.scalars(
            select(User).where(User.user_type == "admin", User.is_archived == False)
        ).all()
        
        for admin in admin_users:
            ProjectService.add_user_to_project(
                project_id=project.id, 
                user_id=admin.id, 
                role="admin", 
                session=session
            )
        
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
    def get_project_reviewers(project_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all reviewers who have submitted ground truth in a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            List of reviewer dictionaries containing: id, name, email
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get all users who have submitted ground truth in this project
        reviewer_ids = session.scalars(
            select(ReviewerGroundTruth.reviewer_id)
            .where(ReviewerGroundTruth.project_id == project_id)
            .distinct()
        ).all()
        
        reviewers = []
        for reviewer_id in reviewer_ids:
            user = session.get(User, reviewer_id)
            if user and not user.is_archived:
                reviewers.append({
                    'id': reviewer_id,
                    'name': user.user_id_str,
                    'email': user.email
                })
        
        return reviewers

    
    @staticmethod
    def get_project_questions(project_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all questions in a project's schema.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            List of question dictionaries containing: id, text, type
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get all questions in the project's schema
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(
                SchemaQuestionGroup.schema_id == project.schema_id,
                Question.is_archived == False
            )
        ).all()
        
        return [{
            'id': q.id,
            'text': q.text,
            'type': q.type
        } for q in questions]
    

    @staticmethod
    def get_project_questions_with_custom_display(project_id: int, video_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all questions in a project's schema with custom display applied for a specific video.
        
        Args:
            project_id: The ID of the project
            video_id: The ID of the video (for custom display context)
            session: Database session
            
        Returns:
            List of question dictionaries with custom display applied
            
        Raises:
            ValueError: If project not found
        """
        # Get original questions
        original_questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        
        # Apply custom display to each question
        result = []
        for q in original_questions:
            # Get full question data with custom display
            question_with_custom = CustomDisplayService.get_custom_display(
                question_id=q['id'],
                project_id=project_id,
                video_id=video_id,
                session=session
            )
            
            # Return same format as original but with custom display fields
            result.append({
                'id': question_with_custom['id'],
                'text': question_with_custom['text'],
                'display_text': question_with_custom['display_text'],  # Custom display applied
                'type': question_with_custom['type'],
                'options': question_with_custom['options'],
                'display_values': question_with_custom['display_values'],  # Custom display applied
                'option_weights': question_with_custom['option_weights'],
                'default_option': question_with_custom['default_option']
            })
        
        return result


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
    def get_project_dict_by_id(project_id: int, session: Session) -> Dict[str, Any]:
        """Get project details as a dictionary by ID.
        
        Returns:
            Dictionary with project details: id, name, description, schema_id, etc.
        """
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        return {
            "id": project.id,
            "name": project.name,
            "description": project.description,
            "schema_id": project.schema_id,
            "created_at": project.created_at,
            "is_archived": project.is_archived,
        }


    @staticmethod
    def add_user_to_project(project_id: int, user_id: int, role: str, session: Session, user_weight: Optional[float] = None) -> None:
        """Add a user to a project with the specified role.
        
        Args:
            project_id: The ID of the project
            user_id: The ID of the user
            role: The role to assign ('annotator', 'reviewer', 'admin', or 'model')
            session: Database session
            user_weight: Optional weight for the user's answers (defaults to 1.0)
            
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
                existing.user_weight = user_weight if user_weight is not None else 1.0
            else:
                session.add(ProjectUserRole(
                    project_id=project_id,
                    user_id=user_id,
                    role=role_type,
                    user_weight=user_weight if user_weight is not None else 1.0
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
    
    
    @staticmethod
    def get_project_annotators(project_id: int, session: Session) -> Dict[str, Dict[str, Any]]:
        """Get all annotators who have submitted answers in a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Dictionary mapping display names to annotator info.
            Each annotator info contains: id, name, email
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get all users who have submitted answers in this project
        user_ids = session.scalars(
            select(AnnotatorAnswer.user_id)
            .where(AnnotatorAnswer.project_id == project_id)
            .distinct()
        ).all()
        
        annotators = {}
        for user_id in user_ids:
            user = session.get(User, user_id)
            if user and not user.is_archived:
                user_name = user.user_id_str
                user_email = user.email or f"user_{user.id}@example.com"
                
                # Use the centralized function
                display_name, initials = AuthService.get_user_display_name_with_initials(user_name)
                
                annotators[display_name] = {
                    'id': user_id,
                    'name': user_name,
                    'email': user_email
                }
        
        return annotators

    @staticmethod
    def check_project_has_full_ground_truth(project_id: int, session: Session) -> bool:
        """Check if project has complete ground truth for all questions and videos.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            True if project has complete ground truth, False otherwise
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        try:
            # Get total questions in schema
            total_questions = session.scalar(
                select(func.count())
                .select_from(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
                .where(
                    SchemaQuestionGroup.schema_id == project.schema_id,
                    Question.is_archived == False
                )
            )
            
            # Get total videos in project
            total_videos = session.scalar(
                select(func.count())
                .select_from(ProjectVideo)
                .join(Video, ProjectVideo.video_id == Video.id)
                .where(
                    ProjectVideo.project_id == project_id,
                    Video.is_archived == False
                )
            )
            
            if total_questions == 0 or total_videos == 0:
                return False
            
            # Get total ground truth answers
            gt_count = session.scalar(
                select(func.count())
                .select_from(ReviewerGroundTruth)
                .join(Question, ReviewerGroundTruth.question_id == Question.id)
                .where(
                    ReviewerGroundTruth.project_id == project_id,
                    Question.is_archived == False
                )
            )
            
            expected_answers = total_questions * total_videos
            return gt_count >= expected_answers
            
        except Exception:
            return False


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
                "Instructions URL": s.instructions_url,
                "Question Groups": ", ".join(g.title for g in groups) if groups else "No groups",
                "Has Custom Display": s.has_custom_display,
                "Archived": s.is_archived
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
    def get_schema_questions_with_custom_display(schema_id: int, project_id: int, video_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a schema with custom display applied for a specific project-video combination.
        
        Args:
            schema_id: The ID of the schema
            project_id: The ID of the project (for custom display context)
            video_id: The ID of the video (for custom display context)
            session: Database session
            
        Returns:
            DataFrame containing questions with custom display applied (same columns as original)
            
        Raises:
            ValueError: If schema not found
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")

        if not schema.has_custom_display:
            raise ValueError(f"Schema with ID {schema_id} does not have custom display enabled")
        
        # Get questions through question groups with custom display
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == schema_id)
        ).all()
        
        rows = []
        for q in questions:
            # Get custom display data
            question_with_custom = CustomDisplayService.get_custom_display(
                question_id=q.id,
                project_id=project_id,
                video_id=video_id,
                session=session
            )
            
            # Get group name
            group_name = session.scalar(
                select(QuestionGroup.title)
                .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                .where(QuestionGroupQuestion.question_id == q.id)
            )
            
            # Same format as original but with custom display
            rows.append({
                "ID": question_with_custom["id"],
                "Text": question_with_custom["text"],
                "Display Text": question_with_custom["display_text"],  # Custom display applied
                "Group": group_name,
                "Type": question_with_custom["type"],
                "Options": ", ".join(question_with_custom["options"] or []) if question_with_custom["options"] else "",
                "Display Values": ", ".join(question_with_custom["display_values"] or []) if question_with_custom["display_values"] else ""  # Custom display applied
            })
        
        return pd.DataFrame(rows)

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
    def get_schema_name_by_id(schema_id: int, session: Session) -> str:
        """Get schema name by ID.
        
        Args:
            schema_id: The ID of the schema
            session: Database session
            
        Returns:
            Schema name
            
        Raises:
            ValueError: If schema not found or if schema is archived
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        if schema.is_archived:
            raise ValueError(f"Schema with ID {schema_id} is archived")
        return schema.name

    @staticmethod
    def verify_create_schema(
            name: str,
            question_group_ids: List[int],
            instructions_url: Optional[str] = None,
            has_custom_display: bool = False,
            session: Session = None
    ) -> None:
        """Verify parameters for creating a new schema.

        This function performs all validation checks and raises ValueError
        if any validation fails. It does not return any objects.

        Args:
            name: Schema name
            question_group_ids: List of question group IDs in desired order
            instructions_url: URL for schema instructions
            has_custom_display: Whether schema has custom display
            session: Database session

        Raises:
            ValueError: If schema with same name exists or validation fails
        """
        # Validate name
        if not name or not name.strip():
            raise ValueError("Schema name is required")

        # Check if schema with same name exists
        existing = session.scalar(select(Schema).where(Schema.name == name))
        if existing:
            raise ValueError(f"Schema with name '{name}' already exists")

        # Validate question group IDs
        if not question_group_ids:
            raise ValueError("Schema must contain at least one question group")

        # Validate instructions URL if provided
        if instructions_url:
            instructions_url = instructions_url.strip()
            if not instructions_url.startswith(("http://", "https://")):
                raise ValueError("Instructions URL must start with http:// or https://")

        # Validate all question groups
        for group_id in question_group_ids:
            # Check if group exists
            group = QuestionGroupService.get_group_by_id(group_id, session)
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
                    raise ValueError(
                        f"Question group {group.title} is not reusable and is already used in schema {existing_schema.name}")

    @staticmethod
    def create_schema(
        name: str,
        question_group_ids: List[int],
        instructions_url: Optional[str] = None,
        has_custom_display: bool = False,
        session: Session = None
    ) -> Schema:
        """Create a new schema with its question groups.

        Args:
            name: Schema name
            question_group_ids: List of question group IDs in desired order
            instructions_url: URL for schema instructions
            has_custom_display: Whether schema has custom display
            session: Database session

        Returns:
            Created schema

        Raises:
            ValueError: If schema with same name exists or validation fails
        """
        # First, verify all parameters (will raise ValueError if validation fails)
        SchemaService.verify_create_schema(name, question_group_ids, instructions_url, has_custom_display, session)

        # Create schema object
        schema = Schema(name=name, instructions_url=instructions_url, has_custom_display=has_custom_display)
        session.add(schema)
        session.flush()  # Get schema ID

        # Add question groups to schema in specified order
        for i, group_id in enumerate(question_group_ids):
            sqg = SchemaQuestionGroup(
                schema_id=schema.id,
                question_group_id=group_id,
                display_order=i
            )
            session.add(sqg)

        session.commit()
        return schema
    
    @staticmethod
    def get_schema_details(schema_id: int, session: Session) -> Dict[str, Any]:
        """Get complete schema details including instructions URL.
        
        Args:
            schema_id: Schema ID
            session: Database session
            
        Returns:
            Dictionary containing schema details
            
        Raises:
            ValueError: If schema not found
        """
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        return {
            "id": schema.id,
            "name": schema.name,
            "instructions_url": schema.instructions_url,
            "created_at": schema.created_at,
            "updated_at": schema.updated_at,
            "has_custom_display": schema.has_custom_display,
            "is_archived": schema.is_archived
        }

    @staticmethod
    def verify_edit_schema(
        schema_id: int,
        name: Optional[str] = None,
        instructions_url: Optional[str] = None,
        has_custom_display: Optional[bool] = None,
        is_archived: Optional[bool] = None,
        session: Session = None
    ) -> None:
        """Verify parameters for editing a schema.

        This function performs all validation checks and raises ValueError
        if any validation fails. It does not return any objects.

        Args:
            schema_id: Schema ID
            name: New schema name (optional)
            instructions_url: New instructions URL (optional, use empty string to clear)
            has_custom_display: New custom display flag (optional)
            is_archived: New archive status (optional)
            session: Database session

        Raises:
            ValueError: If schema not found or validation fails
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        # Validate name if provided
        if name is not None:
            name = name.strip()
            if not name:
                raise ValueError("Schema name cannot be empty")
            
            # Check for unique name (excluding current schema)
            existing = session.scalar(
                select(Schema).where(Schema.name == name, Schema.id != schema_id)
            )
            if existing:
                raise ValueError(f"Schema with name '{name}' already exists")
        
        # Validate instructions URL if provided
        if instructions_url is not None:
            if instructions_url.strip():
                instructions_url = instructions_url.strip()
                if not instructions_url.startswith(("http://", "https://")):
                    raise ValueError("Instructions URL must start with http:// or https://")
        
        # Validate archive status change if provided
        if is_archived is not None:
            if is_archived and not schema.is_archived:
                # Trying to archive - check no non-archived projects are using this schema
                projects_using_schema = session.scalars(
                    select(Project)
                    .where(
                        Project.schema_id == schema_id,
                        Project.is_archived == False
                    )
                ).all()
                
                if projects_using_schema:
                    project_names = [p.name for p in projects_using_schema]
                    raise ValueError(
                        f"Cannot archive schema. The following non-archived projects are using it: "
                        f"{', '.join(project_names)}"
                    )
            
            elif not is_archived and schema.is_archived:
                # Trying to unarchive - check all question groups in schema are not archived
                archived_groups = session.scalars(
                    select(QuestionGroup)
                    .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
                    .where(
                        SchemaQuestionGroup.schema_id == schema_id,
                        QuestionGroup.is_archived == True
                    )
                ).all()
                
                if archived_groups:
                    group_names = [g.title for g in archived_groups]
                    raise ValueError(
                        f"Cannot unarchive schema. The following question groups in the schema are archived: "
                        f"{', '.join(group_names)}"
                    )

    @staticmethod
    def edit_schema(
        schema_id: int, 
        name: Optional[str] = None,
        instructions_url: Optional[str] = None,
        has_custom_display: Optional[bool] = None,
        is_archived: Optional[bool] = None,
        session: Session = None
    ) -> None:
        """Edit a schema's properties.
        
        Args:
            schema_id: Schema ID
            name: New schema name (optional)
            instructions_url: New instructions URL (optional, use empty string to clear)
            has_custom_display: New custom display flag (optional)
            is_archived: New archive status (optional)
            session: Database session
            
        Raises:
            ValueError: If schema not found or validation fails
        """
        # First, verify all parameters (will raise ValueError if validation fails)
        SchemaService.verify_edit_schema(
            schema_id, name, instructions_url, has_custom_display, is_archived, session
        )
        
        # Get schema object for updating
        schema = session.get(Schema, schema_id)
        
        # Update name if provided
        if name is not None:
            schema.name = name.strip()
        
        # Update instructions URL if provided
        if instructions_url is not None:
            if instructions_url.strip():
                schema.instructions_url = instructions_url.strip()
            else:
                # Empty string means clear the URL
                schema.instructions_url = None
        
        # Update custom display flag if provided
        if has_custom_display is not None:
            schema.has_custom_display = has_custom_display
        
        # Update archive status if provided
        if is_archived is not None:
            schema.is_archived = is_archived
        
        schema.updated_at = datetime.now(timezone.utc)
        session.commit()

    @staticmethod
    def archive_schema(schema_id: int, session: Session) -> None:
        """Archive a schema and prevent its use in new projects.
        
        Args:
            schema_id: The ID of the schema to archive
            session: Database session
            
        Raises:
            ValueError: If schema not found or has active projects
        """
        # Use the edit function with proper validation
        SchemaService.edit_schema(schema_id=schema_id, is_archived=True, session=session)

    @staticmethod
    def unarchive_schema(schema_id: int, session: Session) -> None:
        """Unarchive a schema to allow its use in new projects.
        
        Args:
            schema_id: The ID of the schema to unarchive
            session: Database session
            
        Raises:
            ValueError: If schema not found or has archived question groups
        """
        # Use the edit function with proper validation
        SchemaService.edit_schema(schema_id=schema_id, is_archived=False, session=session)

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
            - Display Title: Question group display title
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
                "Display Title": group.display_title,
                "Description": group.description,
                "Reusable": group.is_reusable,
                "Archived": group.is_archived,
                "Display Order": display_order,
                "Question Count": question_count
            })
            
        return pd.DataFrame(rows)
    
    @staticmethod
    def get_schema_question_groups_list(schema_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get question groups in a schema as a list of dictionaries.
        
        Args:
            schema_id: The ID of the schema
            session: Database session
            
        Returns:
            List of question group dictionaries containing: ID, Title, Description
            
        Raises:
            ValueError: If schema not found
        """
        # Check if schema exists
        schema = session.get(Schema, schema_id)
        if not schema:
            raise ValueError(f"Schema with ID {schema_id} not found")
        
        groups = session.scalars(
            select(QuestionGroup)
            .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == schema_id)
            .order_by(SchemaQuestionGroup.display_order)
        ).all()
        
        return [{
            "ID": g.id,
            "Title": g.title,
            "Display Title": g.display_title,
            "Description": g.description
        } for g in groups]

class QuestionService:
    
    @staticmethod
    def get_all_questions(session: Session) -> pd.DataFrame:
        """Get all questions with their group information.
        
        Args:
            session: Database session
        
        Returns:
            DataFrame containing questions with columns:
            - ID: Question ID
            - Text: Question text (immutable, unique)
            - Display Text: UI display text
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
                "Display Text": q.display_text,
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
    def get_all_questions_with_custom_display(project_id: int, video_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a project's schema with custom display applied for a specific video.
        
        Args:
            project_id: Project ID 
            video_id: Video ID (for custom display context)
            session: Database session
            
        Returns:
            DataFrame containing all questions with custom display applied
            
        Raises:
            ValueError: If project not found
        """
        # Get project to find schema
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        
        # Get all questions in project's schema
        qs = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(SchemaQuestionGroup.schema_id == project.schema_id)
        ).all()
        
        rows = []
        for q in qs:
            # Get custom display data
            question_with_custom = CustomDisplayService.get_custom_display(
                question_id=q.id,
                project_id=project_id,
                video_id=video_id,
                session=session
            )
            
            # Get group name
            group_name = session.scalar(
                select(QuestionGroup.title)
                .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                .where(QuestionGroupQuestion.question_id == q.id)
            )
            
            # Same format as original get_all_questions but with custom display
            rows.append({
                "ID": question_with_custom["id"], 
                "Text": question_with_custom["text"], 
                "Display Text": question_with_custom["display_text"],  # Custom display applied
                "Type": question_with_custom["type"],
                "Group": group_name,
                "Options": ", ".join(question_with_custom["options"] or []) if question_with_custom["options"] else "",
                "Display Values": ", ".join(question_with_custom["display_values"] or []) if question_with_custom["display_values"] else "",  # Custom display applied  
                "Default": question_with_custom["default_option"] or "",
                "Archived": question_with_custom["archived"]
            })
    
    @staticmethod
    def get_question_object_by_id(question_id: int, session: Session) -> Question:
        """Get a question object by its ID.
        
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

    @staticmethod
    def add_question(text: str, qtype: str, options: Optional[List[str]], default: Optional[str], 
                    session: Session, display_values: Optional[List[str]] = None, display_text: Optional[str] = None,
                    option_weights: Optional[List[float]] = None) -> Question:
        """Add a new question.
        
        Args:
            text: Question text (immutable, unique)
            qtype: Question type ('single' or 'description')
            options: List of options for single-choice questions
            default: Default option for single-choice questions
            session: Database session
            display_values: Optional list of display text for options. For single-type questions, if not provided, uses options as display values.
            display_text: Optional display text for UI. If not provided, uses text.
            option_weights: Optional list of weights for each option. If not provided, defaults to 1.0 for each option.
        
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
                
            # Handle option weights
            if option_weights:
                if len(option_weights) != len(options):
                    raise ValueError("Number of option weights must match number of options")
            else:
                option_weights = [1.0] * len(options)  # Default to 1.0 for each option
        else:
            # For description-type questions, display_values and option_weights should be None
            display_values = None
            option_weights = None
        
        # Set display_text
        if not display_text:
            display_text = text
        
        # Create question
        q = Question(
            text=text, 
            display_text=display_text,
            type=qtype, 
            options=options, 
            display_values=display_values,
            option_weights=option_weights,
            default_option=default
        )
        session.add(q)
        session.commit()
        return q

    @staticmethod
    def verify_edit_question(
            question_id: int,
            new_display_text: str,
            new_opts: Optional[List[str]],
            new_default: Optional[str],
            session: Session,
            new_display_values: Optional[List[str]] = None,
            new_option_weights: Optional[List[float]] = None
    ) -> None:
        """Verify parameters for editing an existing question.

        Args:
            question_id: Current question ID
            new_display_text: New display text for UI
            new_opts: New options for single-choice questions. Must include all existing options.
            new_default: New default option for single-choice questions
            session: Database session
            new_display_values: Optional new display values for options. For single-type questions, if not provided, maintains existing display values or uses options.
            new_option_weights: Optional new weights for options. For single-type questions, if not provided, maintains existing weights.

        Raises:
            ValueError: If question not found or validation fails
        """
        # Get question
        q = QuestionService.get_question_object_by_id(question_id=question_id, session=session)

        # Check if question is archived
        if q.is_archived:
            raise ValueError(f"Question with ID {question_id} is archived")

        # For single-choice questions, validate options and display values
        if q.type == "single":
            if not new_opts:
                raise ValueError("Cannot change question type")
            if new_default and new_default not in new_opts:
                raise ValueError(
                    f"Default option '{new_default}' must be one of the available options: {', '.join(new_opts)}")

            # Validate that all existing options are included in new options
            missing_opts = set(q.options) - set(new_opts)
            if missing_opts:
                raise ValueError(f"Cannot remove existing options: {', '.join(missing_opts)}")

            # Validate display values
            if new_display_values:
                if len(new_display_values) != len(new_opts):
                    raise ValueError("Number of display values must match number of options")

            # Validate option weights
            if new_option_weights:
                if len(new_option_weights) != len(new_opts):
                    raise ValueError("Number of option weights must match number of options")
        else:  # description type
            if new_opts is not None or new_default is not None or new_display_values is not None or new_option_weights is not None:
                raise ValueError("Cannot change question type")

    @staticmethod
    def edit_question(question_id: int, new_display_text: str, new_opts: Optional[List[str]],
                      new_default: Optional[str],
                      session: Session, new_display_values: Optional[List[str]] = None,
                      new_option_weights: Optional[List[float]] = None) -> None:
        """Edit an existing question (only display_text and options, not text).

        Args:
            question_id: Current question ID
            new_display_text: New display text for UI.
            new_opts: New options for single-choice questions. Must include all existing options.
            new_default: New default option for single-choice questions
            session: Database session
            new_display_values: Optional new display values for options. For single-type questions, if not provided, maintains existing display values or uses options.
            new_option_weights: Optional new weights for options. For single-type questions, if not provided, maintains existing weights or defaults to 1.0.

        Raises:
            ValueError: If question not found or validation fails
        """
        # Verify parameters (raises ValueError if invalid)
        QuestionService.verify_edit_question(
            question_id, new_display_text, new_opts, new_default, session, new_display_values, new_option_weights
        )

        # Get question again after validation
        q = QuestionService.get_question_object_by_id(question_id=question_id, session=session)

        # Process defaults after validation passes
        if q.type == "single":
            # For single-type questions, ensure we have display values
            if new_display_values is None:
                # If no new display values provided, maintain existing mapping for unchanged options
                new_display_values = []
                for opt in new_opts:
                    if opt in q.options:
                        idx = q.options.index(opt)
                        new_display_values.append(q.display_values[idx])
                    else:
                        new_display_values.append(opt)

            # Handle option weights
            if new_option_weights is None:
                # If no new weights provided, maintain existing weights for unchanged options
                new_option_weights = []
                for opt in new_opts:
                    if opt in q.options:
                        idx = q.options.index(opt)
                        new_option_weights.append(q.option_weights[idx])
                    else:
                        new_option_weights.append(1.0)  # Default to 1.0 for new options
        # Update only display_text, options, display_values, option_weights, default_option
        q.display_text = new_display_text
        q.options = new_opts
        q.display_values = new_display_values
        q.option_weights = new_option_weights
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
    def get_question_by_text(text: str, session: Session) -> Dict[str, Any]:
        """Get a question by its text.
        
        Args:
            text: Question text
            session: Database session
            
        Returns:
            Dictionary with question data if found
            
        Raises:
            ValueError: If question not found
        """
        question = session.scalar(select(Question).where(Question.text == text))
        if not question:
            raise ValueError(f"Question with text '{text}' not found")
        
        return {
            "id": question.id,
            "text": question.text,
            "display_text": question.display_text,
            "type": question.type,
            "options": question.options,
            "display_values": question.display_values,
            "default_option": question.default_option,
            "option_weights": question.option_weights,
            "created_at": question.created_at,
            "archived": question.is_archived
        }

    @staticmethod
    def get_question_by_text_with_custom_display(text: str, project_id: int, video_id: int, session: Session) -> Dict[str, Any]:
        """Get a question by its text with custom display applied for a specific project-video combination.
        
        Args:
            text: Question text (immutable text field)
            project_id: Project ID (for custom display context)
            video_id: Video ID (for custom display context)
            session: Database session
            
        Returns:
            Dictionary with question data including custom display (same format as original)
            
        Raises:
            ValueError: If question not found
        """
        # First get the question by text to find its ID
        original_question = QuestionService.get_question_by_text(text=text, session=session)
        
        # Then get it with custom display applied
        return CustomDisplayService.get_custom_display(
            question_id=original_question["id"],
            project_id=project_id,
            video_id=video_id,
            session=session
        )

    @staticmethod
    def get_question_by_id(question_id: int, session: Session) -> Dict[str, Any]:
        """Get a question by its ID.
        
        Args:
            question_id: Question ID
            session: Database session
            
        Returns:
            Dictionary with question data if found
            
        Raises:
            ValueError: If question not found
        """
        question = session.get(Question, question_id)
        if not question:
            raise ValueError(f"Question with ID {question_id} not found")
        
        return {
            "id": question.id,
            "text": question.text,
            "display_text": question.display_text,
            "type": question.type,
            "options": question.options,
            "display_values": question.display_values,
            "default_option": question.default_option,
            "option_weights": question.option_weights,
            "created_at": question.created_at,
            "archived": question.is_archived
        }
    
    @staticmethod 
    def get_question_by_id_with_custom_display(question_id: int, project_id: int, video_id: int, session: Session) -> Dict[str, Any]:
        """Get a question by its ID with custom display applied for a specific project-video combination.
        
        Args:
            question_id: Question ID
            project_id: Project ID (for custom display context)
            video_id: Video ID (for custom display context)
            session: Database session
            
        Returns:
            Dictionary with question data including custom display (same format as original)
            
        Raises:
            ValueError: If question not found
        """
        # This directly uses our custom display service which already returns the right format
        return CustomDisplayService.get_custom_display(
            question_id=question_id,
            project_id=project_id,
            video_id=video_id,
            session=session
        )
    
    @staticmethod
    def get_questions_by_group_id(group_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all questions in a group as dictionaries.
        
        Args:
            group_id: The ID of the question group
            session: Database session
        
        Returns:
            List of question dictionaries containing: id, text, display_text, type, options, display_values, default_option
        
        Raises:
            ValueError: If group not found
        """
        # Check if group exists
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == group_id)
            .order_by(QuestionGroupQuestion.display_order)
        ).all()
        
        return [{
            "id": q.id,
            "text": q.text,
            "display_text": q.display_text,
            "type": q.type,
            "options": q.options,
            "option_weights": q.option_weights,
            "display_values": q.display_values,
            "default_option": q.default_option
        } for q in questions]
    
    @staticmethod
    def get_questions_by_group_id_with_custom_display(group_id: int, project_id: int, video_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all questions in a group with custom display applied for a specific project-video combination.
        
        Args:
            group_id: The ID of the question group
            project_id: The ID of the project (for custom display context)
            video_id: The ID of the video (for custom display context)
            session: Database session
        
        Returns:
            List of question dictionaries with custom display applied (same format as original)
        
        Raises:
            ValueError: If group not found
        """
        # Get original questions
        original_questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        
        # Apply custom display to each question
        result = []
        for q in original_questions:
            # Get question with custom display applied
            question_with_custom = CustomDisplayService.get_custom_display(
                question_id=q['id'],
                project_id=project_id,
                video_id=video_id,
                session=session
            )
            
            # Return same format as original but with custom display
            result.append({
                "id": question_with_custom["id"],
                "text": question_with_custom["text"],
                "display_text": question_with_custom["display_text"],  # Custom display applied
                "type": question_with_custom["type"],
                "options": question_with_custom["options"],
                "option_weights": question_with_custom["option_weights"],
                "display_values": question_with_custom["display_values"],  # Custom display applied
                "default_option": question_with_custom["default_option"]
            })
        
        return result

class CustomDisplayService:
    @staticmethod
    def _validate_project_video_question_relationship(project_id: int, video_id: int, question_id: int, session: Session) -> None:
        """Validate that video and question are both in the specified project.
        
        Args:
            project_id: Project ID
            video_id: Video ID  
            question_id: Question ID
            session: Database session
            
        Raises:
            ValueError: If validation fails
        """
        # Check if video is in project
        project_video = session.scalar(
            select(ProjectVideo).where(
                ProjectVideo.project_id == project_id,
                ProjectVideo.video_id == video_id
            )
        )
        if not project_video:
            raise ValueError(f"Video {video_id} is not assigned to project {project_id}")
        
        # Check if question is in project's schema
        project = session.get(Project, project_id)
        question_in_schema = session.scalar(
            select(func.count())
            .select_from(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(
                SchemaQuestionGroup.schema_id == project.schema_id,
                Question.id == question_id
            )
        )
        if not question_in_schema:
            raise ValueError(f"Question {question_id} is not in project {project_id}'s schema")

    @staticmethod
    def verify_set_custom_display(
        project_id: int,
        video_id: int, 
        question_id: int,
        custom_display_text: Optional[str] = None,
        custom_option_display_map: Optional[Dict[str, str]] = None,
        session: Session = None
    ) -> None:
        """Verify parameters for setting custom display.
        
        Args:
            project_id: Project ID
            video_id: Video ID
            question_id: Question ID
            custom_display_text: Custom display text for question
            custom_option_display_map: Dict mapping option values to custom display text
            session: Database session
            
        Raises:
            ValueError: If validation fails
        """
        # Validate project exists and is not archived
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        if project.is_archived:
            raise ValueError(f"Project {project_id} is archived")
        
        # Validate video exists and is not archived
        video = session.get(Video, video_id)
        if not video:
            raise ValueError(f"Video {video_id} not found")
        if video.is_archived:
            raise ValueError(f"Video {video_id} is archived")
        
        # Validate question exists and is not archived
        question_dict = QuestionService.get_question_by_id(question_id=question_id, session=session)
        if question_dict["archived"]:
            raise ValueError(f"Question {question_id} is archived")
        
        reusable_group_count = session.scalar(
            select(func.count())
            .select_from(QuestionGroup)
            .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
            .where(
                QuestionGroupQuestion.question_id == question_id,
                QuestionGroup.is_reusable == True,
                QuestionGroup.is_archived == False
            )
        )

        if reusable_group_count > 0:
            # Get the reusable group names for a better error message
            reusable_groups = session.scalars(
                select(QuestionGroup.title)
                .join(QuestionGroupQuestion, QuestionGroup.id == QuestionGroupQuestion.question_group_id)
                .where(
                    QuestionGroupQuestion.question_id == question_id,
                    QuestionGroup.is_reusable == True,
                    QuestionGroup.is_archived == False
                )
            ).all()
            
            group_names = ", ".join(reusable_groups)
            raise ValueError(
                f"Cannot set custom display for question {question_id}. "
                f"Question belongs to reusable question group(s): {group_names}. "
                f"Reusable groups must maintain consistent display across all schemas."
            )
        
        # Validate project-video-question relationship
        CustomDisplayService._validate_project_video_question_relationship(
            project_id=project_id, video_id=video_id, question_id=question_id, session=session
        )
        
        # Check that schema has custom display enabled
        schema = session.get(Schema, project.schema_id)
        if not schema.has_custom_display:
            raise ValueError(f"Schema {schema.id} does not have custom display enabled")
        
        # For single-choice questions with custom option mapping
        if custom_option_display_map is not None:
            if question_dict["type"] != "single":
                raise ValueError(f"Custom option display mapping can only be set for single-choice questions, not {question_dict['type']}")
            
            if not question_dict["options"]:
                raise ValueError(f"Question {question_id} has no options defined")
            
            # Check that all keys in the mapping are valid option values
            valid_options = set(question_dict["options"])
            invalid_options = set(custom_option_display_map.keys()) - valid_options
            if invalid_options:
                raise ValueError(f"Invalid option values in custom mapping: {invalid_options}. Valid options: {valid_options}")

    @staticmethod
    def set_custom_display(
        project_id: int,
        video_id: int, 
        question_id: int,
        custom_display_text: Optional[str] = None,
        custom_option_display_map: Optional[Dict[str, str]] = None,
        session: Session = None
    ) -> None:
        """Set custom display for a specific project-video-question combination.
        
        Args:
            project_id: Project ID
            video_id: Video ID
            question_id: Question ID
            custom_display_text: Custom display text for question
            custom_option_display_map: Dict mapping option values to custom display text
            session: Database session
            
        Raises:
            ValueError: If validation fails
        """
        # Verify all parameters
        CustomDisplayService.verify_set_custom_display(
            project_id=project_id,
            video_id=video_id,
            question_id=question_id,
            custom_display_text=custom_display_text,
            custom_option_display_map=custom_option_display_map,
            session=session
        )
        
        # Get or create custom display entry
        custom_display = session.get(
            ProjectVideoQuestionDisplay,
            (project_id, video_id, question_id)
        )
        
        if custom_display:
            # Update existing entry
            if custom_display_text is not None:
                custom_display.custom_display_text = custom_display_text
            if custom_option_display_map is not None:
                custom_display.custom_option_display_map = custom_option_display_map
            custom_display.updated_at = datetime.now(timezone.utc)
        else:
            # Create new entry
            custom_display = ProjectVideoQuestionDisplay(
                project_id=project_id,
                video_id=video_id,
                question_id=question_id,
                custom_display_text=custom_display_text,
                custom_option_display_map=custom_option_display_map
            )
            session.add(custom_display)
        
        session.commit()

    @staticmethod
    def get_custom_display(
        question_id: int, 
        project_id: int, 
        video_id: int, 
        session: Session
    ) -> Dict[str, Any]:
        """Get question display data with custom display if applicable.
        
        Args:
            question_id: Question ID
            project_id: Project ID
            video_id: Video ID
            session: Database session
            
        Returns:
            Dictionary with question data including custom display if applicable
            
        Raises:
            ValueError: If question, project, or video not found
        """
        # Get the original question
        question_dict = QuestionService.get_question_by_id(question_id=question_id, session=session)
        
        # Get the project to check if custom display is enabled
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        
        # Get the schema to check custom display flag
        schema = session.get(Schema, project.schema_id)
        if not schema or not schema.has_custom_display:
            # No custom display enabled - return original data
            return question_dict
        
        # Try to get custom display entries
        custom_display = session.get(
            ProjectVideoQuestionDisplay, 
            (project_id, video_id, question_id)
        )
        
        # Start with original question data
        result = question_dict.copy()
        
        if custom_display:
            # Override display text if available
            if custom_display.custom_display_text:
                result["display_text"] = custom_display.custom_display_text
            
            # Override option display values if available
            if (custom_display.custom_option_display_map and 
                question_dict["type"] == "single" and 
                question_dict["options"]):
                
                # Build new display_values array by mapping original options
                new_display_values = []
                for option_value in question_dict["options"]:
                    custom_display_text = custom_display.custom_option_display_map.get(option_value)
                    if custom_display_text:
                        new_display_values.append(custom_display_text)
                    else:
                        # Fall back to original display value
                        original_index = question_dict["options"].index(option_value)
                        original_display = (question_dict["display_values"][original_index] 
                                          if question_dict["display_values"] else option_value)
                        new_display_values.append(original_display)
                
                result["display_values"] = new_display_values
        
        return result

    @staticmethod
    def remove_custom_display(
        project_id: int,
        video_id: int,
        question_id: int,
        session: Session
    ) -> bool:
        """Remove custom display for a specific project-video-question combination.
        
        Args:
            project_id: Project ID
            video_id: Video ID
            question_id: Question ID
            session: Database session
            
        Returns:
            True if overrides were removed, False if none existed
        """
        custom_display = session.get(
            ProjectVideoQuestionDisplay,
            (project_id, video_id, question_id)
        )
        
        if custom_display:
            session.delete(custom_display)
            session.commit()
            return True
        
        return False

    @staticmethod
    def get_all_custom_displays_for_video(
        project_id: int,
        video_id: int,
        session: Session
    ) -> List[Dict[str, Any]]:
        """Get all questions that have custom display for a specific project-video combination.
        
        Args:
            project_id: Project ID
            video_id: Video ID
            session: Database session
            
        Returns:
            List of dictionaries with custom display information
        """
        overrides = session.scalars(
            select(ProjectVideoQuestionDisplay)
            .where(
                ProjectVideoQuestionDisplay.project_id == project_id,
                ProjectVideoQuestionDisplay.video_id == video_id
            )
        ).all()
        
        result = []
        for override in overrides:
            question_dict = QuestionService.get_question_by_id(question_id=override.question_id, session=session)
            result.append({
                "question_id": override.question_id,
                "question_text": question_dict["text"],
                "has_custom_text": override.custom_display_text is not None,
                "has_custom_options": override.custom_option_display_map is not None,
                "custom_display_text": override.custom_display_text,
                "custom_option_display_map": override.custom_option_display_map
            })
        
        return result

    @staticmethod
    def get_all_custom_displays_for_project(
        project_id: int,
        session: Session
    ) -> List[Dict[str, Any]]:
        """Get all custom display entries for a project.
        
        Args:
            project_id: Project ID
            session: Database session
            
        Returns:
            List of dictionaries with all custom display overrides
        """
        overrides = session.scalars(
            select(ProjectVideoQuestionDisplay)
            .where(ProjectVideoQuestionDisplay.project_id == project_id)
        ).all()
        
        result = []
        for override in overrides:
            video = session.get(Video, override.video_id)
            question_dict = QuestionService.get_question_by_id(question_id=override.question_id, session=session)
            
            result.append({
                "project_id": override.project_id,
                "video_id": override.video_id,
                "video_uid": video.video_uid if video else None,
                "question_id": override.question_id,
                "question_text": question_dict["text"],
                "custom_display_text": override.custom_display_text,
                "custom_option_display_map": override.custom_option_display_map,
                "created_at": override.created_at,
                "updated_at": override.updated_at
            })
        
        return result


class AuthService:
    @staticmethod
    def get_user_display_name_with_initials(user_name: str) -> tuple[str, str]:
        """Get user display name with initials.
        
        Args:
            user_name: The user's name/ID string
            
        Returns:
            Tuple of (display_name_with_initials, initials_only)
            
        Example:
            ("alice_annotator (AL)", "AL")
        """
        name_parts = user_name.split()
        if len(name_parts) >= 2:
            initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
        else:
            initials = user_name[:2].upper()
        
        display_name = f"{user_name} ({initials})"
        return display_name, initials

    @staticmethod
    def get_user_info_by_id(user_id: int, session: Session) -> Dict[str, Any]:
        """Get user information by numeric ID as a dictionary.
        
        Args:
            user_id: The numeric ID of the user
            session: Database session
            
        Returns:
            Dictionary containing user info: id, user_id_str, email, user_type, is_archived
            
        Raises:
            ValueError: If user not found
        """
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        return {
            "id": user.id,
            "user_id_str": user.user_id_str,
            "email": user.email,
            "user_type": user.user_type,
            "is_archived": user.is_archived,
            "created_at": user.created_at
        }
    
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
        if not email:
            raise ValueError("Email is required")
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
    def seed_admin(session: Session, email: str = "admin@example.com", password: str = "password123", user_id: str = "Admin") -> None:
        """Create admin user with specified credentials if not present.
        
        Args:
            session: Database session
            email: Admin email address
            password: Admin password  
            user_id: Admin user ID string
        """
        try:
            # Use create_user which handles all validation and duplicate checking
            AuthService.create_user(
                user_id=user_id,
                email=email,
                password_hash=password,
                user_type="admin",
                session=session
            )
        except ValueError:
            # User already exists, that's fine for seeding
            pass

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
            
            # Assign user as admin to each project using KEYWORD ARGUMENTS
            for project in projects:
                ProjectService.add_user_to_project(
                    project_id=project.id, 
                    user_id=user_id, 
                    role="admin", 
                    session=session
                )
        else:
            user.user_type = new_role
            session.commit()
    
    @staticmethod
    def get_user_weights_for_project(project_id: int, session: Session) -> Dict[int, float]:
        """Get user weights for a specific project.
        
        Args:
            project_id: Project ID
            session: Database session
            
        Returns:
            Dictionary mapping user_id to user_weight
            
        Raises:
            ValueError: If error getting user weights
        """
        try:
            assignments_df = AuthService.get_project_assignments(session=session)
            project_assignments = assignments_df[assignments_df["Project ID"] == project_id]
            
            user_weights = {}
            for _, assignment in project_assignments.iterrows():
                user_id = assignment["User ID"]
                weight = assignment.get("User Weight", 1.0)
                user_weights[user_id] = weight
            
            return user_weights
        except Exception as e:
            raise ValueError(f"Error getting user weights for project: {str(e)}")


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
        
        # Single query with joins instead of individual gets
        query = select(
            ProjectUserRole.project_id,
            Project.name.label('project_name'),
            ProjectUserRole.user_id,
            User.user_id_str.label('user_name'),
            ProjectUserRole.role,
            ProjectUserRole.is_archived,
            ProjectUserRole.assigned_at,
            ProjectUserRole.completed_at,
            ProjectUserRole.user_weight
        ).select_from(
            ProjectUserRole
        ).join(
            Project, ProjectUserRole.project_id == Project.id
        ).join(
            User, ProjectUserRole.user_id == User.id
        ).where(
            ProjectUserRole.is_archived == False
        )
        
        result = session.execute(query).all()
        
        return pd.DataFrame([
            {
                "Project ID": row.project_id,
                "Project Name": row.project_name,
                "User ID": row.user_id,
                "User Name": row.user_name,
                "Role": row.role,
                "Archived": row.is_archived,
                "Assigned At": row.assigned_at,
                "Completed At": row.completed_at,
                "User Weight": row.user_weight
            }
            for row in result
        ])

    @staticmethod
    def get_project_assignments_old(session: Session) -> pd.DataFrame:
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
                "Completed At": a.completed_at,
                "User Weight": a.user_weight
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
        
        # Check if user already exists - handle model users differently
        if user_type == "model":
            # For model users, only check user_id_str since all model users have email=None
            existing_user = session.scalar(
                select(User).where(User.user_id_str == user_id)
            )
        else:
            # For human and admin users, check both user_id_str and email
            existing_user = session.scalar(
                select(User).where(
                    (User.user_id_str == user_id) | 
                    (User.email == email)
                )
            )
        
        if existing_user:
            if user_type == "model":
                raise ValueError(f"Model user with ID '{user_id}' already exists")
            else:
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
        
        # If user is admin, assign to all existing projects using KEYWORD ARGUMENTS
        if user_type == "admin" and not is_archived:
            projects = session.scalars(
                select(Project).where(Project.is_archived == False)
            ).all()
            
            for project in projects:
                ProjectService.add_user_to_project(
                    project_id=project.id, 
                    user_id=user.id, 
                    role="admin", 
                    session=session
                )
        
        session.commit()
        return user

    @staticmethod
    def assign_user_to_project(user_id: int, project_id: int, role: str, session: Session, user_weight: Optional[float] = None) -> None:
        """Assign a user to a project with role validation and admin privileges.
        
        Args:
            user_id: The ID of the user
            project_id: The ID of the project
            role: The role to assign ('annotator', 'reviewer', 'admin', or 'model')
            session: Database session
            user_weight: Optional weight for the user's answers (defaults to 1.0)
            
        Raises:
            ValueError: If user or project not found, or if role is invalid
        """
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
            existing.user_weight = user_weight if user_weight is not None else 1.0
        else:
            assignment = ProjectUserRole(
                project_id=project_id,
                user_id=user_id,
                role=role,
                user_weight=user_weight if user_weight is not None else 1.0,
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
                AuthService.assign_user_to_project(
                    user_id=user_id, 
                    project_id=project_id, 
                    role=role, 
                    session=session
                )
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
    
    @staticmethod
    def get_annotator_user_ids_from_display_names(display_names: List[str], project_id: int, session: Session) -> List[int]:
        """Convert display names to user IDs for annotators in a project.
        
        Args:
            display_names: List of display names like "John Doe (JD)"
            project_id: The ID of the project
            session: Database session
            
        Returns:
            List of unique user IDs (no duplicates)
        """
        # Remove duplicates from input list first
        unique_display_names = list(dict.fromkeys(display_names))
        
        user_ids = []
        
        # Get all annotators in project
        annotators = session.scalars(
            select(User)
            .join(ProjectUserRole, User.id == ProjectUserRole.user_id)
            .where(
                ProjectUserRole.project_id == project_id,
                ProjectUserRole.role.in_(["annotator", "reviewer", "model", "admin"]),
                ProjectUserRole.is_archived == False,
                User.is_archived == False
            )
            .distinct()  # Ensure no duplicate users
        ).all()
        
        # Create mapping from display names to user IDs
        for annotator in annotators:
            name_parts = annotator.user_id_str.split()
            if len(name_parts) >= 2:
                initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
            else:
                initials = annotator.user_id_str[:2].upper()
            
            display_name = f"{annotator.user_id_str} ({initials})"
            if display_name in unique_display_names:
                user_ids.append(annotator.id)
        
        # Return unique user IDs only
        return list(set(user_ids))
    
    @staticmethod
    def get_user_projects_by_role(user_id: int, session: Session) -> Dict[str, List[Dict[str, Any]]]:
        """Get projects assigned to user by role.
        
        Args:
            user_id: The ID of the user
            session: Database session
            
        Returns:
            Dictionary with role keys containing lists of project info dicts
        """
        assignments = session.scalars(
            select(ProjectUserRole)
            .join(Project, ProjectUserRole.project_id == Project.id)
            .where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.is_archived == False,
                Project.is_archived == False
            )
        ).all()
        
        projects = {"annotator": [], "reviewer": [], "admin": []}
        for assignment in assignments:
            project = session.get(Project, assignment.project_id)
            if project and not project.is_archived:
                projects[assignment.role].append({
                    "id": project.id,
                    "name": project.name,
                    "description": project.description
                })
        
        return projects


class QuestionGroupService:
    @staticmethod
    def get_group_verification_function(group_id: int, session: Session) -> Optional[str]:
        """Get the verification function for a question group.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            Verification function name or None if not set
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        return group.verification_function

    @staticmethod
    def get_group_details_with_verification(group_id: int, session: Session) -> dict:
        """Get complete details of a question group including verification function.
        
        Args:
            group_id: Group ID
            session: Database session
            
        Returns:
            Dictionary containing all group details including verification_function
            
        Raises:
            ValueError: If group not found
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        return {
            "title": group.title,
            "display_title": group.display_title,
            "description": group.description,
            "is_reusable": group.is_reusable,
            "is_auto_submit": group.is_auto_submit,
            "is_archived": group.is_archived,
            "verification_function": group.verification_function
        }
    
    
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
            - Auto Submit: Whether the group is auto-submit
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
                "Display Title": g.display_title,
                "Description": g.description,
                "Questions": "\n".join(question_list) if question_list else "No questions",
                "Reusable": g.is_reusable,
                "Auto Submit": g.is_auto_submit,
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
                "Display Text": q.display_text,
                "Type": q.type,
                "Options": ", ".join(q.options or []) if q.options else "",
                "Default": q.default_option or "",
                "Archived": q.is_archived
            }
            for q in questions
        ])

    @staticmethod
    def get_group_questions_with_custom_display(group_id: int, project_id: int, video_id: int, session: Session) -> pd.DataFrame:
        """Get all questions in a group with custom display applied for a specific project-video combination.
        
        Args:
            group_id: Group ID
            project_id: Project ID (for custom display context)
            video_id: Video ID (for custom display context)
            session: Database session
            
        Returns:
            DataFrame containing questions with custom display applied (same columns as original)
            
        Raises:
            ValueError: If group not found
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
        
        rows = []
        for q in questions:
            # Get custom display data
            question_with_custom = CustomDisplayService.get_custom_display(
                question_id=q.id,
                project_id=project_id,
                video_id=video_id,
                session=session
            )
            
            # Same format as original but with custom display
            rows.append({
                "ID": question_with_custom["id"],
                "Text": question_with_custom["text"],
                "Display Text": question_with_custom["display_text"],  # Custom display applied
                "Type": question_with_custom["type"],
                "Options": ", ".join(question_with_custom["options"] or []) if question_with_custom["options"] else "",
                "Display Values": ", ".join(question_with_custom["display_values"] or []) if question_with_custom["display_values"] else "",  # Custom display applied
                "Default": question_with_custom["default_option"] or "",
                "Archived": question_with_custom["archived"]
            })
            
        return pd.DataFrame(rows)
    
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
            "display_title": group.display_title,
            "description": group.description,
            "is_reusable": group.is_reusable,
            "is_auto_submit": group.is_auto_submit,
            "is_archived": group.is_archived,
            "verification_function": group.verification_function
        }

    @staticmethod
    def set_group_verification_function(group_id: int, verification_function: Optional[str], session: Session) -> None:
        """Set the verification function for a question group.
        
        Args:
            group_id: Group ID
            verification_function: Function name or None to remove
            session: Database session
            
        Raises:
            ValueError: If group not found or function invalid
        """
        group = session.get(QuestionGroup, group_id)
        if not group:
            raise ValueError(f"Question group with ID {group_id} not found")
        
        # Validate verification function if provided
        if verification_function:
            if not hasattr(verify, verification_function):
                raise ValueError(f"Verification function '{verification_function}' not found in verify.py")
        
        group.verification_function = verification_function
        session.commit()

    @staticmethod
    def verify_create_group(
            title: str,
            display_title: str,
            description: str,
            is_reusable: bool,
            question_ids: List[int],
            verification_function: Optional[str],
            is_auto_submit: bool = False,
            session: Session = None
    ) -> None:
        """Verify parameters for creating a new question group.

        This function performs all validation checks and raises ValueError
        if any validation fails. It does not return any objects.

        Args:
            title: Group title
            description: Group description
            is_reusable: Whether group can be used in multiple schemas
            question_ids: List of question IDs in desired order
            verification_function: Optional name of verification function from verify.py
            is_auto_submit: If TRUE, answers are automatically submitted for annotation mode
            session: Database session

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
        
        if is_reusable:
            questions_with_custom_displays = session.scalar(
                select(func.count())
                .select_from(ProjectVideoQuestionDisplay)
                .where(ProjectVideoQuestionDisplay.question_id.in_(question_ids))
            )
            
            if questions_with_custom_displays > 0:
                raise ValueError(
                    f"Cannot create reusable question group. {questions_with_custom_displays} questions "
                    f"already have custom displays. Reusable groups must maintain consistent display."
                )

        # Validate verification function if provided
        if verification_function:
            if not hasattr(verify, verification_function):
                raise ValueError(f"Verification function '{verification_function}' not found in verify.py")

        # Validate all questions exist and aren't archived
        for question_id in question_ids:
            question = session.scalar(select(Question).where(Question.id == question_id))
            if not question:
                raise ValueError(f"Question with ID {question_id} not found")
            if question.is_archived:
                raise ValueError(f"Question with ID {question_id} is archived")

    @staticmethod
    def create_group(
            title: str,
            display_title: str,
            description: str,
            is_reusable: bool,
            question_ids: List[int],
            verification_function: Optional[str],
            is_auto_submit: bool = False,
            session: Session = None
    ) -> QuestionGroup:
        """Create a new question group.

        Args:
            title: Group title (unique, immutable)
            display_title: Group display title (can be changed)
            description: Group description
            is_reusable: Whether group can be used in multiple schemas
            question_ids: List of question IDs in desired order
            verification_function: Optional name of verification function from verify.py
            is_auto_submit: If TRUE, answers are automatically submitted for annotation mode
            session: Database session

        Returns:
            Created QuestionGroup

        Raises:
            ValueError: If title already exists or validation fails
        """
        if display_title is None:
            display_title = title
        
        # First, verify all parameters (will raise ValueError if validation fails)
        QuestionGroupService.verify_create_group(
            title, display_title, description, is_reusable, question_ids,
            verification_function, is_auto_submit, session
        )

        # Create group object
        group = QuestionGroup(
            title=title,
            display_title=display_title,
            description=description,
            is_reusable=is_reusable,
            verification_function=verification_function,
            is_auto_submit=is_auto_submit
        )

        session.add(group)
        session.flush()  # Get the group ID

        # Add questions to group in specified order
        for i, question_id in enumerate(question_ids):
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
    def get_available_verification_functions() -> List[str]:
        """Get all available verification functions from verify.py.
        
        Returns:
            List of function names that can be used for verification
        """
        import inspect
        
        # Get all functions from verify module
        functions = []
        for name, obj in inspect.getmembers(verify):
            if (inspect.isfunction(obj) and 
                not name.startswith('_') and  # Exclude private functions
                name != 'verify'):  # Exclude the module name itself if it exists
                functions.append(name)
        
        return sorted(functions)

    @staticmethod
    def get_verification_function_info(function_name: str) -> Dict[str, Any]:
        """Get information about a specific verification function.
        
        Args:
            function_name: Name of the verification function
            
        Returns:
            Dictionary containing function info: name, docstring, signature
            
        Raises:
            ValueError: If function not found
        """
        import inspect
        
        if not hasattr(verify, function_name):
            raise ValueError(f"Verification function '{function_name}' not found in verify.py")
        
        func = getattr(verify, function_name)
        
        return {
            "name": function_name,
            "docstring": inspect.getdoc(func) or "No documentation available",
            "signature": str(inspect.signature(func)),
            "parameters": list(inspect.signature(func).parameters.keys())
        }

    @staticmethod
    def verify_edit_group(
            group_id: int,
            new_display_title: str,
            new_description: str,
            is_reusable: bool,
            verification_function: Optional[str],
            is_auto_submit: bool = False,
            session: Session = None
    ) -> None:
        """Verify parameters for editing a question group.

        Args:
            group_id: Group ID
            new_display_title: New group display title
            new_description: New group description
            is_reusable: Whether the group is reusable
            verification_function: New verification function name (can be None to remove)
            is_auto_submit: If not None, sets the auto-submit flag
            session: Database session

        Raises:
            ValueError: If group not found or validation fails
        """
        # Get and validate group exists
        group = QuestionGroupService.get_group_by_id(group_id, session)
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
        
        if is_reusable and not group.is_reusable:  # Making group reusable
            # Check if any questions in this group have custom displays
            custom_displays = session.scalar(
                select(func.count())
                .select_from(ProjectVideoQuestionDisplay)
                .join(QuestionGroupQuestion, ProjectVideoQuestionDisplay.question_id == QuestionGroupQuestion.question_id)
                .where(QuestionGroupQuestion.question_group_id == group_id)
            )
            
            if custom_displays > 0:
                raise ValueError(
                    f"Cannot make group reusable. {custom_displays} questions in this group "
                    f"have existing custom displays that would conflict with reusability."
                )

        # Check if new title conflicts with existing group
        if new_display_title != group.display_title:
            existing = session.scalar(
                select(QuestionGroup).where(QuestionGroup.display_title == new_display_title)
            )
            if existing:
                raise ValueError(f"Question group with display title '{new_display_title}' already exists")

        # Validate verification function if provided
        if verification_function:
            if not hasattr(verify, verification_function):
                raise ValueError(f"Verification function '{verification_function}' not found in verify.py")

    @staticmethod
    def edit_group(group_id: int, new_display_title: str, new_description: str, is_reusable: bool,
                   verification_function: Optional[str], is_auto_submit: bool = False, session: Session = None) -> None:
        """Edit a question group including its verification function and auto-submit flag.

        Args:
            group_id: Group ID
            new_display_title: New group display title
            new_description: New group description
            is_reusable: Whether the group is reusable
            verification_function: New verification function name (can be None to remove)
            is_auto_submit: If not None, sets the auto-submit flag
            session: Database session

        Raises:
            ValueError: If group not found or validation fails
        """
        # Verify parameters
        QuestionGroupService.verify_edit_group(
            group_id, new_display_title, new_description, is_reusable,
            verification_function, is_auto_submit, session
        )

        # Get group object for updating
        group = QuestionGroupService.get_group_by_id(group_id, session)

        # Update group properties
        group.display_title = new_display_title
        group.description = new_description
        group.is_reusable = is_reusable
        group.verification_function = verification_function  # This can be None to remove verification
        group.is_auto_submit = is_auto_submit
        session.commit()

    @staticmethod
    def test_verification_function(function_name: str, test_answers: Dict[str, str]) -> Dict[str, Any]:
        """Test a verification function with sample answers.
        
        Args:
            function_name: Name of the verification function
            test_answers: Dictionary of test answers to validate
            
        Returns:
            Dictionary containing test results: success (bool), error_message (str or None)
        """
        if not hasattr(verify, function_name):
            return {
                "success": False,
                "error_message": f"Verification function '{function_name}' not found in verify.py"
            }
        
        try:
            verify_func = getattr(verify, function_name)
            verify_func(test_answers)
            return {
                "success": True,
                "error_message": None
            }
        except Exception as e:
            return {
                "success": False,
                "error_message": str(e)
            }

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
            'annotator': ['annotator', 'reviewer', 'model', 'admin'],
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
        project, user = AnnotatorService._validate_project_and_user(project_id=project_id, user_id=user_id, session=session)
        
        # Validate user role
        AnnotatorService._validate_user_role(user_id=user_id, project_id=project_id, required_role="annotator", session=session)
            
        # Validate question group and get questions
        group, questions = AnnotatorService._validate_question_group(question_group_id=question_group_id, session=session)
        
        # Validate answers match questions
        AnnotatorService._validate_answers_match_questions(answers=answers, questions=questions)
            
        # Run verification if specified
        AnnotatorService._run_verification(group=group, answers=answers)
            
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
            AnnotatorService._validate_answer_value(question=question, answer_value=answer_value)
            
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
        AnnotatorService._check_and_update_completion(user_id=user_id, project_id=project_id, session=session)

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
    
    @staticmethod
    def get_user_answers_for_question_group(video_id: int, project_id: int, user_id: int, question_group_id: int, session: Session) -> Dict[str, str]:
        """Get user's existing answers for a video and question group as a dictionary.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            user_id: The ID of the user
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            Dictionary mapping question text to answer value
        """
        answers = session.scalars(
            select(AnnotatorAnswer)
            .join(Question, AnnotatorAnswer.question_id == Question.id)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(
                AnnotatorAnswer.video_id == video_id,
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.user_id == user_id,
                QuestionGroupQuestion.question_group_id == question_group_id
            )
        ).all()
        
        result = {}
        for answer in answers:
            question = session.get(Question, answer.question_id)
            if question:
                result[question.text] = answer.answer_value
        
        return result

    @staticmethod
    def check_user_has_submitted_answers(video_id: int, project_id: int, user_id: int, question_group_id: int, session: Session) -> bool:
        """Check if user has submitted any answers for a question group.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            user_id: The ID of the user
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            True if user has submitted answers, False otherwise
        """
        answer_count = session.scalar(
            select(func.count())
            .select_from(AnnotatorAnswer)
            .join(Question, AnnotatorAnswer.question_id == Question.id)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(
                AnnotatorAnswer.video_id == video_id,
                AnnotatorAnswer.project_id == project_id,
                AnnotatorAnswer.user_id == user_id,
                QuestionGroupQuestion.question_group_id == question_group_id
            )
        )
        
        return answer_count > 0
    
    @staticmethod
    def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
        """Calculate user's overall progress across all question groups in a project.
        
        Args:
            user_id: The ID of the user
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Progress percentage (0-100)
            
        Raises:
            ValueError: If project or user not found
        """
        # Validate project and user
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User with ID {user_id} not found")
        
        # Get total questions across all groups in schema
        total_questions = session.scalar(
            select(func.count())
            .select_from(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(
                SchemaQuestionGroup.schema_id == project.schema_id,
                Question.is_archived == False
            )
        )
        
        # Get total videos in project
        total_videos = session.scalar(
            select(func.count())
            .select_from(ProjectVideo)
            .join(Video, ProjectVideo.video_id == Video.id)
            .where(
                ProjectVideo.project_id == project_id,
                Video.is_archived == False
            )
        )
        
        # Get user's total answers
        answered = session.scalar(
            select(func.count())
            .select_from(AnnotatorAnswer)
            .join(Question, AnnotatorAnswer.question_id == Question.id)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .join(SchemaQuestionGroup, QuestionGroupQuestion.question_group_id == SchemaQuestionGroup.question_group_id)
            .where(
                AnnotatorAnswer.user_id == user_id,
                AnnotatorAnswer.project_id == project_id,
                SchemaQuestionGroup.schema_id == project.schema_id,
                Question.is_archived == False
            )
        )
        
        total_possible = total_questions * total_videos
        return (answered / total_possible * 100) if total_possible > 0 else 0.0
    
    @staticmethod
    def get_all_user_answers_for_question_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> List[Dict[str, Any]]:
        """Get all users' answers for a video and question group.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            List of answer dictionaries
        """
        answers = session.scalars(
            select(AnnotatorAnswer)
            .join(Question, AnnotatorAnswer.question_id == Question.id)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(
                AnnotatorAnswer.video_id == video_id,
                AnnotatorAnswer.project_id == project_id,
                QuestionGroupQuestion.question_group_id == question_group_id
            )
        ).all()
        
        result = []
        for answer in answers:
            question = session.get(Question, answer.question_id)
            if question:
                result.append({
                    "user_id": answer.user_id,
                    "question_id": question.id,
                    "question_text": question.text,
                    "answer_value": answer.answer_value,
                    "created_at": answer.created_at,
                    "modified_at": answer.modified_at
                })
        
        return result

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
        project, reviewer = GroundTruthService._validate_project_and_user(project_id=project_id, user_id=reviewer_id, session=session)
        
        # Validate reviewer role
        GroundTruthService._validate_user_role(user_id=reviewer_id, project_id=project_id, required_role="reviewer", session=session)
            
        # Validate question group and get questions
        group, questions = GroundTruthService._validate_question_group(question_group_id=question_group_id, session=session)
        
        # Validate answers match questions
        GroundTruthService._validate_answers_match_questions(answers=answers, questions=questions)
            
        # Run verification if specified
        GroundTruthService._run_verification(group=group, answers=answers)
            
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
            GroundTruthService._validate_answer_value(question=question, answer_value=answer_value)
            
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
        GroundTruthService._check_and_update_completion(user_id=reviewer_id, project_id=project_id, session=session)

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
    def get_ground_truth_for_question(video_id: int, project_id: int, question_id: int, session: Session) -> Optional[Dict]:
        """Get ground truth for a single question, returns None if no ground truth exists."""
        gt = session.scalar(
            select(ReviewerGroundTruth)
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id == question_id
            )
        )
        
        if not gt:
            return None
            
        return {
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

    @staticmethod
    def get_ground_truth_for_question_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> pd.DataFrame:
        """Get ground truth for all questions in a specific question group."""
        
        # Use existing service method to get questions
        try:
            questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
            if not questions:
                return pd.DataFrame()
            
            question_ids = [q["id"] for q in questions]
            
            # Get ground truth for these specific questions
            gts = session.scalars(
                select(ReviewerGroundTruth)
                .where(
                    ReviewerGroundTruth.video_id == video_id,
                    ReviewerGroundTruth.project_id == project_id,
                    ReviewerGroundTruth.question_id.in_(question_ids)
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
        except Exception as e:
            print(f"Error in get_ground_truth_for_question_group: {e}")
            return pd.DataFrame()

    @staticmethod
    def check_ground_truth_exists_for_question(video_id: int, project_id: int, question_id: int, session: Session) -> bool:
        """Check if ground truth exists for a single question (most efficient for existence checks)."""
        return session.scalar(
            select(func.count(ReviewerGroundTruth.question_id))
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id == question_id
            )
        ) > 0

    # @staticmethod
    # TODO: See if we want to use this instead
    # def check_question_modified_by_admin_optimized(video_id: int, project_id: int, question_id: int, session: Session) -> bool:
    #     """Optimized version that doesn't fetch full ground truth data."""
    #     return session.scalar(
    #         select(func.count(ReviewerGroundTruth.question_id))
    #         .where(
    #             ReviewerGroundTruth.video_id == video_id,
    #             ReviewerGroundTruth.project_id == project_id,
    #             ReviewerGroundTruth.question_id == question_id,
    #             ReviewerGroundTruth.modified_by_admin_id.is_not(None)
    #         )
    #     ) > 0

    @staticmethod
    def get_ground_truth_for_question(video_id: int, project_id: int, question_id: int, session: Session) -> Optional[Dict]:
        """Get ground truth answer for a specific question on a video.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project  
            question_id: The ID of the specific question
            session: Database session
            
        Returns:
            Dictionary with ground truth data for the question, or None if not found
        """
        gt = session.scalar(
            select(ReviewerGroundTruth)
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id == question_id
            )
        )
        
        if not gt:
            return None
            
        return {
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

    @staticmethod
    def get_reviewer_accuracy(project_id: int, session: Session) -> Dict[int, Dict[int, Dict[str, int]]]:
        """Get accuracy data for all reviewers in a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Dictionary: {reviewer_id: {question_id: {"total": int, "correct": int}}}
            A question is correct if it was NOT modified by admin.
            If no ground truth exists for a question, both total and correct are 0.
            
        Raises:
            ValueError: If project not found
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Get all reviewers and questions
        reviewers = ProjectService.get_project_reviewers(project_id, session)
        questions = ProjectService.get_project_questions(project_id, session)
        
        # Initialize result structure
        accuracy_data = {}
        for reviewer in reviewers:
            reviewer_id = reviewer['id']
            accuracy_data[reviewer_id] = {}
            for question in questions:
                question_id = question['id']
                accuracy_data[reviewer_id][question_id] = {"total": 0, "correct": 0}
        
        # Get all ground truth answers in the project
        ground_truths = session.scalars(
            select(ReviewerGroundTruth)
            .where(ReviewerGroundTruth.project_id == project_id)
        ).all()
        
        # Process each ground truth answer
        for gt in ground_truths:
            reviewer_id = gt.reviewer_id
            question_id = gt.question_id
            
            # Skip if reviewer or question not in our tracking
            if reviewer_id not in accuracy_data or question_id not in accuracy_data[reviewer_id]:
                continue
                
            # Count total
            accuracy_data[reviewer_id][question_id]["total"] += 1
            
            # Count correct (not modified by admin)
            if gt.modified_by_admin_id is None:
                accuracy_data[reviewer_id][question_id]["correct"] += 1
        
        return accuracy_data

    @staticmethod
    def get_annotator_accuracy(project_id: int, session: Session) -> Dict[int, Dict[int, Dict[str, int]]]:
        """Get accuracy data for all annotators in a project.
        
        Args:
            project_id: The ID of the project
            session: Database session
            
        Returns:
            Dictionary: {annotator_id: {question_id: {"total": int, "correct": int}}}
            For single choice: correct if answer matches ground truth
            For description: only count if reviewed (not pending), correct if approved
            
        Raises:
            ValueError: If project not found, no videos, no answers, or incomplete ground truth
        """
        # Validate project exists
        project = session.get(Project, project_id)
        if not project:
            raise ValueError(f"Project with ID {project_id} not found")
        
        # Check if project has complete ground truth
        if not ProjectService.check_project_has_full_ground_truth(project_id, session):
            raise ValueError(f"Project {project_id} does not have complete ground truth")
        
        # Get all videos in project
        videos = session.scalars(
            select(ProjectVideo.video_id)
            .join(Video, ProjectVideo.video_id == Video.id)
            .where(
                ProjectVideo.project_id == project_id,
                Video.is_archived == False
            )
        ).all()
        
        if not videos:
            raise ValueError(f"No videos found in project {project_id}")
        
        # Get all annotator answers
        annotator_answers = session.scalars(
            select(AnnotatorAnswer)
            .where(AnnotatorAnswer.project_id == project_id)
        ).all()
        
        if not annotator_answers:
            raise ValueError(f"No annotator answers found in project {project_id}")
        
        # Get annotators and questions
        annotators = ProjectService.get_project_annotators(project_id, session)
        questions = ProjectService.get_project_questions(project_id, session)
        
        # Initialize result structure
        accuracy_data = {}
        for display_name, annotator_info in annotators.items():
            annotator_id = annotator_info['id']
            accuracy_data[annotator_id] = {}
            for question in questions:
                question_id = question['id']
                accuracy_data[annotator_id][question_id] = {"total": 0, "correct": 0}
        
        # Process each annotator answer
        for answer in annotator_answers:
            annotator_id = answer.user_id
            question_id = answer.question_id
            video_id = answer.video_id
            
            # Skip if annotator or question not in our tracking
            if annotator_id not in accuracy_data or question_id not in accuracy_data[annotator_id]:
                continue
            
            # Get question details
            question = session.get(Question, question_id)
            if not question:
                continue
                
            if question.type == "single":
                # For single choice questions
                # Get ground truth
                gt = session.get(ReviewerGroundTruth, (video_id, question_id, project_id))
                if gt:
                    accuracy_data[annotator_id][question_id]["total"] += 1
                    if answer.answer_value == gt.answer_value:
                        accuracy_data[annotator_id][question_id]["correct"] += 1
                        
            elif question.type == "description":
                # For description questions, check review status
                review = session.scalar(
                    select(AnswerReview)
                    .where(AnswerReview.answer_id == answer.id)
                )
                
                # Only count if reviewed (not pending or missing)
                if review and review.status != "pending":
                    accuracy_data[annotator_id][question_id]["total"] += 1
                    if review.status == "approved":
                        accuracy_data[annotator_id][question_id]["correct"] += 1
        
        return accuracy_data

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
        project, admin = GroundTruthService._validate_project_and_user(project_id=project_id, user_id=admin_id, session=session)
        
        # Validate project admin role
        GroundTruthService._validate_user_role(user_id=admin_id, project_id=project_id, required_role="admin", session=session)
            
        # Validate question group and get questions
        group, questions = GroundTruthService._validate_question_group(question_group_id=question_group_id, session=session)
        
        # Validate answers match questions
        GroundTruthService._validate_answers_match_questions(answers=answers, questions=questions)
            
        # Run verification if specified
        GroundTruthService._run_verification(group=group, answers=answers)
            
        # Override each ground truth answer
        for question in questions:
            answer_value = answers[question.text]
            
            # Validate answer value
            GroundTruthService._validate_answer_value(question=question, answer_value=answer_value)
            
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
        GroundTruthService._validate_user_role(user_id=reviewer_id, project_id=answer.project_id, required_role="reviewer", session=session)
        
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
            .where(AnswerReview.answer_id == int(answer_id))
        )
        
        if not review:
            return None
            
        return {
            "status": review.status,
            "comment": review.comment,
            "reviewer_id": review.reviewer_id,
            "reviewed_at": review.reviewed_at
        }
    
    @staticmethod
    def check_question_modified_by_admin(video_id: int, project_id: int, question_id: int, session: Session) -> bool:
        """Check if a question's ground truth has been modified by admin.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project  
            question_id: The ID of the question
            session: Database session
            
        Returns:
            True if the question has been modified by admin, False otherwise
        """
        gt = session.get(ReviewerGroundTruth, (video_id, question_id, project_id))
        return gt is not None and gt.modified_by_admin_id is not None

    @staticmethod
    def get_admin_modification_details(video_id: int, project_id: int, question_id: int, session: Session) -> Optional[Dict[str, Any]]:
        """Get admin modification details for a question.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_id: The ID of the question
            session: Database session
            
        Returns:
            Dictionary with admin modification details or None if not modified by admin.
            Contains: original_value, current_value, admin_id, admin_name, modified_at
        """
        gt = session.get(ReviewerGroundTruth, (video_id, question_id, project_id))
        if not gt or gt.modified_by_admin_id is None:
            return None
        
        # Get admin user info
        admin_user = session.get(User, gt.modified_by_admin_id)
        admin_name = admin_user.user_id_str if admin_user else f"Admin {gt.modified_by_admin_id}"
        
        return {
            "original_value": gt.original_answer_value,
            "current_value": gt.answer_value,
            "admin_id": gt.modified_by_admin_id,
            "admin_name": admin_name,
            "modified_at": gt.modified_by_admin_at
        }

    @staticmethod
    def check_all_questions_modified_by_admin(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
        """Check if all questions in a group have been modified by admin.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            True if all questions in the group have been modified by admin, False otherwise
        """
        # Get all questions in the group
        questions = session.scalars(
            select(Question)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(QuestionGroupQuestion.question_group_id == question_group_id)
        ).all()
        
        if not questions:
            return False
        
        # Check if all questions have been modified by admin
        for question in questions:
            if not GroundTruthService.check_question_modified_by_admin(video_id=video_id, project_id=project_id, question_id=question.id, session=session):
                return False
        
        return True

    @staticmethod
    def get_question_option_selections(video_id: int, project_id: int, question_id: int, annotator_user_ids: List[int], session: Session) -> Dict[str, List[Dict[str, str]]]:
        """Get who selected which option for a single-choice question.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_id: The ID of the question
            annotator_user_ids: List of annotator user IDs to include
            session: Database session
            
        Returns:
            Dictionary mapping option values to list of selector info dicts.
            Each selector dict contains: name, initials, type ('annotator' or 'ground_truth')
        """
        option_selections = {}
        
        # Track which users we've already processed to avoid duplicates
        processed_users = set()
        
        # Get annotator answers
        for user_id in annotator_user_ids:
            # Skip if we've already processed this user
            if user_id in processed_users:
                continue
            processed_users.add(user_id)
            
            answer = session.scalar(
                select(AnnotatorAnswer)
                .where(
                    AnnotatorAnswer.video_id == video_id,
                    AnnotatorAnswer.project_id == project_id,
                    AnnotatorAnswer.question_id == question_id,
                    AnnotatorAnswer.user_id == user_id
                )
            )
            
            if answer:
                user = session.get(User, user_id)
                if user:
                    name_parts = user.user_id_str.split()
                    if len(name_parts) >= 2:
                        initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
                    else:
                        initials = user.user_id_str[:2].upper()
                    
                    option_value = answer.answer_value
                    if option_value not in option_selections:
                        option_selections[option_value] = []
                    
                    # Check if this user is already in the list for this option
                    user_already_listed = any(
                        selector["name"] == user.user_id_str and selector["type"] == "annotator"
                        for selector in option_selections[option_value]
                    )
                    
                    if not user_already_listed:
                        option_selections[option_value].append({
                            "name": user.user_id_str,
                            "initials": initials,
                            "type": "annotator"
                        })
        
        # Get ground truth answer
        gt_answer = session.scalar(
            select(ReviewerGroundTruth)
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id,
                ReviewerGroundTruth.question_id == question_id
            )
        )
        
        if gt_answer:
            option_value = gt_answer.answer_value
            if option_value not in option_selections:
                option_selections[option_value] = []
            
            # Check if ground truth is already listed
            gt_already_listed = any(
                selector["type"] == "ground_truth"
                for selector in option_selections[option_value]
            )
            
            if not gt_already_listed:
                option_selections[option_value].append({
                    "name": "Ground Truth",
                    "initials": "GT",
                    "type": "ground_truth"
                })
        
        return option_selections

    @staticmethod
    def get_question_text_answers(video_id: int, project_id: int, question_id: int, annotator_user_ids: List[int], session: Session) -> List[Dict[str, str]]:
        """Get text answers for a description question.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_id: The ID of the question
            annotator_user_ids: List of annotator user IDs to include
            session: Database session
            
        Returns:
            List of answer dictionaries containing: name, initials, answer_value
        """
        text_answers = []
        
        for user_id in annotator_user_ids:
            answer = session.scalar(
                select(AnnotatorAnswer)
                .where(
                    AnnotatorAnswer.video_id == video_id,
                    AnnotatorAnswer.project_id == project_id,
                    AnnotatorAnswer.question_id == question_id,
                    AnnotatorAnswer.user_id == user_id
                )
            )
            
            if answer:
                user = session.get(User, user_id)
                if user:
                    name_parts = user.user_id_str.split()
                    if len(name_parts) >= 2:
                        initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper()
                    else:
                        initials = user.user_id_str[:2].upper()
                    
                    text_answers.append({
                        "name": user.user_id_str,
                        "initials": initials,
                        "answer_value": answer.answer_value
                    })
        
        return text_answers
    
    @staticmethod
    def get_ground_truth_dict_for_question_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> Dict[str, str]:
        """Get existing ground truth for a video and question group.
        
        Args:
            video_id: The ID of the video
            project_id: The ID of the project
            question_group_id: The ID of the question group
            session: Database session
            
        Returns:
            Dictionary mapping question text to answer value
        """
        gts = session.scalars(
            select(ReviewerGroundTruth)
            .join(Question, ReviewerGroundTruth.question_id == Question.id)
            .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
            .where(
                ReviewerGroundTruth.video_id == video_id,
                ReviewerGroundTruth.project_id == project_id,
                QuestionGroupQuestion.question_group_id == question_group_id
            )
        ).all()
        
        result = {}
        for gt in gts:
            question = session.get(Question, gt.question_id)
            if question:
                result[question.text] = gt.answer_value
        
        return result

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
            ProjectGroupService._validate_project_group_uniqueness(project_ids=project_ids, session=session)
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
            ProjectGroupService._validate_project_group_uniqueness(project_ids=all_ids, session=session)
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
        # Convert group to dict
        group = {
            "id": group.id,
            "name": group.name,
            "description": group.description,
            "created_at": group.created_at
        }
        # Convert projects to dicts
        projects = [
            {
                "id": p.id,
                "name": p.name,
                "schema_id": p.schema_id,
                "description": p.description,
                "created_at": p.created_at,
                "is_archived": p.is_archived
            }
            for p in projects
        ]
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
        groups = [
            {
                "id": g.id,
                "name": g.name,
                "description": g.description,
                "created_at": g.created_at
            }
            for g in groups
        ]
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
                raise ValueError(f"Project not found")
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
                    # Get project names
                    project1_name = projects[i].name
                    project2_name = projects[j].name
                    
                    # Get video UIDs for better readability
                    overlapping_video_uids = []
                    for video_id in v_overlap:
                        video = session.get(Video, video_id)
                        if video:
                            overlapping_video_uids.append(video.video_uid)
                    
                    # Format the error message with better readability
                    if len(overlapping_video_uids) <= 5:
                        # Show all video UIDs if 5 or fewer
                        video_list = ", ".join(overlapping_video_uids)
                        error_msg = (f"Cannot group projects '{project1_name}' and '{project2_name}' together. "
                                   f"They have {len(overlapping_video_uids)} overlapping videos with shared questions: {video_list}")
                    else:
                        # Show first 3 and summary if more than 5
                        preview_videos = ", ".join(overlapping_video_uids[:3])
                        remaining_count = len(overlapping_video_uids) - 3
                        error_msg = (f"Cannot group projects '{project1_name}' and '{project2_name}' together. "
                                   f"They have {len(overlapping_video_uids)} overlapping videos with shared questions. "
                                   f"Examples: {preview_videos} (and {remaining_count} more)")
                    
                    raise ValueError(error_msg)

    
    @staticmethod
    def get_grouped_projects_for_user(user_id: int, role: str, session: Session) -> Dict[str, List[Dict[str, Any]]]:
        """Get project groups with their projects for a user.
        
        Args:
            user_id: The ID of the user
            role: The role to filter by
            session: Database session
            
        Returns:
            Dictionary mapping group names to lists of project info dicts
        """
        # Get all projects assigned to user
        user_projects = session.scalars(
            select(Project)
            .join(ProjectUserRole, Project.id == ProjectUserRole.project_id)
            .where(
                ProjectUserRole.user_id == user_id,
                ProjectUserRole.role == role,
                ProjectUserRole.is_archived == False,
                Project.is_archived == False
            )
        ).all()
        
        if not user_projects:
            return {"Unassigned": []}
        
        # Get all project groups
        project_groups = session.scalars(select(ProjectGroup).where(ProjectGroup.is_archived == False)).all()
        
        # Group projects
        grouped_projects = {"Unassigned": []}
        
        # Add named groups
        for group in project_groups:
            grouped_projects[group.name] = []
        
        # Categorize projects
        for project in user_projects:
            # Check if project is in any group
            group_assignment = session.scalar(
                select(ProjectGroupProject)
                .join(ProjectGroup, ProjectGroupProject.project_group_id == ProjectGroup.id)
                .where(
                    ProjectGroupProject.project_id == project.id,
                    ProjectGroup.is_archived == False
                )
            )
            
            project_dict = {
                "id": project.id,
                "name": project.name,
                "description": project.description,
                "created_at": project.created_at
            }
            
            if group_assignment:
                group_name = session.get(ProjectGroup, group_assignment.project_group_id).name
                if group_name in grouped_projects:
                    grouped_projects[group_name].append(project_dict)
            else:
                grouped_projects["Unassigned"].append(project_dict)
        
        # Remove empty groups
        return {name: projects for name, projects in grouped_projects.items() if projects}

class AutoSubmitService:
    @staticmethod
    def get_weighted_votes_for_question(
        video_id: int, 
        project_id: int, 
        question_id: int,
        include_user_ids: List[int],
        virtual_responses: List[Dict],
        session: Session,
        user_weights: Dict[int, float] = None
    ) -> Dict[str, float]:
        """Calculate weighted votes for a single question with user and option weights"""
        try:
            from label_pizza.models import User, ProjectUserRole
            from sqlalchemy import select
            
            # Get question details
            question = QuestionService.get_question_by_id(question_id=question_id, session=session)
            if not question:
                return {}
            
            user_weights = user_weights or {}
            vote_weights = {}
            
            # Get real user answers
            answers_df = AnnotatorService.get_question_answers(
                question_id=question_id, project_id=project_id, session=session
            )
            
            if not answers_df.empty:
                video_answers = answers_df[
                    (answers_df["Video ID"] == video_id) & 
                    (answers_df["User ID"].isin(include_user_ids))
                ]
                
                for _, answer_row in video_answers.iterrows():
                    user_id = int(answer_row["User ID"])
                    answer_value = str(answer_row["Answer Value"])
                    
                    # Get user weight - prioritize passed weights, then database, then default
                    user_weight = user_weights.get(user_id)
                    if user_weight is None:
                        assignment = session.execute(
                            select(ProjectUserRole).where(
                                ProjectUserRole.user_id == user_id,
                                ProjectUserRole.project_id == project_id,
                                ProjectUserRole.role == "annotator"
                            )
                        ).first()
                        user_weight = float(assignment[0].user_weight) if assignment else 1.0
                    
                    # Get option weight
                    option_weight = 1.0
                    if question["type"] == "single" and question["option_weights"]:
                        try:
                            option_index = question["options"].index(answer_value)
                            option_weight = float(question["option_weights"][option_index])
                        except (ValueError, IndexError):
                            option_weight = 1.0
                    
                    # Combined weight = user_weight * option_weight
                    combined_weight = user_weight * option_weight
                    vote_weights[answer_value] = vote_weights.get(answer_value, 0.0) + combined_weight
            
            # Add virtual responses
            for virtual_response in virtual_responses:
                answer_value = str(virtual_response["answer"])
                user_weight = float(virtual_response["user_weight"])
                
                option_weight = 1.0
                if question["type"] == "single" and question["option_weights"]:
                    try:
                        option_index = question["options"].index(answer_value)
                        option_weight = float(question["option_weights"][option_index])
                    except (ValueError, IndexError):
                        option_weight = 1.0
                
                # Combined weight = user_weight * option_weight
                combined_weight = user_weight * option_weight
                vote_weights[answer_value] = vote_weights.get(answer_value, 0.0) + combined_weight
            
            return vote_weights
            
        except Exception as e:
            raise ValueError(f"Error calculating weighted votes: {str(e)}")
    
    @staticmethod
    def calculate_auto_submit_answers(
        video_id: int,
        project_id: int, 
        question_group_id: int,
        include_user_ids: List[int],
        virtual_responses_by_question: Dict[int, List[Dict]],
        thresholds: Dict[int, float],
        session: Session,
        user_weights: Dict[int, float] = None
    ) -> Dict[str, Any]:
        """Calculate which answers would be auto-submitted"""
        try:
            questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
            results = {
                "answers": {},
                "skipped": [],
                "threshold_failures": [],
                "vote_details": {},
                "voting_summary": {}
            }
            
            # Check existing answers
            existing_answers = {}
            try:
                existing_answers = AnnotatorService.get_user_answers_for_question_group(
                    video_id=video_id, project_id=project_id, user_id=include_user_ids[0] if include_user_ids else 1,
                    question_group_id=question_group_id, session=session
                )
            except:
                pass
            
            total_votes = 0
            annotator_count = len(include_user_ids)
            total_confidence = 0.0
            confidence_count = 0
            consensus_scores = []
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                
                # Skip if answer already exists
                if question_text in existing_answers and existing_answers[question_text]:
                    results["skipped"].append(question_text)
                    continue
                
                virtual_responses = virtual_responses_by_question.get(question_id, [])
                vote_weights = AutoSubmitService.get_weighted_votes_for_question(
                    video_id=video_id, project_id=project_id, question_id=question_id,
                    include_user_ids=include_user_ids, virtual_responses=virtual_responses, 
                    session=session, user_weights=user_weights
                )
                
                results["vote_details"][question_text] = vote_weights
                
                if not vote_weights:
                    continue
                
                total_votes += sum(vote_weights.values())
                
                # Calculate consensus score for this question
                total_weight = sum(vote_weights.values())
                if total_weight > 0:
                    max_weight = max(vote_weights.values())
                    consensus_score = (max_weight / total_weight) * 100
                    consensus_scores.append(consensus_score)
                
                # Find winning option
                if total_weight == 0:
                    continue
                
                winning_option = max(vote_weights.keys(), key=lambda k: vote_weights[k])
                winning_weight = vote_weights[winning_option]
                winning_percentage = (winning_weight / total_weight) * 100
                
                # Check threshold
                threshold = thresholds.get(question_id, 100.0)
                if winning_percentage >= threshold:
                    results["answers"][question_text] = winning_option
                else:
                    results["threshold_failures"].append({
                        "question": question_text,
                        "percentage": winning_percentage,
                        "threshold": threshold
                    })
            
            # Add voting summary
            results["voting_summary"] = {
                "total_votes": total_votes,
                "annotator_count": annotator_count,
                "avg_confidence": total_confidence / confidence_count if confidence_count > 0 else 0,
                "consensus_score": sum(consensus_scores) / len(consensus_scores) if consensus_scores else 0
            }
            
            return results
            
        except Exception as e:
            raise ValueError(f"Error calculating auto-submit answers: {str(e)}")
    
    @staticmethod
    def auto_submit_question_group(
        video_id: int,
        project_id: int,
        question_group_id: int, 
        user_id: int,
        include_user_ids: List[int],
        virtual_responses_by_question: Dict[int, List[Dict]],
        thresholds: Dict[int, float],
        session: Session,
        user_weights: Dict[int, float] = None
    ) -> Dict[str, Any]:
        """Actually submit the auto-calculated answers"""
        try:
            # Calculate what would be submitted
            calculation_results = AutoSubmitService.calculate_auto_submit_answers(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id,
                include_user_ids=include_user_ids, virtual_responses_by_question=virtual_responses_by_question,
                thresholds=thresholds, session=session, user_weights=user_weights
            )
            
            answers = calculation_results["answers"]
            
            if not answers:
                return {
                    "success": True,
                    "submitted_count": 0,
                    "skipped_count": len(calculation_results["skipped"]),
                    "threshold_failures": len(calculation_results["threshold_failures"]),
                    "verification_failed": False,
                    "details": calculation_results
                }
            
            # Run verification if the group has one
            try:
                group_details = QuestionGroupService.get_group_details_with_verification(
                    group_id=question_group_id, session=session
                )
                verification_function = group_details.get("verification_function")
                
                if verification_function:
                    from label_pizza.models import QuestionGroup
                    from sqlalchemy import select
                    
                    group = session.execute(
                        select(QuestionGroup).where(QuestionGroup.id == question_group_id)
                    ).scalar_one()
                    
                    AnnotatorService._run_verification(group, answers)
                    
            except ValueError as verification_error:
                return {
                    "success": False,
                    "submitted_count": 0,
                    "skipped_count": len(calculation_results["skipped"]),
                    "threshold_failures": len(calculation_results["threshold_failures"]),
                    "verification_failed": True,
                    "verification_error": str(verification_error),
                    "details": calculation_results
                }
            
            # Submit the answers
            AnnotatorService.submit_answer_to_question_group(
                video_id=video_id, project_id=project_id, user_id=user_id,
                question_group_id=question_group_id, answers=answers, session=session
            )
            
            return {
                "success": True,
                "submitted_count": len(answers),
                "skipped_count": len(calculation_results["skipped"]),
                "threshold_failures": len(calculation_results["threshold_failures"]),
                "verification_failed": False,
                "details": calculation_results
            }
            
        except Exception as e:
            raise ValueError(f"Error in auto-submit: {str(e)}")


class ReviewerAutoSubmitService:
    @staticmethod
    def auto_submit_ground_truth_group_with_custom_weights(
        video_id: int,
        project_id: int,
        question_group_id: int, 
        reviewer_id: int,
        include_user_ids: List[int],
        virtual_responses_by_question: Dict[int, List[Dict]],
        thresholds: Dict[int, float],
        session: Session,
        user_weights: Dict[int, float] = None,
        custom_option_weights: Dict[int, Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Auto-submit ground truth using weighted voting with custom option weights"""
        try:
            # Calculate what would be submitted
            calculation_results = ReviewerAutoSubmitService.calculate_auto_submit_ground_truth_with_custom_weights(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id,
                include_user_ids=include_user_ids, virtual_responses_by_question=virtual_responses_by_question,
                thresholds=thresholds, session=session, user_weights=user_weights,
                custom_option_weights=custom_option_weights
            )
            
            answers = calculation_results["answers"]
            
            if not answers:
                return {
                    "success": True,
                    "submitted_count": 0,
                    "skipped_count": len(calculation_results["skipped"]),
                    "threshold_failures": len(calculation_results["threshold_failures"]),
                    "verification_failed": False,
                    "details": calculation_results
                }
            
            # Run verification (unchanged)
            try:
                group_details = QuestionGroupService.get_group_details_with_verification(
                    group_id=question_group_id, session=session
                )
                verification_function = group_details.get("verification_function")
                
                if verification_function:
                    from label_pizza.models import QuestionGroup
                    from sqlalchemy import select
                    
                    group = session.execute(
                        select(QuestionGroup).where(QuestionGroup.id == question_group_id)
                    ).scalar_one()
                    AnnotatorService._run_verification(group, answers)
                    
            except ValueError as verification_error:
                return {
                    "success": False,
                    "submitted_count": 0,
                    "skipped_count": len(calculation_results["skipped"]),
                    "threshold_failures": len(calculation_results["threshold_failures"]),
                    "verification_failed": True,
                    "verification_error": str(verification_error),
                    "details": calculation_results
                }
            
            # Submit ground truth (unchanged)
            GroundTruthService.submit_ground_truth_to_question_group(
                video_id=video_id, project_id=project_id, reviewer_id=reviewer_id,
                question_group_id=question_group_id, answers=answers, session=session
            )
            
            return {
                "success": True,
                "submitted_count": len(answers),
                "skipped_count": len(calculation_results["skipped"]),
                "threshold_failures": len(calculation_results["threshold_failures"]),
                "verification_failed": False,
                "details": calculation_results
            }
            
        except Exception as e:
            raise ValueError(f"Error in reviewer auto-submit: {str(e)}")

    @staticmethod
    def calculate_auto_submit_ground_truth_with_custom_weights(
        video_id: int,
        project_id: int, 
        question_group_id: int,
        include_user_ids: List[int],
        virtual_responses_by_question: Dict[int, List[Dict]],
        thresholds: Dict[int, float],
        session: Session,
        user_weights: Dict[int, float] = None,
        custom_option_weights: Dict[int, Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Calculate ground truth with custom option weights"""
        try:
            questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
            results = {
                "answers": {},
                "skipped": [],
                "threshold_failures": [],
                "vote_details": {},
                "voting_summary": {}
            }
            
            # Check existing GROUND TRUTH answers
            existing_answers = {}
            try:
                existing_answers = GroundTruthService.get_ground_truth_dict_for_question_group(
                    video_id=video_id, project_id=project_id, 
                    question_group_id=question_group_id, session=session
                )
            except:
                pass
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                
                # Skip if GROUND TRUTH answer already exists
                if question_text in existing_answers and existing_answers[question_text]:
                    results["skipped"].append(question_text)
                    continue
                
                virtual_responses = virtual_responses_by_question.get(question_id, [])
                
                # MINIMAL CHANGE: Use custom option weights for this question
                question_custom_weights = None
                if custom_option_weights and question_id in custom_option_weights:
                    question_custom_weights = custom_option_weights[question_id]
                
                vote_weights = ReviewerAutoSubmitService.get_weighted_votes_for_question_with_custom_weights(
                    video_id=video_id, project_id=project_id, question_id=question_id,
                    include_user_ids=include_user_ids, virtual_responses=virtual_responses, 
                    session=session, user_weights=user_weights,
                    custom_option_weights=question_custom_weights
                )
                
                results["vote_details"][question_text] = vote_weights
                
                if not vote_weights:
                    continue
                
                total_weight = sum(vote_weights.values())
                if total_weight == 0:
                    continue
                
                winning_option = max(vote_weights.keys(), key=lambda k: vote_weights[k])
                winning_weight = vote_weights[winning_option]
                winning_percentage = (winning_weight / total_weight) * 100
                
                # Check threshold
                threshold = thresholds.get(question_id, 100.0)
                if winning_percentage >= threshold:
                    results["answers"][question_text] = winning_option
                else:
                    results["threshold_failures"].append({
                        "question": question_text,
                        "percentage": winning_percentage,
                        "threshold": threshold
                    })
            
            return results
            
        except Exception as e:
            raise ValueError(f"Error calculating auto-submit ground truth: {str(e)}")

    @staticmethod
    def get_weighted_votes_for_question_with_custom_weights(
        video_id: int, 
        project_id: int, 
        question_id: int,
        include_user_ids: List[int],
        virtual_responses: List[Dict],
        session: Session,
        user_weights: Dict[int, float] = None,
        custom_option_weights: Dict[str, float] = None,
        cache_data: Dict = None  # ADD THIS PARAMETER
    ) -> Dict[str, float]:
        """
        OPTIMIZED: Uses cached data when available to avoid database queries
        """
        try:
            # Get question details - still need this for option weights
            question = QuestionService.get_question_by_id(question_id=question_id, session=session)
            if not question:
                return {}
            
            user_weights = user_weights or {}
            vote_weights = {}
            
            # OPTIMIZED: Use cached data if available
            if cache_data and question_id in cache_data.get("annotator_answers", {}):
                # Use cached answers
                for answer_record in cache_data["annotator_answers"][question_id]:
                    user_id = int(answer_record["User ID"])
                    answer_value = str(answer_record["Answer Value"])
                    
                    # Get user weight - prioritize passed weights, then cached, then default
                    user_weight = user_weights.get(user_id)
                    if user_weight is None:
                        # Try to get from cache first
                        if user_id in cache_data.get("user_weights", {}):
                            user_weight = float(cache_data["user_weights"][user_id])
                        else:
                            # Fallback to service method
                            try:
                                project_user_weights = AuthService.get_user_weights_for_project(
                                    project_id=project_id, session=session
                                )
                                user_weight = float(project_user_weights.get(user_id, 1.0))
                            except:
                                user_weight = 1.0
                    
                    # Use custom option weights if provided (for reviewers)
                    option_weight = 1.0
                    if question["type"] == "single":
                        if custom_option_weights and answer_value in custom_option_weights:
                            option_weight = float(custom_option_weights[answer_value])
                        elif question["option_weights"]:
                            try:
                                option_index = question["options"].index(answer_value)
                                option_weight = float(question["option_weights"][option_index])
                            except (ValueError, IndexError):
                                option_weight = 1.0
                    
                    # Combined weight = user_weight * option_weight
                    combined_weight = user_weight * option_weight
                    vote_weights[answer_value] = vote_weights.get(answer_value, 0.0) + combined_weight
            else:
                # Fallback to original method if no cache
                answers_df = AnnotatorService.get_question_answers(
                    question_id=question_id, project_id=project_id, session=session
                )
                
                if not answers_df.empty:
                    video_answers = answers_df[
                        (answers_df["Video ID"] == video_id) & 
                        (answers_df["User ID"].isin(include_user_ids))
                    ]
                    
                    for _, answer_row in video_answers.iterrows():
                        user_id = int(answer_row["User ID"])
                        answer_value = str(answer_row["Answer Value"])
                        
                        # Get user weight - prioritize passed weights, then service, then default
                        user_weight = user_weights.get(user_id)
                        if user_weight is None:
                            try:
                                project_user_weights = AuthService.get_user_weights_for_project(
                                    project_id=project_id, session=session
                                )
                                user_weight = float(project_user_weights.get(user_id, 1.0))
                            except:
                                user_weight = 1.0
                        
                        # Use custom option weights if provided (for reviewers)
                        option_weight = 1.0
                        if question["type"] == "single":
                            if custom_option_weights and answer_value in custom_option_weights:
                                option_weight = float(custom_option_weights[answer_value])
                            elif question["option_weights"]:
                                try:
                                    option_index = question["options"].index(answer_value)
                                    option_weight = float(question["option_weights"][option_index])
                                except (ValueError, IndexError):
                                    option_weight = 1.0
                        
                        # Combined weight = user_weight * option_weight
                        combined_weight = user_weight * option_weight
                        vote_weights[answer_value] = vote_weights.get(answer_value, 0.0) + combined_weight
            
            # Add virtual responses (unchanged)
            for virtual_response in virtual_responses:
                answer_value = str(virtual_response["answer"])
                user_weight = float(virtual_response["user_weight"])
                
                option_weight = 1.0
                if question["type"] == "single":
                    if custom_option_weights and answer_value in custom_option_weights:
                        option_weight = float(custom_option_weights[answer_value])
                    elif question["option_weights"]:
                        try:
                            option_index = question["options"].index(answer_value)
                            option_weight = float(question["option_weights"][option_index])
                        except (ValueError, IndexError):
                            option_weight = 1.0
                
                # Combined weight = user_weight * option_weight
                combined_weight = user_weight * option_weight
                vote_weights[answer_value] = vote_weights.get(answer_value, 0.0) + combined_weight
            
            return vote_weights
            
        except Exception as e:
            raise ValueError(f"Error calculating weighted votes: {str(e)}")

class GroundTruthExportService:
    @staticmethod
    def export_ground_truth_data(project_ids: List[int], session: Session) -> List[Dict[str, Any]]:
        """Export ground truth data from a list of projects.
        
        Args:
            project_ids: List of project IDs to export from
            session: Database session
            
        Returns:
            List of video dictionaries with video_uid, url, and answers
            
        Raises:
            ValueError: If projects not found or reusable question groups have inconsistent answers
        """
        # Validate all projects exist
        for project_id in project_ids:
            project = session.get(Project, project_id)
            if not project:
                raise ValueError(f"Project with ID {project_id} not found")
            if project.is_archived:
                raise ValueError(f"Project with ID {project_id} is archived")
        
        # Check reusable question group consistency before proceeding
        GroundTruthExportService._validate_reusable_question_groups(project_ids, session)

        # Check non-reusable question group constraints
        GroundTruthExportService._validate_non_reusable_question_groups(project_ids, session)
        
        # Get all unique videos across projects
        all_videos = set()
        for project_id in project_ids:
            from label_pizza.models import ProjectVideo
            videos = session.scalars(
                select(ProjectVideo.video_id)
                .where(ProjectVideo.project_id == project_id)
            ).all()
            all_videos.update(videos)
        
        # Export data for each video
        export_data = []
        for video_id in sorted(all_videos):  # Sort for consistent ordering
            video = session.get(Video, video_id)
            if not video or video.is_archived:
                continue
                
            video_data = {
                "video_uid": video.video_uid,
                "url": video.url,
                "answers": {}, # question.text -> raw answer value
                "question_display_text": {}, # question.text -> display text shown
                "answer_display_values": {} # question.text -> display value shown
            }
            
            # Get all ground truth answers for this video across all projects
            # Use a set to track which questions we've already processed to avoid duplicates
            processed_questions = set()
            
            for project_id in project_ids:
                gts = session.scalars(
                    select(ReviewerGroundTruth)
                    .where(
                        ReviewerGroundTruth.video_id == video_id,
                        ReviewerGroundTruth.project_id == project_id
                    )
                ).all()
                
                for gt in gts:
                    question = session.get(Question, gt.question_id)
                    if question and not question.is_archived:
                        # For reusable groups, we've already validated consistency,
                        # so we can safely take the first answer we encounter
                        if question.text not in processed_questions:
                            # video_data["answers"][question.text] = gt.answer_value
                            # processed_questions.add(question.text)
                            try:
                                question_with_display = CustomDisplayService.get_custom_display(
                                    question_id=gt.question_id,
                                    project_id=project_id,
                                    video_id=video_id,
                                    session=session
                                )
                                display_text = question_with_display["display_text"]
                                
                                # Get display value for the selected answer
                                if question_with_display["type"] == "single" and question_with_display["display_values"]:
                                    try:
                                        option_index = question_with_display["options"].index(gt.answer_value)
                                        display_value = question_with_display["display_values"][option_index]
                                    except (ValueError, IndexError):
                                        display_value = gt.answer_value  # Fallback to raw value
                                else:
                                    display_value = gt.answer_value  # For description type or no display values
                                    
                            except Exception:
                                # Fallback to original display text if custom display fails
                                display_text = question.display_text
                                display_value = gt.answer_value
                            
                            # Store all the information
                            video_data["answers"][question.text] = gt.answer_value
                            video_data["question_display_text"][question.text] = display_text
                            video_data["answer_display_values"][question.text] = display_value
                            processed_questions.add(question.text)
                            
            if video_data["answers"]:  # Only include videos with answers
                export_data.append(video_data)
        
        return export_data
    
    @staticmethod
    def _validate_reusable_question_groups(project_ids: List[int], session: Session) -> None:
        """Validate that reusable question groups have consistent answers across projects.
        
        Raises:
            ValueError: If inconsistencies are found
        """
        # Get all reusable question groups used in these projects
        reusable_groups = session.scalars(
            select(QuestionGroup)
            .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
            .join(Project, SchemaQuestionGroup.schema_id == Project.schema_id)
            .where(
                Project.id.in_(project_ids),
                QuestionGroup.is_reusable == True,
                QuestionGroup.is_archived == False
            )
            .distinct()
        ).all()
        
        if not reusable_groups:
            return  # No reusable groups to validate
        
        # Check consistency for each reusable group
        group_inconsistencies = {}
        
        for group in reusable_groups:
            # Get all questions in this group
            questions = session.scalars(
                select(Question)
                .join(QuestionGroupQuestion, Question.id == QuestionGroupQuestion.question_id)
                .where(
                    QuestionGroupQuestion.question_group_id == group.id,
                    Question.is_archived == False
                )
            ).all()
            
            # Find which projects use this reusable group
            projects_using_group = session.scalars(
                select(Project.id)
                .join(SchemaQuestionGroup, Project.schema_id == SchemaQuestionGroup.schema_id)
                .where(
                    SchemaQuestionGroup.question_group_id == group.id,
                    Project.id.in_(project_ids),
                    Project.is_archived == False
                )
            ).all()
            
            if len(projects_using_group) < 2:
                continue  # No conflict possible with only one project
            
            # For each question, check consistency across projects
            for question in questions:
                # Get all videos that have answers for this question across the relevant projects
                video_answer_map = {}  # video_id -> {project_id -> answer_value}
                
                for project_id in projects_using_group:
                    gts = session.scalars(
                        select(ReviewerGroundTruth)
                        .where(
                            ReviewerGroundTruth.question_id == question.id,
                            ReviewerGroundTruth.project_id == project_id
                        )
                    ).all()
                    
                    for gt in gts:
                        video_id = gt.video_id
                        if video_id not in video_answer_map:
                            video_answer_map[video_id] = {}
                        video_answer_map[video_id][project_id] = gt.answer_value
                
                # Check for inconsistencies - videos that appear in multiple projects with different answers
                inconsistent_videos = []
                for video_id, project_answers in video_answer_map.items():
                    if len(project_answers) > 1:  # Video appears in multiple projects
                        unique_answers = set(project_answers.values())
                        if len(unique_answers) > 1:  # Different answers across projects
                            video = session.get(Video, video_id)
                            if video:
                                inconsistent_videos.append(video.video_uid)
                
                if inconsistent_videos:
                    if group.title not in group_inconsistencies:
                        group_inconsistencies[group.title] = []
                    group_inconsistencies[group.title].append({
                        "question": question.text,
                        "videos": inconsistent_videos
                    })
        
        if group_inconsistencies:
            error_parts = []
            total_failing_videos = set()
            
            for group_name, inconsistencies in group_inconsistencies.items():
                group_videos = set()
                question_details = []
                
                for inc in inconsistencies:
                    group_videos.update(inc['videos'])
                    question_details.append(f"    Question '{inc['question']}': {len(inc['videos'])} videos")
                
                total_failing_videos.update(group_videos)
                
                error_parts.append(
                    f"Reusable question group '{group_name}' ({len(group_videos)} failing videos):\n" +
                    "\n".join(question_details)
                )
            
            failing_video_list = sorted(list(total_failing_videos))
            
            raise ValueError(
                f"Cannot export due to inconsistent answers in reusable question groups.\n"
                f"Total videos with inconsistencies: {len(failing_video_list)}\n"
                f"Failing videos: {', '.join(failing_video_list[:10])}"
                f"{'...' if len(failing_video_list) > 10 else ''}\n\n"
                f"Details:\n" + "\n\n".join(error_parts)
            )

    @staticmethod
    def _validate_non_reusable_question_groups(project_ids: List[int], session: Session) -> None:
        """Validate that non-reusable question groups don't have videos appearing in multiple projects.
        
        Raises:
            ValueError: If any video appears in multiple projects sharing non-reusable question groups
        """
        # Get all non-reusable question groups used in these projects
        non_reusable_groups = session.scalars(
            select(QuestionGroup)
            .join(SchemaQuestionGroup, QuestionGroup.id == SchemaQuestionGroup.question_group_id)
            .join(Project, SchemaQuestionGroup.schema_id == Project.schema_id)
            .where(
                Project.id.in_(project_ids),
                QuestionGroup.is_reusable == False,
                QuestionGroup.is_archived == False
            )
            .distinct()
        ).all()
        
        if not non_reusable_groups:
            return  # No non-reusable groups to validate
        
        # Check for violations for each non-reusable group
        group_violations = {}
        
        for group in non_reusable_groups:
            # Find which projects use this non-reusable group
            projects_using_group = session.scalars(
                select(Project.id)
                .join(SchemaQuestionGroup, Project.schema_id == SchemaQuestionGroup.schema_id)
                .where(
                    SchemaQuestionGroup.question_group_id == group.id,
                    Project.id.in_(project_ids),
                    Project.is_archived == False
                )
            ).all()
            
            if len(projects_using_group) < 2:
                continue  # No violation possible with only one project
            
            # Get all videos for each project using this group
            project_videos = {}  # project_id -> set of video_ids
            for project_id in projects_using_group:
                videos = set(session.scalars(
                    select(ProjectVideo.video_id)
                    .where(ProjectVideo.project_id == project_id)
                ).all())
                project_videos[project_id] = videos
            
            # Check for overlapping videos between projects
            overlapping_videos = set()
            project_list = list(projects_using_group)
            
            for i in range(len(project_list)):
                for j in range(i + 1, len(project_list)):
                    project1_id = project_list[i]
                    project2_id = project_list[j]
                    
                    overlap = project_videos[project1_id] & project_videos[project2_id]
                    if overlap:
                        overlapping_videos.update(overlap)
            
            if overlapping_videos:
                # Get video UIDs for better error messages
                video_uids = []
                for video_id in overlapping_videos:
                    video = session.get(Video, video_id)
                    if video:
                        video_uids.append(video.video_uid)
                
                group_violations[group.title] = {
                    "videos": sorted(video_uids),
                    "projects": projects_using_group
                }
        
        if group_violations:
            error_parts = []
            total_failing_videos = set()
            
            for group_name, violation in group_violations.items():
                total_failing_videos.update(violation['videos'])
                
                # Get project names for better error messages
                project_names = []
                for project_id in violation['projects']:
                    project = session.get(Project, project_id)
                    if project:
                        project_names.append(f"'{project.name}' (ID: {project_id})")
                
                error_parts.append(
                    f"Non-reusable question group '{group_name}' is used in multiple projects:\n"
                    f"    Projects: {', '.join(project_names)}\n"
                    f"    Overlapping videos ({len(violation['videos'])}): {', '.join(violation['videos'][:10])}"
                    f"{'...' if len(violation['videos']) > 10 else ''}"
                )
            
            raise ValueError(
                f"Cannot export due to non-reusable question groups being shared across projects.\n"
                f"Non-reusable groups should only be used in one project each.\n"
                f"Total videos affected: {len(total_failing_videos)}\n\n"
                f"Violations:\n" + "\n\n".join(error_parts)
            )

def save_export_as_json(export_data: List[Dict[str, Any]], filepath: str) -> None:
    """Save export data as JSON file with pretty formatting."""
    with open(filepath, 'w', encoding='utf-8') as f:
        import json
        json.dump(export_data, f, indent=2, ensure_ascii=False)


def save_export_as_excel(export_data: List[Dict[str, Any]], filepath_or_buffer) -> None:
    """Save export data as Excel file with pretty formatting."""
    if not export_data:
        # Create empty Excel file
        pd.DataFrame({"Message": ["No data to export"]}).to_excel(filepath_or_buffer, index=False)
        return
    
    # Collect all unique question texts
    all_questions = set()
    for video in export_data:
        all_questions.update(video["answers"].keys())
    
    all_questions = sorted(list(all_questions))
    
    # Prepare data for DataFrame
    rows = []
    for video in export_data:
        row = {
            "Video UID": video["video_uid"],
            "URL": video["url"]
        }
        
        # Add answers for each question
        for i, question in enumerate(all_questions):
            display_text = video["question_display_text"].get(question, question)
            display_value = video["answer_display_values"].get(question, "")
            
            row[f"Q{i}: {question}"] = display_text      # Custom display question text
            row[f"Q{i} Answer"] = display_value          # Custom display answer value
        
        rows.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    
    # Save to Excel with formatting
    try:
        with pd.ExcelWriter(filepath_or_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Ground Truth Export', index=False)
            
            # Get the workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Ground Truth Export']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                
                # Set width with some padding, cap at reasonable size
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Style the header row
            try:
                from openpyxl.styles import Font, PatternFill, Alignment
                
                header_font = Font(bold=True)
                header_fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
                center_alignment = Alignment(horizontal="center", vertical="center")
                
                for cell in worksheet[1]:  # First row
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = center_alignment
                    
                # Freeze the header row
                worksheet.freeze_panes = "A2"
                
            except ImportError:
                # If openpyxl styling modules aren't available, skip formatting
                pass
                
    except Exception as e:
        # Fallback to basic Excel export if formatting fails
        df.to_excel(filepath_or_buffer, index=False)
        if hasattr(filepath_or_buffer, 'write'):  # It's a buffer, not a file path
            pass  # Don't print for buffers
        else:
            print(f"Warning: Excel formatting failed ({e}), saved with basic formatting")


# Example usage function
def export_projects_ground_truth(project_ids: List[int], output_path: str, format_type: str = "json", session: Session = None):
    """Convenience function to export ground truth data.
    
    Args:
        project_ids: List of project IDs to export
        output_path: Path for output file
        format_type: "json" or "excel"
        session: Database session
    """
    if session is None:
        raise ValueError("Database session is required")
    
    try:
        # Export the data
        export_data = GroundTruthExportService.export_ground_truth_data(project_ids, session)
        
        # Ensure correct file extension
        output_path = Path(output_path)
        if format_type == "json" and not output_path.suffix.lower() == ".json":
            output_path = output_path.with_suffix(".json")
        elif format_type == "excel" and not output_path.suffix.lower() in [".xlsx", ".xls"]:
            output_path = output_path.with_suffix(".xlsx")
        
        # Save the data
        if format_type == "json":
            save_export_as_json(export_data, str(output_path))
        elif format_type == "excel":
            save_export_as_excel(export_data, str(output_path))
        else:
            raise ValueError(f"Unsupported format: {format_type}")
        
        print(f"Successfully exported {len(export_data)} videos to {output_path}")
        return str(output_path)
        
    except ValueError as e:
        print(f"Export failed: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error during export: {e}")
        raise