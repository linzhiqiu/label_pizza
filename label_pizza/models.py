from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, Float, Enum,
    UniqueConstraint, Index, create_engine, JSON, func, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, Session
import json

Base = declarative_base()
now = lambda: datetime.utcnow()


class User(Base):
    """Global identity; user_type='admin' bypasses project ACLs.
    Soft-disable via is_archived keeps audit history.
    Email is only required for human and admin users; model users don't have emails.
    """
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    user_id_str = Column(String(128), unique=True, nullable=False)  # login / SSO handle
    email = Column(String(255), unique=True, nullable=True)  # Required for human/admin users, NULL for model users
    password_hash = Column(Text, nullable=False)
    user_type = Column(Enum("human", "model", "admin", name="user_types"), default="human")
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class Video(Base):
    """video_uid lets UI find assets without joins; metadata stays searchable.
    Archiving supports takedowns.
    """
    __tablename__ = "videos"
    id = Column(Integer, primary_key=True)
    video_uid = Column(String(255), unique=True, nullable=False)  # file-name or UUID
    url = Column(Text)
    video_metadata = Column(JSONB)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class VideoTag(Base):
    """Global flags (NSFW, epilepsy). Only trusted actors write rows."""
    __tablename__ = "video_tags"
    video_id = Column(Integer, primary_key=True)
    tag = Column(String(64), primary_key=True)
    tag_source = Column(Enum("model", "reviewer", name="tag_sources"), default="model")
    created_at = Column(DateTime(timezone=True), default=now)
    __table_args__ = (
        Index("ix_tag_lookup", "tag"),  # Regular B-tree index for tag lookups
    )

class QuestionGroup(Base):
    """Question group for bundling logically related questions.
    Verification function allows custom validation per question group.
    """
    __tablename__ = "question_groups"
    id = Column(Integer, primary_key=True)
    title = Column(String(255), unique=True, nullable=False)
    display_title = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    is_reusable = Column(Boolean, default=False)  # TRUE ⇒ can be imported by many schemas
    is_auto_submit = Column(Boolean, default=False)  # TRUE ⇒ answers are automatically submitted for annotation mode
    is_archived = Column(Boolean, default=False)
    verification_function = Column(String(255), nullable=True)  # Name of verification function in verify.py

class Question(Base):
    """Supports both radio and free-text; single-choice values indexed, descriptions not.
    Question text must be unique to prevent confusion and ensure consistent labeling.
    For single-choice questions:
    - options stores the actual values used in answers
    - display_values stores the UI-friendly text for each option
    - option_weights stores the weight for each option (defaults to 1.0)
    - All arrays must have matching lengths
    - For description-type questions, all fields are NULL
    """
    __tablename__ = "questions"
    id = Column(Integer, primary_key=True)
    text = Column(Text, unique=True)
    display_text = Column(Text, nullable=False)  # UI display text, editable
    type = Column(Enum("single", "description", name="question_type"))
    options = Column(JSONB, nullable=True)  # Actual option values used in answers
    display_values = Column(JSONB, nullable=True)  # Display text for options in UI
    option_weights = Column(JSONB, nullable=True)  # Weights for each option
    default_option = Column(String(120), nullable=True)
    is_archived = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=now)

class QuestionGroupQuestion(Base):
    """Many-to-many relationship between questions and groups with ordering."""
    __tablename__ = "question_group_questions"
    question_group_id = Column(Integer, nullable=False)
    question_id = Column(Integer, nullable=False)
    display_order = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('question_group_id', 'question_id'),
    )

class Schema(Base):
    """Reusable question sets; no FK allows forks without cascade.
    """
    __tablename__ = "schemas"
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    instructions_url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    has_custom_display = Column(Boolean, default=False)
    is_archived = Column(Boolean, default=False)

class SchemaQuestionGroup(Base):
    """Many-to-many relationship between schemas and question groups with ordering."""
    __tablename__ = "schema_question_groups"
    schema_id = Column(Integer, nullable=False)
    question_group_id = Column(Integer, nullable=False)
    display_order = Column(Integer, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('schema_id', 'question_group_id'),
    )

class Project(Base):
    """A project = schema + video subset + roles; archiving hides it while retaining history."""
    __tablename__ = "projects"
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)  # Changed from String to Text to match design
    schema_id = Column(Integer, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    is_archived = Column(Boolean, default=False)

class ProjectVideo(Base):
    __tablename__ = "project_videos"
    project_id = Column(Integer, primary_key=True)
    video_id = Column(Integer, primary_key=True)
    added_at = Column(DateTime(timezone=True), default=now)

class ProjectUserRole(Base):
    __tablename__ = "project_user_roles"
    project_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    role = Column(Enum("annotator", "reviewer", "admin", "model", name="project_roles"), nullable=False)
    user_weight = Column(Float, nullable=True, default=1.0)
    assigned_at = Column(DateTime(timezone=True), default=now)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    is_archived = Column(Boolean, default=False)
    __table_args__ = (
        PrimaryKeyConstraint('project_id', 'user_id', 'role'),
        Index("ix_user_projects", "user_id"),  # Index for user's project lookups
    )

class ProjectGroup(Base):
    """Bundles arbitrary projects for export/report dashboards."""
    __tablename__ = "project_groups"
    id = Column(Integer, primary_key=True)
    name = Column(Text, unique=True, nullable=False)  # Changed from String to Text to match design
    description = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now)
    is_archived = Column(Boolean, default=False)

class ProjectGroupProject(Base):
    __tablename__ = "project_group_projects"
    project_group_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, primary_key=True)

class ProjectVideoQuestionDisplay(Base):
    """Custom display overrides for specific project-video-question combinations"""
    __tablename__ = "project_video_question_displays"
    
    project_id = Column(Integer, primary_key=True)
    video_id = Column(Integer, primary_key=True) 
    question_id = Column(Integer, primary_key=True)
    
    # Custom display overrides (nullable - only override what's needed)
    custom_display_text = Column(Text, nullable=True)  # Override question display_text
    custom_display_values = Column(JSONB, nullable=True)  # Override option display_values
    
    created_at = Column(DateTime(timezone=True), default=now)
    updated_at = Column(DateTime(timezone=True), default=now, onupdate=now)
    
    __table_args__ = (
        # Primary key is the composite key above
        Index("ix_project_display_lookup", "project_id"),  # Fast project lookups
        Index("ix_video_display_lookup", "project_id", "video_id"),  # Fast video lookups
    )

class AnnotatorAnswer(Base):
    """Stores annotator submissions with confidence scores and modification history."""
    __tablename__ = "annotator_answers"
    id = Column(Integer, primary_key=True)
    video_id = Column(Integer, nullable=False)
    question_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)  # Annotator who submitted
    project_id = Column(Integer, nullable=False)
    answer_type = Column(Enum("single", "description", name="answer_value_types"), default="single", nullable=False)
    answer_value = Column(Text, nullable=False)
    confidence_score = Column(Float)
    created_at = Column(DateTime(timezone=True), default=now)
    modified_at = Column(DateTime(timezone=True), default=now)
    notes = Column(Text)

    __table_args__ = (
        # Same unique constraint pattern as original Answer table
        UniqueConstraint("video_id", "question_id", "user_id", "project_id", name="uq_annotator_answer_scope"),
        
        # Same index patterns as original Answer table
        Index("ix_annotator_question", "question_id"),  # Query answers by question
        Index("ix_annotator_proj_q", "project_id", "question_id"),  # Query project+question combinations
        Index("ix_proj_q_val_single", "project_id", "question_id", "answer_value", 
              postgresql_where=(answer_type == "single")),  # Fast lookups for single-choice answers
        Index("ix_annotator_vid_q", "video_id", "question_id"),  # Query answers for specific video+question
        Index("ix_annotator_user_proj", "user_id", "project_id"),  # Query annotator's answers for a project
        Index("ix_annotator_user_proj_q", "user_id", "project_id", "question_id"),  # Query annotator's answers for a project+question
    )

class ReviewerGroundTruth(Base):
    """Stores ground truth answers with modification tracking and accuracy metrics."""
    __tablename__ = "reviewer_ground_truth"
    
    # Composite primary key ensures exactly one ground truth per (video, question, project)
    video_id = Column(Integer, primary_key=True)
    question_id = Column(Integer, primary_key=True)
    project_id = Column(Integer, primary_key=True)
    
    # Ground truth metadata
    reviewer_id = Column(Integer, nullable=False)  # Reviewer who created GT
    answer_type = Column(Enum("single", "description", name="answer_value_types"), default="single", nullable=False)
    answer_value = Column(Text, nullable=False)  # Current GT answer
    original_answer_value = Column(Text, nullable=False)  # Original GT answer (for accuracy)
    
    # Modification tracking - two types
    modified_at = Column(DateTime(timezone=True))  # When reviewer modified their own GT (self-correction)
    modified_by_admin_id = Column(Integer)  # Admin who overrode GT (affects accuracy)
    modified_by_admin_at = Column(DateTime(timezone=True))  # When admin overrode GT
    
    # Additional metadata
    confidence_score = Column(Float)
    created_at = Column(DateTime(timezone=True), default=now, nullable=False)
    notes = Column(Text)

    __table_args__ = (
        # Since (video_id, question_id, project_id) is already the PK, we get unique constraint for free
        
        # Similar index patterns adapted for ground truth queries
        Index("ix_gt_question", "question_id"),  # Query GT by question
        Index("ix_gt_proj_q", "project_id", "question_id"),  # Query GT by project+question
        Index("ix_proj_q_val_single_gt", "project_id", "question_id", "answer_value",
              postgresql_where=(answer_type == "single")),  # Fast GT lookups for single-choice
        Index("ix_gt_vid_q", "video_id", "question_id"),  # Query GT for video+question
        
        # Additional indexes specific to ground truth operations
        Index("ix_gt_reviewer", "project_id", "reviewer_id"),  # Calculate reviewer accuracy
        Index("ix_gt_admin_modified", "project_id", "modified_by_admin_id"),  # Find admin-modified GTs
    )

class AnswerReview(Base):
    """Tracks review status and comments for annotator answers. One review per answer."""
    __tablename__ = "answer_reviews"
    id = Column(Integer, primary_key=True)
    answer_id = Column(Integer, nullable=False, unique=True)  # Reference to annotator_answers.id
    reviewer_id = Column(Integer, nullable=False)  # Reviewer who performed the review
    status = Column(Enum("pending", "approved", "rejected", name="review_status"), default="pending")
    comment = Column(Text)
    reviewed_at = Column(DateTime(timezone=True), default=now)

# ---------------- create & smoke-test -------------------------------------
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    engine = create_engine(os.environ["DBURL"], echo=False)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(User(user_id_str="alice", email="alice@example.com", password_hash="x"))
        session.commit()
