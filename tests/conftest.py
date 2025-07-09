import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from label_pizza.models import (
    Video, Project, ProjectVideo, Schema, QuestionGroup,
    Question, ProjectUserRole, AnnotatorAnswer, ReviewerGroundTruth, User, AnswerReview,
    QuestionGroupQuestion, SchemaQuestionGroup
)
from label_pizza.services import (
    VideoService, ProjectService, SchemaService, QuestionService,
    QuestionGroupService, AuthService, AnnotatorService, GroundTruthService
)
from label_pizza.db import Base, test_engine, TestSessionLocal, init_test_db, get_test_session
from dotenv import load_dotenv
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

load_dotenv()

@pytest.fixture(scope="session")
def engine():
    """Create a test database engine."""
    return test_engine

@pytest.fixture(scope="function")
def tables(engine):
    """Create and drop all tables for each test."""
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture(scope="function")
def session(engine, tables):
    """Create a new database session for a test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()

# Test data setup
@pytest.fixture
def test_user(session):
    AuthService.create_user(
        user_id="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session,
        is_archived=False
    )
    return AuthService.get_user_by_id("test_user", session)

@pytest.fixture
def test_video(session):
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    return VideoService.get_video_by_uid("test.mp4", session)

@pytest.fixture
def test_schema(session):
    """Create a test schema."""
    # Create a question first
    QuestionService.add_question(
        text="test question for schema",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_values=["Option 1", "Option 2"],
        display_text="Test Question for Schema"
    )
    question = QuestionService.get_question_by_text("test question for schema", session)
    
    # Create a question group with the question
    group = QuestionGroupService.create_group(
        title="test_group_for_schema",
        display_title="test_group_for_schema",
        description="test description",
        is_reusable=True,
        question_ids=[question["id"]],
        verification_function=None,
        session=session
    )
    
    # Create schema with the question group
    schema = SchemaService.create_schema("test_schema", [group.id], session)
    return schema

@pytest.fixture
def test_question_group(session):
    """Create a test question group."""
    # Create a question first
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_values=["Option 1", "Option 2"],
        display_text="Test Question"
    )
    question = QuestionService.get_question_by_text("test question", session)
    
    # Create question group with the question
    group = QuestionGroupService.create_group(
        title="test_group",
        display_title="test_group",
        description="test description",
        is_reusable=True,
        question_ids=[question["id"]],  # Add the question to the group
        verification_function=None,
        session=session
    )
    
    return group

@pytest.fixture
def test_question(session):
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session,
        display_values=["Option 1", "Option 2"],
        display_text="Test Question"
    )
    return QuestionService.get_question_by_text("test question", session)

@pytest.fixture
def test_project(session, test_schema, test_video):
    ProjectService.create_project(
        name="test_project",
        description="test description",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    return ProjectService.get_project_by_name("test_project", session) 