import pytest
from label_pizza.services import ProjectService, SchemaService, QuestionService, QuestionGroupService, VideoService
import pandas as pd

def test_project_service_create_project(session, test_schema, test_video):
    """Test creating a new project."""
    ProjectService.create_project(
        name="test_project",
        schema_id=test_schema.id,
        video_ids=[test_video.id],
        session=session
    )
    
    # Verify project was created
    project = ProjectService.get_project_by_name("test_project", session)
    assert project.name == "test_project"
    assert project.schema_id == test_schema.id
    assert not project.is_archived

def test_project_service_create_project_duplicate(session, test_project):
    """Test creating a project with duplicate name."""
    with pytest.raises(ValueError, match="Project with name 'test_project' already exists"):
        ProjectService.create_project(
            name="test_project",
            description="test description",
            schema_id=test_project.schema_id,
            video_ids=[],
            session=session
        )

def test_project_service_create_project_invalid_schema(session, test_video):
    """Test creating a project with invalid schema."""
    with pytest.raises(ValueError, match="Schema with ID 999 not found"):
        ProjectService.create_project(
            name="test_project",
            description="test description",
            schema_id=999,  # Non-existent schema ID
            video_ids=[test_video.id],
            session=session
        )

def test_project_service_create_project_invalid_video(session, test_schema):
    """Test creating a project with invalid video."""
    with pytest.raises(ValueError, match="Video with ID 999 not found"):
        ProjectService.create_project(
            name="test_project",
            description="test description",
            schema_id=test_schema.id,
            video_ids=[999],  # Non-existent video ID
            session=session
        )

def test_project_service_get_project_by_name(session, test_project):
    """Test getting a project by name."""
    project = ProjectService.get_project_by_name("test_project", session)
    assert project.id == test_project.id
    assert project.name == "test_project"

def test_project_service_get_project_by_name_not_found(session):
    """Test getting a non-existent project by name."""
    with pytest.raises(ValueError, match="Project with name 'non_existent_project' not found"):
        ProjectService.get_project_by_name("non_existent_project", session)

def test_project_service_get_project_by_name_archived(session, test_project):
    """Test getting an archived project by name."""
    ProjectService.archive_project(test_project.id, session)
    project = ProjectService.get_project_by_name("test_project", session)
    assert project.is_archived

def test_project_service_archive_project(session, test_project):
    """Test archiving a project."""
    ProjectService.archive_project(test_project.id, session)
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.is_archived

def test_project_service_archive_project_not_found(session):
    """Test archiving a non-existent project."""
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.archive_project(999, session)

def test_project_service_get_project_by_id(session, test_project):
    """Test getting a project by ID."""
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.name == "test_project"
    assert project.schema_id == test_project.schema_id

def test_project_service_get_project_by_id_not_found(session):
    """Test getting a non-existent project by ID."""
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.get_project_by_id(999, session)

def test_project_service_get_project_by_id_archived(session, test_project):
    """Test getting an archived project by ID."""
    ProjectService.archive_project(test_project.id, session)
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.is_archived

def test_project_service_get_all_projects(session, test_project):
    """Test getting all projects."""
    projects_df = ProjectService.get_all_projects(session)
    assert len(projects_df) == 1
    assert projects_df.iloc[0]["ID"] == test_project.id
    assert projects_df.iloc[0]["Name"] == "test_project"

def test_project_service_get_all_projects_empty(session):
    """Test getting all projects when none exist."""
    projects_df = ProjectService.get_all_projects(session)
    assert len(projects_df) == 0

def test_project_service_get_project_videos(session, test_project):
    """Test getting videos in a project."""
    videos_df = VideoService.get_videos_with_project_status(session)
    project_videos = videos_df[videos_df["Projects"].str.contains("test_project")]
    assert len(project_videos) == 1
    assert project_videos.iloc[0]["Video UID"] == "test.mp4"

def test_project_service_get_project_videos_not_found(session):
    """Test getting videos for a non-existent project."""
    videos_df = VideoService.get_videos_with_project_status(session)
    assert len(videos_df) == 0


def test_project_service_get_project_schema(session, test_project, test_schema):
    """Test getting schema for a project."""
    schema = SchemaService.get_schema_by_id(test_project.schema_id, session)
    assert schema.id == test_schema.id
    assert schema.name == "test_schema"

def test_project_service_get_project_schema_not_found(session):
    """Test getting schema for a non-existent project."""
    with pytest.raises(ValueError, match="Project with ID 999 not found"):
        ProjectService.get_project_by_id(999, session)

def test_project_service_get_project_schema_archived(session, test_project):
    """Test getting schema for an archived project."""
    ProjectService.archive_project(test_project.id, session)
    project = ProjectService.get_project_by_id(test_project.id, session)
    assert project.is_archived
    schema = SchemaService.get_schema_by_id(project.schema_id, session)
    assert schema.id == test_project.schema_id 