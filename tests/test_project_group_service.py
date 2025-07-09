import pytest
from label_pizza.services import ProjectGroupService, ProjectService, SchemaService, QuestionGroupService, QuestionService, VideoService
from label_pizza.models import ProjectGroup, ProjectGroupProject, Project, Video, Schema, Question, QuestionGroup, QuestionGroupQuestion, SchemaQuestionGroup

def test_project_group_service_create_project_group(session, test_project):
    """Test creating a project group."""
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test group description",
        project_ids=[test_project.id],
        session=session
    )
    
    assert group.name == "test_group"
    assert group.description == "Test group description"
    
    # Verify project assignment using get_project_group_by_id
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    assert len(result["projects"]) == 1
    assert result["projects"][0]["id"] == test_project.id

def test_project_group_service_create_project_group_duplicate(session, test_project):
    """Test creating a project group with duplicate name."""
    # Create first group
    ProjectGroupService.create_project_group(
        name="test_group",
        description="Test group description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Try to create second group with same name
    with pytest.raises(ValueError, match="Project group with name 'test_group' already exists"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Another description",
            project_ids=[],
            session=session
        )

def test_project_group_service_create_project_group_invalid_project(session):
    """Test creating a project group with invalid project ID."""
    with pytest.raises(ValueError, match="Project not found"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Test group description",
            project_ids=[999],  # Non-existent project ID
            session=session
        )

def test_project_group_service_edit_project_group(session, test_project):
    """Test editing a project group."""
    # Create initial group
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Initial description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Edit group
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name="updated_group",
        description="Updated description",
        add_project_ids=None,
        remove_project_ids=None,
        session=session
    )
    
    assert updated_group.name == "updated_group"
    assert updated_group.description == "Updated description"
    
    # Verify project assignments unchanged using get_project_group_by_id
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    assert len(result["projects"]) == 1
    assert result["projects"][0]["id"] == test_project.id

def test_project_group_service_edit_project_group_not_found(session):
    """Test editing a non-existent project group."""
    with pytest.raises(ValueError, match="Project group with ID 999 not found"):
        ProjectGroupService.edit_project_group(
            group_id=999,
            name="new_name",
            description="New description",
            add_project_ids=None,
            remove_project_ids=None,
            session=session
        )

def test_project_group_service_edit_project_group_duplicate_name(session, test_project):
    """Test editing a project group to have duplicate name."""
    # Create two groups
    group1 = ProjectGroupService.create_project_group(
        name="group1",
        description="First group",
        project_ids=[],
        session=session
    )
    group2 = ProjectGroupService.create_project_group(
        name="group2",
        description="Second group",
        project_ids=[],
        session=session
    )
    
    # Try to rename group2 to group1
    with pytest.raises(ValueError, match="Project group with name 'group1' already exists"):
        ProjectGroupService.edit_project_group(
            group_id=group2.id,
            name="group1",
            description="Updated description",
            add_project_ids=None,
            remove_project_ids=None,
            session=session
        )

def test_project_group_service_edit_project_group_add_remove_projects(session, test_project):
    """Test adding and removing projects from a project group."""
    # Create another project with a different schema
    schema = Schema(name="test_schema_2")  # Changed schema name to be unique
    session.add(schema)
    session.flush()
    
    project2 = Project(name="test_project2", schema_id=schema.id)
    session.add(project2)
    session.flush()
    
    # Create initial group with one project
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Add second project
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name=None,
        description=None,
        add_project_ids=[project2.id],
        remove_project_ids=None,
        session=session
    )
    
    # Verify both projects are assigned using get_project_group_by_id
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    assert len(result["projects"]) == 2
    project_ids = {p.id for p in result["projects"]}
    assert project_ids == {test_project.id, project2.id}
    
    # Remove first project
    updated_group = ProjectGroupService.edit_project_group(
        group_id=group.id,
        name=None,
        description=None,
        add_project_ids=None,
        remove_project_ids=[test_project.id],
        session=session
    )
    
    # Verify only second project remains using get_project_group_by_id
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    assert len(result["projects"]) == 1
    assert result["projects"][0]["id"] == project2.id

def test_project_group_service_get_project_group_by_id(session, test_project):
    """Test getting a project group by ID."""
    # Create group
    group = ProjectGroupService.create_project_group(
        name="test_group",
        description="Test description",
        project_ids=[test_project.id],
        session=session
    )
    
    # Get group
    result = ProjectGroupService.get_project_group_by_id(group.id, session)
    
    assert result["group"].name == "test_group"
    assert result["group"].description == "Test description"
    assert len(result["projects"]) == 1
    assert result["projects"][0]["id"] == test_project.id

def test_project_group_service_get_project_group_by_id_not_found(session):
    """Test getting a non-existent project group."""
    with pytest.raises(ValueError, match="Project group with ID 999 not found"):
        ProjectGroupService.get_project_group_by_id(999, session)

def test_project_group_service_list_project_groups(session, test_project):
    """Test listing all project groups."""
    # Create multiple groups
    group1 = ProjectGroupService.create_project_group(
        name="group1",
        description="First group",
        project_ids=[test_project.id],
        session=session
    )
    group2 = ProjectGroupService.create_project_group(
        name="group2",
        description="Second group",
        project_ids=[],
        session=session
    )
    
    # List groups
    groups = ProjectGroupService.list_project_groups(session)
    
    assert len(groups) == 2
    group_names = {g["name"] for g in groups}
    assert group_names == {"group1", "group2"}

def test_project_group_service_validate_project_group_uniqueness(session):
    """Test that project group validation prevents adding projects with overlapping content."""
    # Create a question
    question = QuestionService.add_question(
        text="test_question",
        qtype="single",
        options=["yes", "no"],
        default=None,
        session=session
    )
    
    # Create a question group
    question_group = QuestionGroupService.create_group(
        title="test_group",
        display_title="test_group",
        description="Test group",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    
    # Create a schema with the question group
    schema = SchemaService.create_schema(
        name="test_schema",
        question_group_ids=[question_group.id],
        session=session
    )
    
    # Create a video
    VideoService.add_video(
        video_uid="video.mp4",
        url="http://example.com/video.mp4",
        session=session
    )
    video = VideoService.get_video_by_uid("video.mp4", session)
    
    # Create two projects with the same schema and video
    ProjectService.create_project(
        name="project1",
        description="test description",
        schema_id=schema.id,
        video_ids=[video.id],
        session=session
    )
    project1 = ProjectService.get_project_by_name("project1", session)
    
    ProjectService.create_project(
        name="project2",
        description="test description",
        schema_id=schema.id,
        video_ids=[video.id],
        session=session
    )
    project2 = ProjectService.get_project_by_name("project2", session)
    # Try to create a group with both projects - should fail due to overlap
    with pytest.raises(ValueError, match="Cannot group projects .* and .* together. They have .* overlapping videos with shared questions: .*"):
        ProjectGroupService.create_project_group(
            name="test_group",
            description="Test description",
            project_ids=[project1.id, project2.id],
            session=session
        )
    
    # Create a group with one project - should succeed
    ProjectGroupService.create_project_group(
        name="test_group_success",
        description="Test description",
        project_ids=[project1.id],
        session=session
    )

    # Get the group
    group = ProjectGroupService.get_project_group_by_name("test_group_success", session)

    with pytest.raises(ValueError, match="Cannot group projects .* and .* together. They have .* overlapping videos with shared questions: .*"):
        ProjectGroupService.edit_project_group(
            group_id=group.id,
            name="test_group_failed",
            description="Test description",
            add_project_ids=[project2.id],
            remove_project_ids=None,
            session=session
        )