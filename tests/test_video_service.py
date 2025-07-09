import pytest
import pandas as pd
from label_pizza.services import VideoService, ProjectService, SchemaService, QuestionService, QuestionGroupService, GroundTruthService, AnnotatorService

def test_video_service_get_all_videos(session):
    """Test getting all videos."""
    # Create a video
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert df.iloc[0]["Archived"] == False

def test_video_service_get_video_by_uid(session):
    """Test getting a video by UID."""
    # Create a video
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    
    # Get video by UID
    found_video = VideoService.get_video_by_uid("test.mp4", session)
    assert found_video is not None
    assert found_video.video_uid == "test.mp4"
    assert found_video.url == "http://example.com/test.mp4"
    
    # Test non-existent video
    not_found = VideoService.get_video_by_uid("nonexistent.mp4", session)
    assert not_found is None

def test_video_service_get_video_url(session):
    """Test getting a video's URL."""
    # Create a video
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    
    # Get video by UID first to get its ID
    video = VideoService.get_video_by_uid("test.mp4", session)
    
    # Get URL
    url = VideoService.get_video_url(video.id, session)
    assert url == "http://example.com/test.mp4"
    
    # Test non-existent video
    with pytest.raises(ValueError, match="not found"):
        VideoService.get_video_url(999, session)

def test_video_service_get_video_metadata(session):
    """Test getting a video's metadata."""
    # Create a video with metadata
    metadata = {"duration": 120, "resolution": "1080p"}
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session, metadata=metadata)
    
    # Get video by UID first to get its ID
    video = VideoService.get_video_by_uid("test.mp4", session)
    
    # Get metadata
    found_metadata = VideoService.get_video_metadata(video.id, session)
    assert found_metadata == metadata
    
    # Test non-existent video
    with pytest.raises(ValueError, match="not found"):
        VideoService.get_video_metadata(999, session)

def test_video_service_archive_video(session):
    """Test archiving a video."""
    # Create a video
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    
    # Get video by UID first to get its ID
    video = VideoService.get_video_by_uid("test.mp4", session)
    
    # Archive video
    VideoService.archive_video(video.id, session)
    
    # Verify video is archived
    df = VideoService.get_all_videos(session)
    assert df.iloc[0]["Archived"] == True
    
    # Test archiving non-existent video
    with pytest.raises(ValueError, match="not found"):
        VideoService.archive_video(999, session)

def test_video_service_get_videos_with_project_status(session, test_video):
    """Test getting videos with project status."""
    # Create a question
    QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    question = QuestionService.get_question_by_text("test question", session)
    
    # Create a question group with the question
    question_group = QuestionGroupService.create_group(
        title="test_group",
        display_title="test_group",
        description="test description",
        is_reusable=True,
        question_ids=[question["id"]],
        verification_function=None,
        session=session
    )
    
    # Create schema with the question group
    schema = SchemaService.create_schema(
        name="test_schema",
        question_group_ids=[question_group.id],
        session=session
    )
    
    # Create project with video
    ProjectService.create_project(
        name="test_project_with_status",
        description="test description",
        schema_id=schema.id,
        video_ids=[test_video.id],
        session=session
    )
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == test_video.video_uid
    assert df.iloc[0]["URL"] == test_video.url
    assert "test_project_with_status: ✗" in df.iloc[0]["Projects"]  # No ground truth yet

def test_video_service_get_videos_with_ground_truth(session, test_video, test_user):
    """Test getting videos with ground truth status."""
    # Create a question
    question = QuestionService.add_question(
        text="test question",
        qtype="single",
        options=["option1", "option2"],
        default="option1",
        session=session
    )
    
    # Create a question group with the question
    question_group = QuestionGroupService.create_group(
        title="test_group_gt",
        display_title="test_group_gt",
        description="test description",
        is_reusable=True,
        question_ids=[question.id],
        verification_function=None,
        session=session
    )
    
    # Create schema with the question group
    schema = SchemaService.create_schema(
        name="test_schema_gt",
        question_group_ids=[question_group.id],
        session=session
    )
    
    # Create project with video
    ProjectService.create_project(
        name="test_project_with_gt",
        description="test description",
        schema_id=schema.id,
        video_ids=[test_video.id],
        session=session
    )
    project = ProjectService.get_project_by_name("test_project_with_gt", session)
    
    # Add admin role to user
    ProjectService.add_user_to_project(
        project_id=project.id,
        user_id=test_user.id,
        role="admin",
        session=session
    )
    session.commit()  # Commit to ensure role is available
    
    # Add ground truth answer for all questions in the group
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=project.id,
        reviewer_id=test_user.id,
        question_group_id=question_group.id,
        answers={question.text: "option1"},  # Answer for the question
        session=session
    )
    
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == test_video.video_uid
    assert df.iloc[0]["URL"] == test_video.url
    assert "test_project_with_gt: ✓" in df.iloc[0]["Projects"]  # Has ground truth for all questions

def test_video_service_add_video(session):
    """Test adding a new video."""
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)
    
    # Verify video was added
    video = VideoService.get_video_by_uid("test.mp4", session)
    assert video is not None
    assert video.video_uid == "test.mp4"
    assert video.url == "http://example.com/test.mp4"
    assert not video.is_archived

def test_video_service_add_video_duplicate(session, test_video):
    """Test adding a duplicate video."""
    with pytest.raises(ValueError, match="already exists"):
        VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session)

def test_video_service_add_video_invalid_url(session):
    """Test adding a video with invalid URL."""
    with pytest.raises(ValueError, match="must start with http:// or https://"):
        VideoService.add_video(video_uid="invalid_url", url="invalid_url", session=session)

def test_video_service_add_video_special_chars(session):
    """Test adding a video with special characters in filename."""
    VideoService.add_video(video_uid="test video (1).mp4", url="http://example.com/test video (1).mp4", session=session)
    
    # Verify video was added
    video = VideoService.get_video_by_uid("test video (1).mp4", session)
    assert video is not None
    assert video.video_uid == "test video (1).mp4"
    assert video.url == "http://example.com/test video (1).mp4"

def test_video_service_add_video_query_params(session):
    """Test adding a video with query parameters in URL."""
    VideoService.add_video(video_uid="test.mp4?param=value", url="http://example.com/test.mp4?param=value", session=session)
    
    # Verify video was added
    video = VideoService.get_video_by_uid("test.mp4?param=value", session)
    assert video is not None
    assert video.video_uid == "test.mp4?param=value"
    assert video.url == "http://example.com/test.mp4?param=value"

def test_video_service_get_all_videos_empty(session):
    """Test getting all videos when none exist."""
    df = VideoService.get_all_videos(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_video_metadata_validation(session):
    """Test video metadata validation."""
    # Test invalid metadata types
    with pytest.raises(ValueError, match="must be a dictionary"):
        VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session, metadata="invalid")

    with pytest.raises(ValueError, match="Invalid metadata value type"):
        VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session, metadata={"key": object()})
    
    # Test valid metadata
    metadata = {
        "duration": 120,
        "resolution": "1080p",
        "tags": ["action", "drama"],
        "info": {"year": 2023, "genre": "action"}
    }
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session, metadata=metadata)
    
    # Verify metadata was added
    video = VideoService.get_video_by_uid("test.mp4", session)
    assert video.video_metadata == metadata

def test_video_uid_special_chars(session):
    """Test various special characters in video UIDs."""
    test_cases = [
        "test video.mp4",
        "test-video.mp4",
        "test_video.mp4",
        "test.video.mp4",
        "test (1).mp4",
        "test[1].mp4",
        "test{1}.mp4",
        "test&1.mp4",
        "test#1.mp4",
        "test@1.mp4",
        "test!1.mp4",
        "test$1.mp4",
        "test%1.mp4",
        "test^1.mp4",
        "test*1.mp4",
        "test+1.mp4",
        "test=1.mp4",
        "test|1.mp4",
        "test\\1.mp4",
        "test?1.mp4",
        "test:1.mp4",
        "test;1.mp4",
        "test'1.mp4",
        "test\"1.mp4",
        "test<1.mp4",
        "test>1.mp4",
        "test,1.mp4"
    ]
    
    for uid in test_cases:
        url = f"http://example.com/{uid}"
        VideoService.add_video(video_uid=uid, url=url, session=session)
        
        # Verify video was added by checking get_video_by_uid
        video = VideoService.get_video_by_uid(uid, session)
        assert video is not None
        assert video.video_uid == uid
        assert video.url == url


def test_video_service_get_all_videos_multiple_projects(session, test_video):
    """Test getting videos that belong to multiple projects."""
    # Create multiple projects through service layer
    projects = []
    for i in range(2):
        # Create a question first
        QuestionService.add_question(
            text=f"test question {i}",
            qtype="single",
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        question = QuestionService.get_question_by_text(f"test question {i}", session)
        
        # Create question group with the question
        group = QuestionGroupService.create_group(
            title=f"test_group{i}",
            display_title=f"test_group{i}",
            description="test description",
            is_reusable=True,
            question_ids=[question["id"]],  # Add the question to the group
            verification_function=None,
            session=session
        )
        
        # Create schema with the question group
        schema = SchemaService.create_schema(
            name=f"test_schema{i}",
            question_group_ids=[group.id],
            session=session
        )
        
        ProjectService.create_project(
            name=f"test_project{i}",
            description="test description",
            schema_id=schema.id,
            video_ids=[test_video.id],
            session=session
        )
        project = ProjectService.get_project_by_name(f"test_project{i}", session)
        projects.append(project)
    
    # Get all videos
    df = VideoService.get_videos_with_project_status(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == test_video.video_uid
    
    # Check project status
    project_status = df.iloc[0]["Projects"]
    assert "test_project0: ✗" in project_status  # No ground truth for project 0
    assert "test_project1: ✗" in project_status  # No ground truth for project 1

def test_video_service_get_all_videos_with_metadata(session):
    """Test getting videos with metadata."""
    # Create a video with metadata
    metadata = {"duration": 120, "resolution": "1080p"}
    VideoService.add_video(video_uid="test.mp4", url="http://example.com/test.mp4", session=session, metadata=metadata)
    
    df = VideoService.get_all_videos(session)
    assert len(df) == 1
    assert df.iloc[0]["Video UID"] == "test.mp4"
    assert df.iloc[0]["URL"] == "http://example.com/test.mp4"
    assert df.iloc[0]["Archived"] == False


def test_video_service_get_all_videos_with_review(session, test_video, test_user):
    """Test getting videos with review status."""
    # Add questions to group through service layer
    questions = []
    for i in range(2):
        question_text = f"test question {i}"
        QuestionService.add_question(
            text=question_text,
            qtype="single",
            options=["option1", "option2"],
            default="option1",
            session=session
        )
        question = QuestionService.get_question_by_text(question_text, session)
        questions.append(question)
    
    # Create a question group with the questions
    question_group = QuestionGroupService.create_group(
        title="test_group_review",
        display_title="test_group_review",
        description="test description",
        is_reusable=True,
        question_ids=[q["id"] for q in questions],
        verification_function=None,
        session=session
    )
    
    # Create schema with the question group
    schema = SchemaService.create_schema(
        name="test_schema_review",
        question_group_ids=[question_group.id],
        session=session
    )
    
    # Create project with the new schema
    ProjectService.create_project(
        name="test_project_with_review",
        description="test description",
        schema_id=schema.id,  # Use the new schema with questions
        video_ids=[test_video.id],
        session=session
    )
    project = ProjectService.get_project_by_name("test_project_with_review", session)
    
    # Add admin role to user
    ProjectService.add_user_to_project(
        project_id=project.id,
        user_id=test_user.id,
        role="admin",
        session=session
    )
    session.commit()  # Commit to ensure role is available
    
    # Add all answers at once using the new method
    AnnotatorService.submit_answer_to_question_group(
        video_id=test_video.id,
        project_id=project.id,
        user_id=test_user.id,
        question_group_id=question_group.id,
        answers={q["text"]: "option1" for q in questions},  # Answer for all questions
        session=session
    )
    
    # Add ground truth for all questions
    GroundTruthService.submit_ground_truth_to_question_group(
        video_id=test_video.id,
        project_id=project.id,
        reviewer_id=test_user.id,
        question_group_id=question_group.id,
        answers={q["text"]: "option1" for q in questions},  # Ground truth for all questions
        session=session
    )
    
    # Get progress
    progress = ProjectService.progress(project.id, session)
    assert progress["total_videos"] == 1
    assert progress["total_questions"] == 2
    assert progress["total_answers"] == 2
    assert progress["ground_truth_answers"] == 2  # Both questions have ground truth
    assert progress["completion_percentage"] == 100.0  # All questions have ground truth 