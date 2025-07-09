#!/usr/bin/env python3
"""
Video Cinematography Analysis Test Setup Script

This script creates a complete test environment with:
- Three question groups (camera movement, cinematography mixed, quality check with auto-submit)
- One schema containing all question groups
- One project with uploaded videos
- A model user with random answers (stress testing)
- Focus on cinematographic analysis and camera movement detection
"""

import os
import random
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import all the services
from label_pizza.services import (
    QuestionService, QuestionGroupService, SchemaService, 
    VideoService, ProjectService, AuthService, AnnotatorService
)
from label_pizza.db import init_database
import label_pizza.db as db

def setup_database(database_url_name="DBURL"):
    """Setup database connection"""
    try:
        init_database(database_url_name)
        print(f"✓ Database initialized using {database_url_name}")
        return db.SessionLocal()
    except Exception as e:
        raise ValueError(f"Database initialization failed: {e}")

def create_questions(session):
    """Create questions for all three groups"""
    print("Creating questions...")
    
    def create_or_get_question(text, qtype, options, default, session, display_text, option_weights=None):
        """Helper function to create question or get existing one"""
        try:
            return QuestionService.add_question(
                text=text,
                qtype=qtype,
                options=options,
                default=default,
                session=session,
                display_text=display_text,
                option_weights=option_weights
            )
        except ValueError as e:
            if "already exists" in str(e):
                print(f"  ⚠️  Question already exists, retrieving: '{text}'")
                return QuestionService.get_question_by_text(text, session)
            else:
                raise e
    
    # Questions for Group 1 (Single Choice Only)
    q1_1 = create_or_get_question(
        text="Does the video include shot transitions?",
        qtype="single",
        options=["Yes", "No"],
        default="No",
        session=session,
        display_text="Shot Transitions Present",
        option_weights=[1.2, 1.0]  # Higher weight for detecting transitions
    )
    
    q1_2 = create_or_get_question(
        text="What is the tracking shot type?",
        qtype="single", 
        options=["Side", "Lead", "Tail", "Aerial", "Pan", "Tilt", "Static", "Unknown"],
        default="Static",
        session=session,
        display_text="Camera Movement Type",
        option_weights=[1.0, 1.1, 1.0, 1.3, 1.1, 1.1, 0.8, 0.5]  # Higher weights for dynamic shots
    )
    
    # Questions for Group 2 (Mixed: Single Choice + Description)
    q2_1 = create_or_get_question(
        text="What is the video framing composition?",
        qtype="single",
        options=["Close-up", "Medium shot", "Wide shot", "Extreme close-up", "Extreme wide", "Mixed"],
        default="Medium shot",
        session=session,
        display_text="Shot Composition",
        option_weights=[1.0, 1.2, 1.0, 0.9, 0.8, 1.1]
    )
    
    q2_2 = create_or_get_question(
        text="Describe the camera movement and cinematographic techniques observed",
        qtype="description",
        options=None,
        default=None,
        session=session,
        display_text="Cinematography Analysis"
    )
    
    q2_3 = create_or_get_question(
        text="What is the primary lighting condition?",
        qtype="single",
        options=["Natural daylight", "Natural low light", "Artificial bright", "Artificial dim", "Mixed lighting", "Backlit"],
        default="Natural daylight",
        session=session,
        display_text="Lighting Analysis",
        option_weights=[1.0, 0.9, 1.1, 0.8, 1.0, 0.7]
    )
    
    # Questions for Group 3 (Yes/No with Auto Submit)
    q3_1 = create_or_get_question(
        text="Does the video contain clear visual content without corruption?",
        qtype="single",
        options=["Yes", "No"],
        default="No",
        session=session,
        display_text="Video Quality Check",
        option_weights=[1.0, 1.0]
    )
    
    return {
        'group1': [q1_1["id"], q1_2["id"]],
        'group2': [q2_1["id"], q2_2["id"], q2_3["id"]], 
        'group3': [q3_1["id"]]
    }

def create_question_groups(session, question_ids):
    """Create the three question groups"""
    print("Creating question groups...")
    
    def create_or_get_group(title, description, is_reusable, question_ids, verification_function, is_auto_submit, session):
        """Helper function to create group or get existing one"""
        try:
            return QuestionGroupService.create_group(
                title=title,
                display_title=title,
                description=description,
                is_reusable=is_reusable,
                question_ids=question_ids,
                verification_function=verification_function,
                is_auto_submit=is_auto_submit,
                session=session
            )
        except ValueError as e:
            if "already exists" in str(e):
                print(f"  ⚠️  Question group already exists, retrieving: '{title}'")
                return QuestionGroupService.get_group_by_name(title, session)
            else:
                raise e
    
    # Group 1: Single Choice Only
    group1 = create_or_get_group(
        title="Camera Movement Analysis",
        description="Single choice questions about shot transitions and camera tracking",
        is_reusable=True,
        question_ids=question_ids['group1'],
        verification_function=None,
        is_auto_submit=False,
        session=session
    )
    
    # Group 2: Mixed Questions
    group2 = create_or_get_group(
        title="Cinematography Assessment", 
        description="Mixed question types for comprehensive cinematographic analysis",
        is_reusable=True,
        question_ids=question_ids['group2'],
        verification_function=None,
        is_auto_submit=False,
        session=session
    )
    
    # Group 3: Yes/No with Auto Submit
    group3 = create_or_get_group(
        title="Video Quality Validation",
        description="Quick quality check with auto-submit enabled",
        is_reusable=True,
        question_ids=question_ids['group3'],
        verification_function=None,
        is_auto_submit=True,  # Auto submit enabled
        session=session
    )
    
    return [group1.id, group2.id, group3.id]

def create_schema_and_project(session, group_ids, video_ids):
    """Create schema and project"""
    print("Creating schema...")
    
    # Try to create schema or get existing one
    try:
        schema = SchemaService.create_schema(
            name="Cinematography Analysis Schema",
            question_group_ids=group_ids,
            session=session
        )
        print("✓ Created new schema")
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Schema already exists, retrieving: 'Cinematography Analysis Schema'")
            schema = SchemaService.get_schema_by_name("Cinematography Analysis Schema", session)
        else:
            raise e
    
    print("Creating project...")
    
    # Try to create project or get existing one
    try:
        ProjectService.create_project(
            name="Video Cinematography Stress Test",
            description="Stress test for video cinematography analysis for auto-submit functionality",
            schema_id=schema.id,
            video_ids=video_ids,
            session=session
        )
        print("✓ Created new project")
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Project already exists, retrieving: 'Video Cinematography Stress Test'")
        else:
            raise e
    
    # Get the project (whether new or existing)
    project = ProjectService.get_project_by_name("Video Cinematography Stress Test", session)
    return project.id

def upload_videos(session):
    """Upload all the video URLs"""
    print("Uploading videos...")
    
    video_urls = {
        "RGSN4S5jn4o.0.0.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/RGSN4S5jn4o.0.0.mp4",
        "PIvOfcR77SQ.4.3.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/PIvOfcR77SQ.4.3.mp4", 
        "PrjpmqAsCZQ.0.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/PrjpmqAsCZQ.0.1.mp4",
        "1934.1.26.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1934.1.26.mp4",
        "1470.0.35.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1470.0.35.mp4",
        "fSWFUFdV5TU.0.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/fSWFUFdV5TU.0.1.mp4",
        "1560.0.17.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1560.0.17.mp4", 
        "1470.0.34.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1470.0.34.mp4",
        "1018.1.0.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1018.1.0.mp4",
        "0UthxdAH0ks.0.2.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/0UthxdAH0ks.0.2.mp4",
        "sKJeTaIEldM.1.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/sKJeTaIEldM.1.1.mp4",
        "uWCGK4nneeU.4.5.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/uWCGK4nneeU.4.5.mp4",
        "1934.1.25.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1934.1.25.mp4",
        "190.2.14.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/190.2.14.mp4",
        "tCRbVEGHZlQ.0.4.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/tCRbVEGHZlQ.0.4.mp4",
        "mlasEBKtDAM.2.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/mlasEBKtDAM.2.1.mp4",
        "x6P57x1gx94.0.4.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/x6P57x1gx94.0.4.mp4"
    }
    
    video_ids = []
    for filename, url in video_urls.items():
        try:
            VideoService.add_video(video_uid=filename, url=url, session=session)
            video = VideoService.get_video_by_uid(filename, session)
            video_ids.append(video.id)
            print(f"✓ Uploaded: {filename}")
        except ValueError as e:
            if "already exists" in str(e):
                print(f"  ⚠️  Video already exists, retrieving: {filename}")
                video = VideoService.get_video_by_uid(filename, session)
                video_ids.append(video.id)
            else:
                print(f"✗ Failed to upload {url}: {e}")
        except Exception as e:
            print(f"✗ Failed to upload {url}: {e}")
    
    return video_ids

def create_model_user(session, project_id):
    """Create a model user and assign to project"""
    print("Creating model user...")
    
    # Try to create model user or get existing one
    try:
        model_user = AuthService.create_user(
            user_id="cinematography_model_v1",
            email=None,  # Model users don't have emails
            password_hash="model_password_hash",
            user_type="model",
            session=session
        )
        print("✓ Created new model user")
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Model user already exists, retrieving: 'cinematography_model_v1'")
            model_user = AuthService.get_user_by_id("cinematography_model_v1", session)
        else:
            raise e
    
    # Try to assign model user to project (may already be assigned)
    try:
        ProjectService.add_user_to_project(
            project_id=project_id,
            user_id=model_user.id,
            role="model",
            session=session,
            user_weight=1.5  # Higher weight for testing
        )
        print("✓ Assigned model user to project")
    except ValueError as e:
        if "must be a model" in str(e) or "already" in str(e).lower():
            print("  ⚠️  Model user already assigned to project")
        else:
            print(f"  ⚠️  Assignment issue (continuing anyway): {e}")
    
    print(f"✓ Model user ready with ID: {model_user.id}")
    return model_user.id

def generate_stress_test_answers(session, project_id, model_user_id, group_ids, video_ids):
    """Generate random answers with confidence scores for stress testing"""
    print("Generating stress test answers...")
    
    # Get all questions for each group
    group_questions = {}
    for group_id in group_ids:
        questions = QuestionService.get_questions_by_group_id(group_id, session)
        group_questions[group_id] = questions
    
    total_answers = 0
    skipped_existing = 0
    
    for video_id in video_ids:
        for group_id in group_ids:
            questions = group_questions[group_id]
            
            # Check if answers already exist for this video/group combination
            existing_answers = {}
            try:
                existing_answers = AnnotatorService.get_user_answers_for_question_group(
                    video_id=video_id,
                    project_id=project_id,
                    user_id=model_user_id,
                    question_group_id=group_id,
                    session=session
                )
            except:
                pass
            
            # Skip if all questions already have answers
            if len(existing_answers) == len(questions):
                skipped_existing += len(questions)
                continue
            
            answers = {}
            confidence_scores = {}
            
            for question in questions:
                # Skip if this specific question already has an answer
                if question["text"] in existing_answers and existing_answers[question["text"]]:
                    continue
                    
                if question["type"] == "single":
                    # For stress testing: make sure one option is never selected
                    available_options = question["options"][:-1]  # Exclude last option
                    selected_option = random.choice(available_options)
                    answers[question["text"]] = selected_option
                    
                    # Random confidence score between 0.5 and 0.95
                    confidence_scores[question["text"]] = round(random.uniform(0.5, 0.95), 2)
                    
                elif question["type"] == "description":
                    # Generate random cinematography descriptions
                    descriptions = [
                        "The camera employs steady tracking movements with smooth transitions between shots.",
                        "Multiple shot compositions are evident with dynamic panning and tilting motions.",
                        "The cinematography features static framing with occasional subtle camera adjustments.",
                        "Handheld camera work creates natural movement with varying shot compositions.",
                        "Professional camera work with deliberate framing choices and smooth movements.",
                        "The video shows standard cinematographic techniques with conventional shot patterns."
                    ]
                    answers[question["text"]] = random.choice(descriptions)
                    confidence_scores[question["text"]] = round(random.uniform(0.6, 0.9), 2)
            
            # Only submit if we have new answers
            if answers:
                try:
                    # Submit answers for this question group
                    AnnotatorService.submit_answer_to_question_group(
                        video_id=video_id,
                        project_id=project_id,
                        user_id=model_user_id,
                        question_group_id=group_id,
                        answers=answers,
                        session=session,
                        confidence_scores=confidence_scores
                    )
                    total_answers += len(answers)
                    
                except Exception as e:
                    print(f"  ⚠️  Failed to submit answers for video {video_id}, group {group_id}: {e}")
    
    print(f"✓ Generated {total_answers} new test answers")
    if skipped_existing > 0:
        print(f"  ⚠️  Skipped {skipped_existing} existing answers")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Set up cinematography analysis test data")
    parser.add_argument(
        "--database-url-name",
        default="DBURL",
        help="Environment variable name for database URL (default: DBURL)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🎬 VIDEO CINEMATOGRAPHY ANALYSIS SETUP")
    print("=" * 60)
    
    try:
        # Setup database
        session = setup_database(args.database_url_name)
        print("✓ Database connection established")
        
        # Create questions
        question_ids = create_questions(session)
        print("✓ Questions ready for all groups")
        
        # Create question groups
        group_ids = create_question_groups(session, question_ids)
        print("✓ Question groups ready")
        print(f"  - Group 1 (Camera Movement): {group_ids[0]}")
        print(f"  - Group 2 (Cinematography Mixed): {group_ids[1]}")
        print(f"  - Group 3 (Quality Check Auto-Submit): {group_ids[2]}")
        
        # Upload videos
        video_ids = upload_videos(session)
        print(f"✓ Videos ready: {len(video_ids)} total")
        
        # Create schema and project
        project_id = create_schema_and_project(session, group_ids, video_ids)
        print(f"✓ Schema and project ready (Project ID: {project_id})")
        
        # Create model user
        model_user_id = create_model_user(session, project_id)
        
        # Generate stress test answers
        generate_stress_test_answers(session, project_id, model_user_id, group_ids, video_ids)
        
        print("\n" + "=" * 60)
        print("🎉 SETUP COMPLETE!")
        print("=" * 60)
        print(f"Project ID: {project_id}")
        print(f"Model User ID: {model_user_id}")
        print(f"Question Groups: {group_ids}")
        print(f"Videos: {len(video_ids)} uploaded")
        print(f"Schema: 'Cinematography Analysis Schema'")
        print(f"Project: 'Video Cinematography Stress Test'")
        print("\nStress Test Notes:")
        print("- Model user has weight 1.5")
        print("- Random confidence scores (0.5-0.95)")
        print("- Last option in each single-choice question is never selected")
        print("- Group 3 (Video Quality Validation) has auto-submit enabled")
        print("- Questions focus on cinematography and camera movement analysis")
        print("- Script handles existing data gracefully (re-runnable)")
        
        print("\n" + "=" * 60)
        print("🔧 NEXT STEPS:")
        print("=" * 60)
        print("- Use the Project ID to access your test environment")
        print("- Test auto-submit functionality with Group 3")
        print("- Verify stress test conditions (last options never selected)")
        print("- Check weighted voting algorithms with the model user data")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise
    
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    main()