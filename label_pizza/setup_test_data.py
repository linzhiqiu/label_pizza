#!/usr/bin/env python3
"""
Label Pizza Test Data Setup Script - Compatible Version
=======================================================
Creates realistic test data including users, videos, projects, and assignments
for testing the video annotation platform.

Compatible with existing db.py structure.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict

# Add current directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from label_pizza.db import init_database
    from label_pizza.models import Base
    from label_pizza.services import (
        VideoService, QuestionService, QuestionGroupService, 
        SchemaService, ProjectService, AuthService, ProjectGroupService
    )
    print("✅ Successfully imported required modules")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all required files (db.py, services.py, models.py) are in the same directory")
    sys.exit(1)

args = argparse.ArgumentParser(add_help=False)
args.add_argument("--database-url-name", default="DBURL")
args, _ = args.parse_known_args()

try:
    init_database(args.database_url_name)
    print(f"✅ Database initialized using {args.database_url_name}")
    from label_pizza.db import engine, SessionLocal
except Exception as e:
    print(f"❌ Database initialization failed: {e}")
    sys.exit(1)

class TestDataSetup:
    """Handles creation of comprehensive test data for Label Pizza"""
    
    def __init__(self):
        self.test_videos = {
            "RGSN4S5jn4o.0.0.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/RGSN4S5jn4o.0.0.mp4",
            "PIvOfcR77SQ.4.3.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/PIvOfcR77SQ.4.3.mp4",
            "PrjpmqAsCZQ.0.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/PrjpmqAsCZQ.0.1.mp4",
            "1934.1.26.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1934.1.26.mp4",
            "1470.0.35.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1470.0.35.mp4",
            "fSWFUFdV5TU.0.1.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/fSWFUFdV5TU.0.1.mp4",
            "1560.0.17.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1560.0.17.mp4",
            "1470.0.34.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1470.0.34.mp4",
            "1018.1.0.mp4": "https://huggingface.co/datasets/zhiqiulin/video_captioning/resolve/main/1018.1.0.mp4",
        }
        
        self.created_items = {
            "users": [],
            "videos": [],
            "questions": [],
            "question_groups": [],
            "schemas": [],
            "projects": [],
            "project_groups": []
        }
    
    def run_setup(self):
        """Run the complete test data setup"""
        print("🍕 Starting Label Pizza Test Data Setup")
        print("=" * 50)
        
        # Check database connection first
        if not self._check_database():
            return False
        
        try:
            with SessionLocal() as session:
                # Step 1: Create test users
                print("\n👥 Creating test users...")
                self._create_test_users(session)
                
                # Step 2: Add test videos
                print("\n📹 Adding test videos...")
                self._create_test_videos(session)
                
                # Step 3: Create questions and question groups
                print("\n❓ Creating questions and question groups...")
                self._create_questions_and_groups(session)
                
                # Step 4: Create schemas
                print("\n📋 Creating schemas...")
                self._create_schemas(session)
                
                # Step 5: Create projects
                print("\n📁 Creating projects...")
                self._create_projects(session)
                
                # Step 6: Create project groups
                print("\n🗂️ Creating project groups...")
                self._create_project_groups(session)
                
                # Step 7: Assign users to projects
                print("\n🔗 Assigning users to projects...")
                self._assign_users_to_projects(session)
                
                # Step 8: Summary
                print("\n📊 Setup Summary:")
                self._print_summary()
                
                print("\n🎉 Test data setup completed successfully!")
                print("\nYou can now test the application with:")
                print("👥 Annotators: alice@example.com, bob@example.com (password: password123)")
                print("🔍 Reviewer: carol@example.com (password: password123)")
                print("⚙️ Admin: admin@example.com (password: password123)")
                
                return True
                
        except Exception as e:
            print(f"❌ Error during setup: {str(e)}")
            import traceback
            print("Full error details:")
            traceback.print_exc()
            return False
    
    def _check_database(self) -> bool:
        """Check database connectivity"""
        print("🔍 Checking database connection...")
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Database connection healthy")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
    
    def _create_test_users(self, session):
        """Create test annotators and reviewer"""
        test_users = [
            {
                "user_id": "alice_annotator",
                "email": "alice@example.com", 
                "password": "password123",
                "user_type": "human",
                "description": "Senior Video Annotator"
            },
            {
                "user_id": "bob_annotator", 
                "email": "bob@example.com",
                "password": "password123", 
                "user_type": "human",
                "description": "Junior Video Annotator"
            },
            {
                "user_id": "carol_reviewer",
                "email": "carol@example.com",
                "password": "password123",
                "user_type": "human", 
                "description": "Video Annotation Reviewer"
            },
            {
                "user_id": "admin_user",
                "email": "admin@example.com",
                "password": "password123",
                "user_type": "admin",
                "description": "Admin User"
            }
        ]
        
        for user_data in test_users:
            try:
                user = AuthService.create_user(
                    user_id=user_data["user_id"],
                    email=user_data["email"],
                    password_hash=user_data["password"],  # In production, this should be hashed
                    user_type=user_data["user_type"],
                    session=session
                )
                self.created_items["users"].append({
                    "name": user_data["user_id"],
                    "email": user_data["email"],
                    "role": user_data["description"]
                })
                print(f"   ✅ Created user: {user_data['user_id']} ({user_data['email']})")
                
            except Exception as e:
                print(f"   ⚠️ User {user_data['user_id']} might already exist: {str(e)}")
    
    def _create_test_videos(self, session):
        """Add test videos with metadata"""
        video_metadata = [
            {"title": "Outdoor Scene 1", "category": "nature", "duration": "unknown"},
            {"title": "Indoor Activity", "category": "indoor", "duration": "unknown"}, 
            {"title": "Person Movement", "category": "action", "duration": "unknown"},
            {"title": "Historical Footage", "category": "historical", "duration": "unknown"},
            {"title": "Daily Activity 1", "category": "lifestyle", "duration": "unknown"},
            {"title": "Transportation Scene", "category": "transport", "duration": "unknown"},
            {"title": "Social Interaction", "category": "social", "duration": "unknown"},
            {"title": "Daily Activity 2", "category": "lifestyle", "duration": "unknown"},
            {"title": "Work Environment", "category": "workplace", "duration": "unknown"},
        ]
        
        video_uids = list(self.test_videos.keys())
        for i, (video_uid, metadata) in enumerate(zip(video_uids, video_metadata)):
            try:
                VideoService.add_video(video_uid=video_uid, url=self.test_videos[video_uid], session=session, metadata=metadata)
                self.created_items["videos"].append({
                    "title": metadata["title"],
                    "category": metadata["category"],
                    "url": self.test_videos[video_uid]
                })
                print(f"   ✅ Added video: {metadata['title']}")
                
            except Exception as e:
                print(f"   ⚠️ Video {metadata['title']} might already exist: {str(e)}")
    
    def _create_questions_and_groups(self, session):
        """Create comprehensive questions and organize them into groups"""
        
        # Question Group 1: Visual Elements
        visual_questions = [
            {
                "text": "What is the primary object or subject in this video?",
                "type": "single",
                "options": ["Person", "Animal", "Vehicle", "Building", "Nature", "Multiple objects", "Other"],
                "default": "Person"
            },
            {
                "text": "What is the setting or environment?", 
                "type": "single",
                "options": ["Indoor", "Outdoor", "Mixed", "Unknown"],
                "default": "Outdoor"
            },
            {
                "text": "What is the lighting condition?",
                "type": "single", 
                "options": ["Bright daylight", "Dim lighting", "Artificial lighting", "Dark/Night", "Mixed lighting"],
                "default": "Bright daylight"
            },
            {
                "text": "Describe the visual quality and any notable features",
                "type": "description",
                "options": None,
                "default": None
            }
        ]
        
        # Question Group 2: Actions & Events
        action_questions = [
            {
                "text": "What is the main action or activity taking place?",
                "type": "single",
                "options": ["Walking/Moving", "Standing/Static", "Working", "Playing", "Eating", "Talking", "Multiple actions", "No clear action"],
                "default": "Walking/Moving"
            },
            {
                "text": "How many people are visible in the video?",
                "type": "single",
                "options": ["0", "1", "2-3", "4-5", "6-10", "More than 10", "Cannot determine"],
                "default": "1"
            },
            {
                "text": "Is there any interaction between people or with objects?",
                "type": "single",
                "options": ["Person-to-person interaction", "Person-to-object interaction", "Both", "None", "Cannot determine"],
                "default": "None"
            }
        ]
        
        # Question Group 3: Scene Context
        context_questions = [
            {
                "text": "What time period does this appear to be from?",
                "type": "single", 
                "options": ["Modern (2010+)", "Recent (2000-2010)", "Older (1990-2000)", "Historical (pre-1990)", "Cannot determine"],
                "default": "Modern (2010+)"
            },
            {
                "text": "What is the overall mood or tone of the video?",
                "type": "single",
                "options": ["Positive/Happy", "Neutral/Calm", "Serious/Formal", "Energetic/Active", "Negative/Sad", "Cannot determine"],
                "default": "Neutral/Calm"
            },
            {
                "text": "Provide a brief caption or description of the video content",
                "type": "description", 
                "options": None,
                "default": None
            }
        ]
        
        # Create questions and groups
        question_groups_data = [
            ("Visual Elements", "Questions about visual aspects, objects, and environment", visual_questions),
            ("Actions & Events", "Questions about activities, people, and interactions", action_questions), 
            ("Scene Context", "Questions about context, mood, and overall description", context_questions)
        ]
        
        for group_title, group_desc, questions_data in question_groups_data:
            # Create questions first
            question_ids = []
            for q_data in questions_data:
                try:
                    question = QuestionService.add_question(
                        text=q_data["text"],
                        qtype=q_data["type"],
                        options=q_data["options"],
                        default=q_data["default"],
                        session=session
                    )
                    question_ids.append(question["id"])
                    self.created_items["questions"].append({
                        "text": q_data["text"][:50] + "...",
                        "type": q_data["type"],
                        "group": group_title
                    })
                    print(f"   ✅ Created question: {q_data['text'][:50]}...")
                    
                except Exception as e:
                    print(f"   ⚠️ Question might already exist: {str(e)}")
                    # Try to get existing question
                    try:
                        question = QuestionService.get_question_by_text(q_data["text"], session)
                        question_ids.append(question["id"])
                    except:
                        continue
            
            # Create question group
            if question_ids:
                try:
                    group = QuestionGroupService.create_group(
                        title=group_title,
                        display_title=group_title,
                        description=group_desc,
                        is_reusable=True,
                        question_ids=question_ids,
                        verification_function=None,
                        session=session
                    )
                    self.created_items["question_groups"].append({
                        "title": group_title,
                        "questions": len(question_ids)
                    })
                    print(f"   ✅ Created question group: {group_title} ({len(question_ids)} questions)")
                    
                except Exception as e:
                    print(f"   ⚠️ Question group {group_title} might already exist: {str(e)}")
    
    def _create_schemas(self, session):
        """Create schemas combining question groups"""
        schemas_data = [
            {
                "name": "Complete Video Analysis",
                "description": "Comprehensive analysis including visual, action, and context questions",
                "groups": ["Visual Elements", "Actions & Events", "Scene Context"]
            },
            {
                "name": "Basic Visual Assessment", 
                "description": "Focus on visual elements and basic scene understanding",
                "groups": ["Visual Elements", "Scene Context"]
            },
            {
                "name": "Action Recognition Study",
                "description": "Specialized for analyzing actions and human behavior",
                "groups": ["Actions & Events", "Scene Context"]
            }
        ]
        
        for schema_data in schemas_data:
            try:
                # Get question group IDs
                group_ids = []
                for group_name in schema_data["groups"]:
                    try:
                        group = QuestionGroupService.get_group_by_name(group_name, session)
                        group_ids.append(group.id)
                    except Exception as e:
                        print(f"   ⚠️ Could not find group {group_name}: {str(e)}")
                
                if group_ids:
                    schema = SchemaService.create_schema(
                        name=schema_data["name"],
                        question_group_ids=group_ids,
                        session=session
                    )
                    self.created_items["schemas"].append({
                        "name": schema_data["name"],
                        "groups": len(group_ids)
                    })
                    print(f"   ✅ Created schema: {schema_data['name']}")
                    
            except Exception as e:
                print(f"   ⚠️ Schema {schema_data['name']} might already exist: {str(e)}")
    
    def _create_projects(self, session):
        """Create test projects with different video subsets"""
        
        # Get video IDs by extracting filenames from URLs
        video_uids = []
        for url in self.test_videos:
            uid = url.split("/")[-1]  # Extract filename from URL
            video_uids.append(uid)
        
        # Get all video IDs
        all_video_ids = ProjectService.get_video_ids_by_uids(video_uids, session)
        
        projects_data = [
            {
                "name": "Nature & Outdoor Scene Analysis",
                "description": "Analysis of outdoor scenes and nature footage",
                "schema": "Complete Video Analysis",
                "video_indices": [0, 2, 4, 6]  # Select specific videos
            },
            {
                "name": "Human Activity Recognition",
                "description": "Focus on human actions and behaviors", 
                "schema": "Action Recognition Study",
                "video_indices": [1, 3, 5, 7, 8]  # Different video subset
            },
            {
                "name": "Historical Video Documentation",
                "description": "Analysis of historical and archival footage",
                "schema": "Basic Visual Assessment", 
                "video_indices": [3, 4, 5]  # Smaller subset for focused study
            },
            {
                "name": "Pilot Study - Video Captioning",
                "description": "Small-scale pilot for video captioning research",
                "schema": "Complete Video Analysis",
                "video_indices": [0, 1, 2]  # First 3 videos for pilot
            }
        ]
        
        for project_data in projects_data:
            try:
                # Get schema ID
                schema_id = SchemaService.get_schema_id_by_name(project_data["schema"], session)
                
                # Get video IDs for this project
                project_video_ids = [all_video_ids[i] for i in project_data["video_indices"] if i < len(all_video_ids)]
                
                if project_video_ids:
                    ProjectService.create_project(
                        name=project_data["name"],
                        description=project_data["description"],
                        schema_id=schema_id,
                        video_ids=project_video_ids,
                        session=session
                    )
                    self.created_items["projects"].append({
                        "name": project_data["name"],
                        "schema": project_data["schema"],
                        "videos": len(project_video_ids)
                    })
                    print(f"   ✅ Created project: {project_data['name']} ({len(project_video_ids)} videos)")
                    
            except Exception as e:
                print(f"   ⚠️ Project {project_data['name']} might already exist: {str(e)}")
    
    def _create_project_groups(self, session):
        """Create project groups to organize projects"""
        groups_data = [
            {
                "name": "Research Projects",
                "description": "Academic research and pilot studies",
                "projects": ["Pilot Study - Video Captioning", "Historical Video Documentation"]
            },
            {
                "name": "Production Annotation",
                "description": "Large-scale production annotation projects", 
                "projects": ["Nature & Outdoor Scene Analysis", "Human Activity Recognition"]
            }
        ]
        
        for group_data in groups_data:
            try:
                # Get project IDs
                project_ids = []
                for project_name in group_data["projects"]:
                    try:
                        project = ProjectService.get_project_by_name(project_name, session)
                        project_ids.append(project.id)
                    except Exception as e:
                        print(f"   ⚠️ Could not find project {project_name}: {str(e)}")
                
                if project_ids:
                    ProjectGroupService.create_project_group(
                        name=group_data["name"],
                        description=group_data["description"],
                        project_ids=project_ids,
                        session=session
                    )
                    self.created_items["project_groups"].append({
                        "name": group_data["name"],
                        "projects": len(project_ids)
                    })
                    print(f"   ✅ Created project group: {group_data['name']}")
                    
            except Exception as e:
                print(f"   ⚠️ Project group {group_data['name']} might already exist: {str(e)}")
    
    def _assign_users_to_projects(self, session):
        """Assign users to projects with appropriate roles"""
        
        # Get user IDs
        try:
            alice = AuthService.get_user_by_email("alice@example.com", session)
            bob = AuthService.get_user_by_email("bob@example.com", session)
            carol = AuthService.get_user_by_email("carol@example.com", session)
        except Exception as e:
            print(f"   ❌ Could not find test users: {str(e)}")
            return
        
        # Assignment plan
        assignments = [
            # Alice (Senior Annotator) - Gets assigned to most projects
            {"project": "Nature & Outdoor Scene Analysis", "user": alice.id, "role": "annotator"},
            {"project": "Human Activity Recognition", "user": alice.id, "role": "annotator"},
            {"project": "Pilot Study - Video Captioning", "user": alice.id, "role": "annotator"},
            
            # Bob (Junior Annotator) - Gets assigned to some projects
            {"project": "Nature & Outdoor Scene Analysis", "user": bob.id, "role": "annotator"},
            {"project": "Historical Video Documentation", "user": bob.id, "role": "annotator"},
            {"project": "Pilot Study - Video Captioning", "user": bob.id, "role": "annotator"},
            
            # Carol (Reviewer) - Gets reviewer access to all projects  
            {"project": "Nature & Outdoor Scene Analysis", "user": carol.id, "role": "reviewer"},
            {"project": "Human Activity Recognition", "user": carol.id, "role": "reviewer"},
            {"project": "Historical Video Documentation", "user": carol.id, "role": "reviewer"},
            {"project": "Pilot Study - Video Captioning", "user": carol.id, "role": "reviewer"},
        ]
        
        for assignment in assignments:
            try:
                project = ProjectService.get_project_by_name(assignment["project"], session)
                ProjectService.add_user_to_project(
                    user_id=assignment["user"],
                    project_id=project.id,
                    role=assignment["role"], 
                    session=session
                )
                
                user_name = "Alice" if assignment["user"] == alice.id else \
                           "Bob" if assignment["user"] == bob.id else "Carol"
                print(f"   ✅ Assigned {user_name} as {assignment['role']} to {assignment['project']}")
                
            except Exception as e:
                print(f"   ⚠️ Assignment might already exist: {str(e)}")
    
    def _print_summary(self):
        """Print a summary of created items"""
        summary_data = [
            ("👥 Users", len(self.created_items["users"])),
            ("📹 Videos", len(self.created_items["videos"])),
            ("❓ Questions", len(self.created_items["questions"])),
            ("📝 Question Groups", len(self.created_items["question_groups"])),
            ("📋 Schemas", len(self.created_items["schemas"])),
            ("📁 Projects", len(self.created_items["projects"])), 
            ("🗂️ Project Groups", len(self.created_items["project_groups"]))
        ]
        
        for item_type, count in summary_data:
            print(f"   {item_type}: {count}")

def main():
    """Main function to run the setup"""
    setup = TestDataSetup()
    
    # Confirm before proceeding
    print("This will create test data including users, videos, and projects.")
    print("Make sure your database is properly set up and connected.")
    print()
    
    response = input("Continue with test data setup? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Setup cancelled.")
        return
    
    success = setup.run_setup()
    
    if success:
        print("\n🚀 You can now test the application!")
        print("Run: streamlit run your_streamlit_app.py")
    else:
        print("\n❌ Setup failed. Please check the error messages above.")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())