#!/usr/bin/env python3
"""
Content Safety Analysis Test Setup Script - 2025-07-01

This script creates a fresh test environment with NO overlap with existing data:
- Two NON-REUSABLE question groups WITH custom display (no auto-submit)
- One REUSABLE question group WITHOUT custom display (can auto-submit)
- Fresh questions, users, projects, and schema using 2025-07-01 naming
- Demonstrates custom display functionality properly
- Full human and model annotations
"""

import os
import random
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import all the services and models
from label_pizza.services import (
    QuestionService, QuestionGroupService, SchemaService, 
    VideoService, ProjectService, AuthService, AnnotatorService,
    CustomDisplayService
)
from label_pizza.models import ProjectVideoQuestionDisplay  # Import the model
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

def create_safety_questions_2025_07_01(session):
    """Create completely new questions for 2025-07-01 safety analysis"""
    print("Creating fresh 2025-07-01 safety analysis questions...")
    
    def create_or_get_question(text, qtype, options, default, session, display_text, option_weights=None):
        """Helper function to create question or get existing one"""
        try:
            question_obj = QuestionService.add_question(
                text=text,
                qtype=qtype,
                options=options,
                default=default,
                session=session,
                display_text=display_text,
                option_weights=option_weights
            )
            # Convert ORM object to dictionary format
            return {
                "id": question_obj.id,
                "text": question_obj.text,
                "display_text": question_obj.display_text,
                "type": question_obj.type,
                "options": question_obj.options,
                "display_values": question_obj.display_values,
                "option_weights": question_obj.option_weights,
                "default_option": question_obj.default_option
            }
        except ValueError as e:
            if "already exists" in str(e):
                print(f"  ⚠️  Question already exists, retrieving: '{text}'")
                return QuestionService.get_question_by_text(text=text, session=session)
            else:
                raise e
    
    # NON-REUSABLE Group 1: Safety Assessment (WITH custom display, NO auto-submit)
    safety_q1 = create_or_get_question(
        text="2025-07-01 Safety: Rate the overall safety level of content shown",
        qtype="single",
        options=["A", "B", "C", "D"],
        default="B",
        session=session,
        display_text="Content Safety Rating Assessment",
        option_weights=[2.0, 1.0, 1.2, 1.5]  # Higher weight for unsafe content
    )
    
    safety_q2 = create_or_get_question(
        text="2025-07-01 Safety: Evaluate potential risk factors present in content",
        qtype="single",
        options=["A", "B", "C"],
        default="A",
        session=session,
        display_text="Risk Factor Evaluation",
        option_weights=[1.0, 1.4, 1.8]  # Escalating weights for risk levels
    )
    
    safety_q3 = create_or_get_question(
        text="2025-07-01 Safety: Provide detailed safety analysis and recommendations",
        qtype="description",
        options=None,
        default=None,
        session=session,
        display_text="Detailed Safety Analysis"
    )
    
    # NON-REUSABLE Group 2: Content Classification (WITH custom display, NO auto-submit)
    classification_q1 = create_or_get_question(
        text="2025-07-01 Classification: Determine primary content theme category",
        qtype="single",
        options=["A", "B", "C", "D", "E"],
        default="C",
        session=session,
        display_text="Primary Content Theme Classification",
        option_weights=[1.1, 1.3, 1.0, 1.2, 0.8]
    )
    
    classification_q2 = create_or_get_question(
        text="2025-07-01 Classification: Assess content appropriateness level",
        qtype="single",
        options=["A", "B", "C", "D"],
        default="A",
        session=session,
        display_text="Content Appropriateness Assessment",
        option_weights=[1.0, 1.1, 1.4, 1.7]  # Higher weights for restricted content
    )
    
    # REUSABLE Group 3: Quality Check (NO custom display, CAN auto-submit)
    quality_q1 = create_or_get_question(
        text="2025-07-01 Quality: Technical quality meets platform standards",
        qtype="single",
        options=["A", "B"],
        default="A",
        session=session,
        display_text="Technical Quality Standards Check",
        option_weights=[1.0, 1.0]
    )
    
    quality_q2 = create_or_get_question(
        text="2025-07-01 Quality: Audio-visual clarity is acceptable",
        qtype="single",
        options=["A", "B", "C"],
        default="A",
        session=session,
        display_text="Audio-Visual Clarity Assessment",
        option_weights=[1.0, 0.8, 0.6]  # Higher weight for good quality
    )
    
    return {
        'safety_group': [safety_q1["id"], safety_q2["id"], safety_q3["id"]],
        'classification_group': [classification_q1["id"], classification_q2["id"]], 
        'quality_group': [quality_q1["id"], quality_q2["id"]]
    }

def create_safety_question_groups_2025_07_01(session, question_ids):
    """Create the three question groups with proper reusability settings"""
    print("Creating 2025-07-01 safety question groups...")
    
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
                return QuestionGroupService.get_group_by_name(name=title, session=session)
            else:
                raise e
    
    # Group 1: NON-REUSABLE with custom display (NO auto-submit)
    safety_group = create_or_get_group(
        title="Safety Assessment 2025-07-01",
        description="Non-reusable safety evaluation with custom display per video type",
        is_reusable=False,  # NON-REUSABLE - can have custom display
        question_ids=question_ids['safety_group'],
        verification_function=None,
        is_auto_submit=False,  # NO auto-submit for custom display groups
        session=session
    )
    
    # Group 2: NON-REUSABLE with custom display (NO auto-submit)
    classification_group = create_or_get_group(
        title="Content Classification 2025-07-01", 
        description="Non-reusable content classification with custom display per video type",
        is_reusable=False,  # NON-REUSABLE - can have custom display
        question_ids=question_ids['classification_group'],
        verification_function=None,
        is_auto_submit=False,  # NO auto-submit for custom display groups
        session=session
    )
    
    # Group 3: REUSABLE without custom display (CAN auto-submit)
    quality_group = create_or_get_group(
        title="Quality Check 2025-07-01",
        description="Reusable quality validation with auto-submit enabled",
        is_reusable=True,  # REUSABLE - cannot have custom display
        question_ids=question_ids['quality_group'],
        verification_function=None,
        is_auto_submit=True,  # CAN auto-submit for reusable groups
        session=session
    )
    
    return [safety_group.id, classification_group.id, quality_group.id]

def create_custom_display_schema_and_project_2025_07_01(session, group_ids, video_ids):
    """Create schema with custom display enabled and project"""
    print("Creating 2025-07-01 custom display schema...")
    
    # Try to create schema with custom display enabled
    try:
        schema = SchemaService.create_schema(
            name="Content Safety Analysis Schema 2025-07-01",
            question_group_ids=group_ids,
            instructions_url="https://example.com/safety-guidelines-2025-07-01",
            has_custom_display=True,  # Enable custom display!
            session=session
        )
        print("✓ Created new schema with custom display enabled")
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Schema already exists, retrieving: 'Content Safety Analysis Schema 2025-07-01'")
            schema = SchemaService.get_schema_by_name(name="Content Safety Analysis Schema 2025-07-01", session=session)
        else:
            raise e
    
    print("Creating 2025-07-01 custom display project...")
    
    # Try to create project
    try:
        ProjectService.create_project(
            name="Content Safety Analysis Project 2025-07-01",
            schema_id=schema.id,
            video_ids=video_ids,
            session=session
        )
        print("✓ Created new project with custom display support")
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Project already exists, retrieving: 'Content Safety Analysis Project 2025-07-01'")
        else:
            raise e
    
    # Get the project (whether new or existing)
    project = ProjectService.get_project_by_name(name="Content Safety Analysis Project 2025-07-01", session=session)
    return project.id, schema.id

def assign_video_content_types_2025_07_01(video_ids):
    """Assign content types to videos for 2025-07-01 custom display demonstration"""
    content_types = ["Gaming", "Educational", "Entertainment", "Cooking", "Sports", "Music"]
    
    video_content_mapping = {}
    for i, video_id in enumerate(video_ids):
        # Cycle through content types
        content_type = content_types[i % len(content_types)]
        video_content_mapping[video_id] = content_type
    
    return video_content_mapping

def create_custom_display_entries_2025_07_01(session, project_id, video_ids, question_ids):
    """Create custom display entries ONLY for NON-REUSABLE groups with A/B/C/D format"""
    print("Creating 2025-07-01 custom display entries for NON-REUSABLE groups only...")
    
    # Get video content type mapping
    video_content_mapping = assign_video_content_types_2025_07_01(video_ids)
    
    # Get question IDs for NON-REUSABLE groups ONLY (safety and classification)
    safety_q1_id = question_ids['safety_group'][0]  # Safety rating
    safety_q2_id = question_ids['safety_group'][1]  # Risk factors
    safety_q3_id = question_ids['safety_group'][2]  # Description
    classification_q1_id = question_ids['classification_group'][0]  # Theme category
    classification_q2_id = question_ids['classification_group'][1]  # Appropriateness
    
    # DO NOT customize quality group questions (reusable group)
    
    custom_displays_created = 0
    
    for video_id, content_type in video_content_mapping.items():
        
        # Customize Safety Question 1: A/B/C/D safety rating
        safety_rating_custom_text = {
            "Gaming": "Rate the safety level of this gaming content for player wellbeing",
            "Educational": "Rate the safety level of this educational content for learners", 
            "Entertainment": "Rate the safety level of this entertainment content for viewers",
            "Cooking": "Rate the safety level of this cooking content for food preparation",
            "Sports": "Rate the safety level of this sports content for athletic activity",
            "Music": "Rate the safety level of this music content for audio consumption"
        }
        
        safety_rating_custom_options = {
            "Gaming": {
                "A": "(A) Safe gaming content", 
                "B": "(B) Mild gaming concerns", 
                "C": "(C) Moderate gaming risks", 
                "D": "(D) High gaming risks"
            },
            "Educational": {
                "A": "(A) Safe educational content", 
                "B": "(B) Mild educational concerns", 
                "C": "(C) Moderate educational risks", 
                "D": "(D) High educational risks"
            },
            "Entertainment": {
                "A": "(A) Safe entertainment content", 
                "B": "(B) Mild entertainment concerns", 
                "C": "(C) Moderate entertainment risks", 
                "D": "(D) High entertainment risks"
            },
            "Cooking": {
                "A": "(A) Safe cooking practices", 
                "B": "(B) Mild cooking concerns", 
                "C": "(C) Moderate cooking risks", 
                "D": "(D) High cooking risks"
            },
            "Sports": {
                "A": "(A) Safe sports content", 
                "B": "(B) Mild sports concerns", 
                "C": "(C) Moderate sports risks", 
                "D": "(D) High sports risks"
            },
            "Music": {
                "A": "(A) Safe music content", 
                "B": "(B) Mild music concerns", 
                "C": "(C) Moderate music risks", 
                "D": "(D) High music risks"
            }
        }
        
        try:
            CustomDisplayService.set_custom_display(
                project_id=project_id,
                video_id=video_id,
                question_id=safety_q1_id,
                custom_display_text=safety_rating_custom_text[content_type],
                custom_option_display_map=safety_rating_custom_options[content_type],
                session=session
            )
            custom_displays_created += 1
        except Exception as e:
            print(f"  ⚠️  Failed to set custom display for safety rating: {e}")
        
        # Customize Safety Question 2: A/B/C risk factors
        risk_factors_custom_options = {
            "Gaming": {
                "A": "(A) No gaming risks identified", 
                "B": "(B) Minor gaming risks present", 
                "C": "(C) Significant gaming risks"
            },
            "Educational": {
                "A": "(A) No educational risks identified", 
                "B": "(B) Minor educational risks present", 
                "C": "(C) Significant educational risks"
            },
            "Entertainment": {
                "A": "(A) No entertainment risks identified", 
                "B": "(B) Minor entertainment risks present", 
                "C": "(C) Significant entertainment risks"
            },
            "Cooking": {
                "A": "(A) No cooking risks identified", 
                "B": "(B) Minor cooking risks present", 
                "C": "(C) Significant cooking risks"
            },
            "Sports": {
                "A": "(A) No sports risks identified", 
                "B": "(B) Minor sports risks present", 
                "C": "(C) Significant sports risks"
            },
            "Music": {
                "A": "(A) No music risks identified", 
                "B": "(B) Minor music risks present", 
                "C": "(C) Significant music risks"
            }
        }
        
        try:
            CustomDisplayService.set_custom_display(
                project_id=project_id,
                video_id=video_id,
                question_id=safety_q2_id,
                custom_option_display_map=risk_factors_custom_options[content_type],
                session=session
            )
            custom_displays_created += 1
        except Exception as e:
            print(f"  ⚠️  Failed to set custom display for risk factors: {e}")
            
        # Customize Safety Question 3: Description text
        safety_description_custom_text = {
            "Gaming": "Provide detailed analysis of gaming safety concerns, player impact, and recommendations",
            "Educational": "Provide detailed analysis of educational safety concerns, learner impact, and recommendations",
            "Entertainment": "Provide detailed analysis of entertainment safety concerns, viewer impact, and recommendations",
            "Cooking": "Provide detailed analysis of cooking safety concerns, food safety impact, and recommendations",
            "Sports": "Provide detailed analysis of sports safety concerns, athletic impact, and recommendations",
            "Music": "Provide detailed analysis of music safety concerns, listener impact, and recommendations"
        }
        
        try:
            CustomDisplayService.set_custom_display(
                project_id=project_id,
                video_id=video_id,
                question_id=safety_q3_id,
                custom_display_text=safety_description_custom_text[content_type],
                session=session
            )
            custom_displays_created += 1
        except Exception as e:
            print(f"  ⚠️  Failed to set custom display for safety description: {e}")
        
        # Customize Classification Question 1: A/B/C/D/E theme categories
        theme_custom_options = {
            "Gaming": {
                "A": "(A) Action/Adventure Gaming", 
                "B": "(B) Educational Gaming", 
                "C": "(C) Casual Gaming", 
                "D": "(D) Competitive Gaming", 
                "E": "(E) Other Gaming"
            },
            "Educational": {
                "A": "(A) Academic Instruction", 
                "B": "(B) Skill Development", 
                "C": "(C) General Knowledge", 
                "D": "(D) Professional Training", 
                "E": "(E) Other Educational"
            },
            "Entertainment": {
                "A": "(A) Comedy Entertainment", 
                "B": "(B) Drama Entertainment", 
                "C": "(C) Reality Entertainment", 
                "D": "(D) Documentary Entertainment", 
                "E": "(E) Other Entertainment"
            },
            "Cooking": {
                "A": "(A) Recipe Demonstration", 
                "B": "(B) Cooking Techniques", 
                "C": "(C) Food Review", 
                "D": "(D) Kitchen Tips", 
                "E": "(E) Other Cooking"
            },
            "Sports": {
                "A": "(A) Competitive Sports", 
                "B": "(B) Training/Fitness", 
                "C": "(C) Sports Analysis", 
                "D": "(D) Recreational Sports", 
                "E": "(E) Other Sports"
            },
            "Music": {
                "A": "(A) Music Performance", 
                "B": "(B) Music Tutorial", 
                "C": "(C) Music Review", 
                "D": "(D) Music Production", 
                "E": "(E) Other Music"
            }
        }
        
        try:
            CustomDisplayService.set_custom_display(
                project_id=project_id,
                video_id=video_id,
                question_id=classification_q1_id,
                custom_option_display_map=theme_custom_options[content_type],
                session=session
            )
            custom_displays_created += 1
        except Exception as e:
            print(f"  ⚠️  Failed to set custom display for theme classification: {e}")
        
        # Customize Classification Question 2: A/B/C/D appropriateness
        appropriateness_custom_options = {
            "Gaming": {
                "A": "(A) All ages gaming", 
                "B": "(B) Teen gaming content", 
                "C": "(C) Mature gaming content", 
                "D": "(D) Adult only gaming"
            },
            "Educational": {
                "A": "(A) All ages learning", 
                "B": "(B) Advanced learning", 
                "C": "(C) Professional learning", 
                "D": "(D) Specialized learning"
            },
            "Entertainment": {
                "A": "(A) Family entertainment", 
                "B": "(B) General entertainment", 
                "C": "(C) Mature entertainment", 
                "D": "(D) Adult entertainment"
            },
            "Cooking": {
                "A": "(A) Basic cooking", 
                "B": "(B) Intermediate cooking", 
                "C": "(C) Advanced cooking", 
                "D": "(D) Professional cooking"
            },
            "Sports": {
                "A": "(A) Recreational sports", 
                "B": "(B) Competitive sports", 
                "C": "(C) Professional sports", 
                "D": "(D) Extreme sports"
            },
            "Music": {
                "A": "(A) Clean music content", 
                "B": "(B) General music content", 
                "C": "(C) Mature music content", 
                "D": "(D) Explicit music content"
            }
        }
        
        try:
            CustomDisplayService.set_custom_display(
                project_id=project_id,
                video_id=video_id,
                question_id=classification_q2_id,
                custom_option_display_map=appropriateness_custom_options[content_type],
                session=session
            )
            custom_displays_created += 1
        except Exception as e:
            print(f"  ⚠️  Failed to set custom display for appropriateness: {e}")
    
    print(f"✓ Created {custom_displays_created} custom display entries for NON-REUSABLE groups")
    print(f"✓ Quality group (REUSABLE) has NO custom display as required")
    
    # Print content type assignments for reference
    print("\nContent Type Assignments:")
    for video_id, content_type in video_content_mapping.items():
        print(f"  Video {video_id}: {content_type}")
    
    print("\nCustom Display Applied To:")
    print("  ✅ Safety Assessment 2025-07-01 (NON-REUSABLE)")
    print("  ✅ Content Classification 2025-07-01 (NON-REUSABLE)")
    print("  ❌ Quality Check 2025-07-01 (REUSABLE - no custom display)")

def create_safety_users_2025_07_01(session, project_id):
    """Create completely new users for 2025-07-01 testing"""
    print("Creating 2025-07-01 safety analysis users...")
    
    users_created = []
    
    # Create human safety analyst
    try:
        human_analyst = AuthService.create_user(
            user_id="safety_analyst_bob_2025_07_01",
            email="bob.safety.2025.07.01@example.com",
            password_hash="safety_password_hash_2025_07_01",
            user_type="human",
            session=session
        )
        print("✓ Created human safety analyst user")
        users_created.append(("human", human_analyst.id))
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  Human safety analyst already exists, retrieving")
            human_analyst = AuthService.get_user_by_id(user_id="safety_analyst_bob_2025_07_01", session=session)
            users_created.append(("human", human_analyst.id))
        else:
            raise e
    
    # Create AI safety model
    try:
        ai_safety_model = AuthService.create_user(
            user_id="ai_safety_model_2025_07_01",
            email=None,  # Model users don't have emails
            password_hash="ai_safety_hash_2025_07_01",
            user_type="model",
            session=session
        )
        print("✓ Created AI safety model user")
        users_created.append(("model", ai_safety_model.id))
    except ValueError as e:
        if "already exists" in str(e):
            print("  ⚠️  AI safety model already exists, retrieving")
            ai_safety_model = AuthService.get_user_by_id(user_id="ai_safety_model_2025_07_01", session=session)
            users_created.append(("model", ai_safety_model.id))
        else:
            raise e
    
    # Assign users to project
    for user_type, user_id in users_created:
        try:
            if user_type == "human":
                ProjectService.add_user_to_project(
                    project_id=project_id,
                    user_id=user_id,
                    role="annotator",
                    session=session,
                    user_weight=1.0
                )
                print(f"✓ Assigned human safety analyst to project as annotator")
            else:  # model
                ProjectService.add_user_to_project(
                    project_id=project_id,
                    user_id=user_id,
                    role="model",
                    session=session,
                    user_weight=1.4  # Higher weight for AI model
                )
                print(f"✓ Assigned AI safety model to project")
        except ValueError as e:
            if "already" in str(e).lower():
                print(f"  ⚠️  User {user_id} already assigned to project")
            else:
                print(f"  ⚠️  Assignment issue: {e}")
    
    return [user_id for _, user_id in users_created]

def generate_safety_test_answers_2025_07_01(session, project_id, user_ids, group_ids, video_ids):
    """Generate comprehensive safety analysis answers for 2025-07-01 testing"""
    print("Generating 2025-07-01 safety analysis answers...")
    
    # Get all questions for each group
    group_questions = {}
    for group_id in group_ids:
        questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        group_questions[group_id] = questions
    
    total_answers = 0
    skipped_existing = 0
    
    for user_id in user_ids:
        for video_id in video_ids:
            for group_id in group_ids:
                questions = group_questions[group_id]
                
                # Check if answers already exist
                existing_answers = {}
                try:
                    existing_answers = AnnotatorService.get_user_answers_for_question_group(
                        video_id=video_id,
                        project_id=project_id,
                        user_id=user_id,
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
                        # Generate realistic safety analysis answers using A/B/C/D format
                        if "safety level" in question["text"].lower():
                            # Most content is safe (80% A=Safe, 15% B=Mild, 4% C=Moderate, 1% D=High)
                            selected_option = random.choices(
                                ["A", "B", "C", "D"], 
                                weights=[80, 15, 4, 1]  # Weighted toward safe content (A)
                            )[0]
                        elif "risk factors" in question["text"].lower():
                            # Most content has no risks (75% A=No risks, 20% B=Minor, 5% C=Significant)
                            selected_option = random.choices(
                                ["A", "B", "C"],
                                weights=[75, 20, 5]
                            )[0]
                        elif "content theme" in question["text"].lower():
                            # Evenly distribute across A/B/C/D/E theme categories
                            selected_option = random.choice(["A", "B", "C", "D", "E"])
                        elif "appropriateness" in question["text"].lower():
                            # Most content is appropriate for general audiences
                            selected_option = random.choices(
                                ["A", "B", "C", "D"],
                                weights=[60, 25, 12, 3]  # Weighted toward A=all ages
                            )[0]
                        elif "quality" in question["text"].lower():
                            # Most content meets quality standards
                            if len(question["options"]) == 2:  # A/B format
                                selected_option = random.choices(
                                    ["A", "B"],
                                    weights=[85, 15]  # Weighted toward A=meets standards
                                )[0]
                            else:  # A/B/C format
                                selected_option = random.choices(
                                    ["A", "B", "C"],
                                    weights=[70, 25, 5]  # Weighted toward A=acceptable
                                )[0]
                        else:
                            # Default: choose first option for any unrecognized questions
                            selected_option = question["options"][0] if question["options"] else "A"
                        
                        answers[question["text"]] = selected_option
                        confidence_scores[question["text"]] = round(random.uniform(0.65, 0.95), 2)
                        
                    elif question["type"] == "description":
                        # Generate realistic safety analysis descriptions
                        descriptions = [
                            "Content demonstrates appropriate safety standards with no concerning elements identified during analysis.",
                            "Safety evaluation reveals standard content presentation with minor considerations noted for review.",
                            "Analysis indicates content follows established safety guidelines with appropriate themes and execution.",
                            "Safety assessment shows content is suitable for intended audience with proper safety considerations applied.",
                            "Detailed review confirms content meets safety requirements with no significant risk factors present.",
                            "Safety analysis reveals professional content presentation with appropriate safety measures demonstrated.",
                            "Comprehensive evaluation shows content adheres to safety standards with proper risk mitigation applied.",
                            "Safety review indicates content demonstrates good safety practices with minimal risk exposure."
                        ]
                        answers[question["text"]] = random.choice(descriptions)
                        confidence_scores[question["text"]] = round(random.uniform(0.75, 0.92), 2)
                
                # Only submit if we have new answers
                if answers:
                    try:
                        AnnotatorService.submit_answer_to_question_group(
                            video_id=video_id,
                            project_id=project_id,
                            user_id=user_id,
                            question_group_id=group_id,
                            answers=answers,
                            session=session,
                            confidence_scores=confidence_scores
                        )
                        total_answers += len(answers)
                        
                    except Exception as e:
                        print(f"  ⚠️  Failed to submit answers for user {user_id}, video {video_id}, group {group_id}: {e}")
    
    print(f"✓ Generated {total_answers} new safety analysis answers")
    if skipped_existing > 0:
        print(f"  ⚠️  Skipped {skipped_existing} existing answers")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Set up 2025-07-01 content safety analysis with custom display")
    parser.add_argument(
        "--database-url-name",
        default="DBURL",
        help="Environment variable name for database URL (default: DBURL)"
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🛡️  CONTENT SAFETY ANALYSIS SETUP - 2025-07-01")
    print("=" * 70)
    
    try:
        # Setup database
        session = setup_database(args.database_url_name)
        print("✓ Database connection established")
        
        # Get existing videos (use a subset for testing)
        existing_videos = VideoService.get_all_videos(session=session)
        if existing_videos.empty:
            print("❌ No videos found. Please run a video setup script first.")
            return
        
        # Use first 15 videos for manageable testing
        all_video_ids = existing_videos['ID'].tolist()
        video_ids = all_video_ids[:15]
        print(f"✓ Using {len(video_ids)} videos (subset of {len(all_video_ids)}) for safety analysis testing")
        
        # Create fresh safety questions
        question_ids = create_safety_questions_2025_07_01(session)
        print("✓ 2025-07-01 safety analysis questions ready")
        
        # Create question groups with proper reusability settings
        group_ids = create_safety_question_groups_2025_07_01(session, question_ids)
        print("✓ Question groups ready with proper reusability settings")
        print(f"  - Safety Assessment (NON-REUSABLE, custom display): {group_ids[0]}")
        print(f"  - Content Classification (NON-REUSABLE, custom display): {group_ids[1]}")
        print(f"  - Quality Check (REUSABLE, auto-submit, no custom display): {group_ids[2]}")
        
        # Create schema with custom display and project
        project_id, schema_id = create_custom_display_schema_and_project_2025_07_01(session, group_ids, video_ids)
        print(f"✓ Custom display schema and project ready")
        print(f"  - Schema ID: {schema_id} (has_custom_display=True)")
        print(f"  - Project ID: {project_id}")
        
        # Create custom display entries ONLY for non-reusable groups
        create_custom_display_entries_2025_07_01(session, project_id, video_ids, question_ids)
        
        # Create fresh users
        user_ids = create_safety_users_2025_07_01(session, project_id)
        
        # Generate comprehensive test answers
        generate_safety_test_answers_2025_07_01(session, project_id, user_ids, group_ids, video_ids)
        
        # Get actual question IDs for display
        actual_question_ids = {}
        for group_name, q_list in question_ids.items():
            actual_question_ids[group_name] = []
            for q in q_list:
                if isinstance(q, dict):
                    actual_question_ids[group_name].append(q["id"])
                else:
                    actual_question_ids[group_name].append(q)
        
        print("\n" + "=" * 70)
        print("🎉 2025-07-01 CONTENT SAFETY ANALYSIS SETUP COMPLETE!")
        print("=" * 70)
        print(f"Project ID: {project_id}")
        print(f"Schema ID: {schema_id} (has_custom_display=True)")
        print(f"User IDs: {user_ids}")
        print(f"Question Groups: {group_ids}")
        print(f"Videos: {len(video_ids)} with custom display on non-reusable groups")
        print(f"Schema: 'Content Safety Analysis Schema 2025-07-01'")
        print(f"Project: 'Content Safety Analysis Project 2025-07-01'")
        print(f"Users: 'safety_analyst_bob_2025_07_01' & 'ai_safety_model_2025_07_01'")
        
        print("\nCustom Display Configuration:")
        print("- ✅ Two NON-REUSABLE groups WITH custom display (no auto-submit)")
        print("- ✅ One REUSABLE group WITHOUT custom display (can auto-submit)")
        print("- ✅ A/B/C/D standardized options with content-specific display text")
        print("- ✅ Each video has different display text for safety and classification groups")
        print("- ✅ Quality group maintains standard display (reusable)")
        print("- ✅ Full human and model annotations generated")
        print("- ✅ Completely fresh data with 2025-07-01 naming")
        
        print("\n" + "=" * 70)
        print("🔧 TESTING CUSTOM DISPLAY:")
        print("=" * 70)
        print("1. Test NON-REUSABLE groups show custom display per video type")
        print("2. Test REUSABLE group shows standard display (no customization)")
        print("3. Verify auto-submit works only on quality group")
        print("4. Compare Gaming vs Cooking videos for different display text")
        print("5. Confirm answer submission uses A/B/C/D values consistently")
        
        print("\nExample Custom Display Test:")
        print("```python")
        print(f"# Test NON-REUSABLE safety group (should have custom display)")
        print(f"gaming_safety = CustomDisplayService.get_custom_display(")
        print(f"    question_id={actual_question_ids['safety_group'][0]},")
        print(f"    project_id={project_id},")
        print(f"    video_id={video_ids[0]},  # Gaming video")
        print(f"    session=session")
        print(f")")
        print(f"print('Gaming safety:', gaming_safety['display_text'])")
        print(f"print('Gaming options:', gaming_safety['display_values'])")
        print("")
        print(f"# Test REUSABLE quality group (should have NO custom display)")
        print(f"quality_check = CustomDisplayService.get_custom_display(")
        print(f"    question_id={actual_question_ids['quality_group'][0]},")
        print(f"    project_id={project_id},")
        print(f"    video_id={video_ids[0]},")
        print(f"    session=session")
        print(f")")
        print(f"print('Quality check:', quality_check['display_text'])  # Should be standard")
        print("```")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise
    
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    main()