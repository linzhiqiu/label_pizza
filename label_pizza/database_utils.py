import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from contextlib import contextmanager
from sqlalchemy import text

from label_pizza.services import (
    AuthService, AnnotatorService, GroundTruthService, 
    QuestionService, SchemaService, CustomDisplayService,
    ProjectService, VideoService, ProjectGroupService
)
from label_pizza.db import SessionLocal
if SessionLocal is None:
    raise ValueError("SessionLocal is not initialized")
from label_pizza.ui_components import custom_info

###############################################################################
# DATABASE UTILITIES
###############################################################################

@contextmanager
def get_db_session():
    """Get database session with proper error handling"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def check_database_health() -> dict:
    """Check database connection health"""
    try:
        with get_db_session() as session:
            try:
                users_df = AuthService.get_all_users(session=session)
                return {
                    "healthy": True,
                    "database": "Connected",
                    "status": "OK",
                    "user_count": len(users_df)
                }
            except:
                session.execute(text("SELECT 1"))
                return {"healthy": True, "database": "Connected", "status": "OK"}
    except Exception as e:
        return {"healthy": False, "error": str(e), "database": "Disconnected"}

def handle_database_errors(func):
    """Decorator to handle database connection errors gracefully"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"Database connection error: {str(e)}")
            st.error("This might be a temporary issue. Please try:")
            custom_info("1. Refresh the page")
            custom_info("2. Check your internet connection") 
            custom_info("3. Contact support if the problem persists")
            
            with st.expander("🔧 Technical Details"):
                try:
                    health = check_database_health()
                    st.json(health)
                except:
                    st.write("Unable to get database health information")
            return None
    return wrapper


###############################################################################
# CACHED DATA LOADERS
###############################################################################

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_all_users(session_id: str) -> pd.DataFrame:
    """Cache all users data - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return AuthService.get_all_users(session=session)
        except Exception as e:
            print(f"Error in get_cached_all_users: {e}")
            return pd.DataFrame()

@st.cache_data(ttl=3600)  # Cache for 1 hour  
def get_cached_project_questions(project_id: int, session_id: str) -> List[Dict]:
    """Cache project questions - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return ProjectService.get_project_questions(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_project_questions: {e}")
            return []

def get_project_questions_cached(project_id: int, session: Session) -> List[Dict]:
    """Get project questions with caching"""
    session_id = get_session_cache_key()
    return get_cached_project_questions(project_id, session_id)

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_cached_question_answers(project_id: int, session_id: str) -> pd.DataFrame:
    """Cache ALL annotator answers for a project - annotator answers rarely change"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            # Get all questions for the project first
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            
            all_answers = []
            for question in questions:
                try:
                    question_answers = AnnotatorService.get_question_answers(
                        question_id=question["id"], project_id=project_id, session=session
                    )
                    if not question_answers.empty:
                        # ADD THE MISSING QUESTION ID COLUMN!
                        question_answers["Question ID"] = question["id"]
                        all_answers.append(question_answers)
                except Exception as e:
                    print(f"Error getting answers for question {question.get('id', 'unknown')}: {e}")
                    continue
            
            if all_answers:
                return pd.concat(all_answers, ignore_index=True)
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Error in get_cached_question_answers: {e}")
            return pd.DataFrame()

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_cached_project_annotators(project_id: int, session_id: str) -> Dict[str, Dict]:
    """Cache project annotators info - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            # Get project assignments to determine project-specific roles
            assignments_df = AuthService.get_project_assignments(session=session)
            project_assignments = assignments_df[assignments_df["Project ID"] == project_id]
            
            # Get all users
            users_df = AuthService.get_all_users(session=session)
            user_lookup = {row["ID"]: row for _, row in users_df.iterrows()}
            
            # Build user role mapping with priority: admin > reviewer > model > annotator
            user_roles = {}
            role_priority = {"admin": 4, "reviewer": 3, "model": 2, "annotator": 1}
            
            for _, assignment in project_assignments.iterrows():
                user_id = assignment["User ID"]
                role = assignment["Role"]
                
                if user_id not in user_roles or role_priority.get(role, 0) > role_priority.get(user_roles[user_id], 0):
                    user_roles[user_id] = role
            
            # Get annotators who have actually submitted answers
            annotators = ProjectService.get_project_annotators(project_id=project_id, session=session)
            
            # Enhance with correct project roles and user info
            enhanced_annotators = {}
            for display_name, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id and user_id in user_lookup:
                    user_data = user_lookup[user_id]
                    project_role = user_roles.get(user_id, 'annotator')  # Default to annotator if not found
                    
                    enhanced_annotators[display_name] = {
                        'id': user_id,
                        'email': user_data["Email"],
                        'Role': project_role,  # Use project-specific role
                        'role': project_role,  # Backup key
                        'system_role': user_data["Role"],  # Keep system role for reference
                        'display_name': display_name
                    }
            
            return enhanced_annotators
            
        except Exception as e:
            print(f"Error in get_cached_project_annotators: {e}")
            return {}


@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_cached_project_videos(project_id: int, session_id: str) -> List[Dict]:
    """Cache project videos - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return VideoService.get_project_videos(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_project_videos: {e}")
            return []


@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_cached_video_reviewer_data(video_id: int, project_id: int, annotator_user_ids: List[int], session_id: str) -> Dict:
    """Cache ALL reviewer data for a specific video to minimize repeated queries"""
    with SessionLocal() as session:
        try:
            cache_data = {
                "annotator_answers": {},
                "user_info": {},
                "confidence_scores": {},
                "text_answers": {},
                "user_weights": {}  # ADD THIS
            }
            
            # Get all questions for the project
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            question_ids = [q["id"] for q in questions]
            
            # Batch load all answers for this video from selected annotators
            for question_id in question_ids:
                answers_df = AnnotatorService.get_question_answers(
                    question_id=question_id, project_id=project_id, session=session
                )
                
                if not answers_df.empty:
                    video_answers = answers_df[
                        (answers_df["Video ID"] == video_id) & 
                        (answers_df["User ID"].isin(annotator_user_ids))
                    ]
                    
                    cache_data["annotator_answers"][question_id] = video_answers.to_dict('records')
                    
                    # Extract confidence scores for model users
                    for _, row in video_answers.iterrows():
                        user_id = row["User ID"]
                        if row["Confidence Score"] is not None:
                            if user_id not in cache_data["confidence_scores"]:
                                cache_data["confidence_scores"][user_id] = {}
                            cache_data["confidence_scores"][user_id][question_id] = row["Confidence Score"]
            
            # Cache user information
            users_df = AuthService.get_all_users(session=session)
            for _, user_row in users_df.iterrows():
                if user_row["ID"] in annotator_user_ids:
                    cache_data["user_info"][user_row["ID"]] = {
                        "name": user_row["User ID"],
                        "email": user_row["Email"],
                        "role": user_row["Role"]
                    }
            
            # OPTIMIZED: Get user weights using service layer
            try:
                # Use the service method to get user weights for the project
                user_weights = AuthService.get_user_weights_for_project(project_id=project_id, session=session)
                for user_id in annotator_user_ids:
                    if user_id in user_weights:
                        cache_data["user_weights"][user_id] = user_weights[user_id]
                    else:
                        cache_data["user_weights"][user_id] = 1.0
            except Exception as e:
                print(f"Error getting user weights: {e}")
                # Default all weights to 1.0 if error
                for user_id in annotator_user_ids:
                    cache_data["user_weights"][user_id] = 1.0
            
            # Cache text answers for description questions
            description_questions = [q for q in questions if q["type"] == "description"]
            for question in description_questions:
                text_answers = GroundTruthService.get_question_text_answers(
                    video_id=video_id, project_id=project_id, 
                    question_id=question["id"], annotator_user_ids=annotator_user_ids, 
                    session=session
                )
                cache_data["text_answers"][question["id"]] = text_answers
            
            return cache_data
            
        except Exception as e:
            print(f"Error in get_cached_video_reviewer_data: {e}")
            return {}

@st.cache_data(ttl=3600)  # Cache for 1 hour - questions rarely change
def get_cached_questions_by_group(group_id: int, session_id: str) -> List[Dict]:
    """Cache questions by group - questions rarely change once project is running"""
    with SessionLocal() as session:
        try:
            return QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_questions_by_group: {e}")
            return []

def get_questions_by_group_cached(group_id: int, session: Session) -> List[Dict]:
    """Get questions by group with caching"""
    session_id = get_session_cache_key()
    return get_cached_questions_by_group(group_id, session_id)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_project_metadata(project_id: int, session_id: str) -> Dict:
    """Cache project metadata - changes infrequently"""
    with SessionLocal() as session:
        try:
            return ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_project_metadata: {e}")
            return {}

def get_project_metadata_cached(project_id: int, session: Session) -> Dict:
    """Get project metadata with caching"""
    session_id = get_session_cache_key()
    return get_cached_project_metadata(project_id, session_id)

def get_cached_user_completion_progress(project_id: int, session: Session) -> Dict[int, float]:
    """Cache completion progress for all users in a project"""
    cache_key = f"completion_progress_{project_id}"
    
    if cache_key not in st.session_state:
        try:
            annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
            progress_map = {}
            
            for annotator_display, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id:
                    try:
                        progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                        progress_map[user_id] = progress
                    except:
                        print(f"Error calculating progress for user {user_id}: {e}")
                        progress_map[user_id] = 0.0
            
            st.session_state[cache_key] = progress_map
        except:
            print(f"Error caching completion progress for project {project_id}: {e}")
            st.session_state[cache_key] = {}
    
    return st.session_state[cache_key]

@st.cache_data(ttl=3600)  # Cache for 1 hour - custom display changes rarely
def get_cached_custom_display_data(project_id: int, session_id: str) -> Dict[str, Any]:
    """Cache all custom display data for a project to minimize database calls"""
    with SessionLocal() as session:
        try:
            # Check if project's schema has custom display enabled
            project = ProjectService.get_project_by_id(project_id=project_id, session=session)
            
            try:
                schema_details = SchemaService.get_schema_details(schema_id=project.schema_id, session=session)
                if not schema_details.get("has_custom_display", False):
                    return {"has_custom_display": False, "custom_displays": {}}
            except ValueError:
                # Schema not found
                return {"has_custom_display": False, "custom_displays": {}}
            
            # Get all custom display entries for this project
            custom_displays = CustomDisplayService.get_all_custom_displays_for_project(
                project_id=project_id, session=session
            )
            
            # Organize by video_id and question_id for efficient lookup
            organized_displays = {}
            for display in custom_displays:
                video_id = display["video_id"]
                question_id = display["question_id"]
                
                if video_id not in organized_displays:
                    organized_displays[video_id] = {}
                
                organized_displays[video_id][question_id] = {
                    "custom_display_text": display["custom_display_text"],
                    "custom_option_display_map": display["custom_option_display_map"]
                }
            
            return {
                "has_custom_display": True,
                "custom_displays": organized_displays,
                "schema_id": schema_details["id"]
            }
            
        except Exception as e:
            print(f"Error in get_cached_custom_display_data: {e}")
            return {"has_custom_display": False, "custom_displays": {}}


def get_project_custom_display_data(project_id: int, session: Session) -> Dict[str, Any]:
    """Get custom display data for project with session state caching"""
    cache_key = f"custom_display_data_{project_id}"
    
    if cache_key not in st.session_state:
        session_id = get_session_cache_key()
        st.session_state[cache_key] = get_cached_custom_display_data(project_id, session_id)
    
    return st.session_state[cache_key]

def get_questions_by_group_with_custom_display_cached(
    group_id: int, 
    project_id: int, 
    video_id: int, 
    session: Session
) -> List[Dict]:
    """Get questions by group with custom display applied, using cached data"""
    
    # Get custom display data for project
    custom_display_data = get_project_custom_display_data(project_id, session)
    
    if not custom_display_data["has_custom_display"]:
        # No custom display - use original method
        return get_questions_by_group_cached(group_id, session)
    
    # Get base questions
    base_questions = get_questions_by_group_cached(group_id, session)
    
    # Apply custom display from cached data
    custom_displays = custom_display_data["custom_displays"].get(video_id, {})
    
    enhanced_questions = []
    for question in base_questions:
        question_id = question["id"]
        enhanced_question = question.copy()
        
        # Apply custom display if available
        if question_id in custom_displays:
            custom_data = custom_displays[question_id]
            
            # Override display text if available
            if custom_data["custom_display_text"]:
                enhanced_question["display_text"] = custom_data["custom_display_text"]
            
            # Override display values if available
            if (custom_data["custom_option_display_map"] and 
                question["type"] == "single" and 
                question["options"]):
                
                new_display_values = []
                for option_value in question["options"]:
                    custom_display_text = custom_data["custom_option_display_map"].get(option_value)
                    if custom_display_text:
                        new_display_values.append(custom_display_text)
                    else:
                        # Fall back to original display value
                        original_index = question["options"].index(option_value)
                        original_display = (question["display_values"][original_index] 
                                          if question["display_values"] else option_value)
                        new_display_values.append(original_display)
                
                enhanced_question["display_values"] = new_display_values
        
        enhanced_questions.append(enhanced_question)
    
    return enhanced_questions

def clear_custom_display_cache(project_id: int):
    """Clear custom display cache for a specific project"""
    cache_key = f"custom_display_data_{project_id}"
    if cache_key in st.session_state:
        del st.session_state[cache_key]

@st.cache_data(ttl=3600)  # Cache for 1 hour - ground truth state changes infrequently
def get_cached_project_has_full_ground_truth(project_id: int, session_id: str) -> bool:
    """Cache project full ground truth check - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return ProjectService.check_project_has_full_ground_truth(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_project_has_full_ground_truth: {e}")
            return False

@st.cache_data(ttl=1800)  # Cache for 30 minutes - accuracy data changes infrequently  
def get_cached_annotator_accuracy(project_id: int, session_id: str) -> Dict[int, Dict[int, Dict[str, int]]]:
    """Cache annotator accuracy data - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_annotator_accuracy: {e}")
            return {}

@st.cache_data(ttl=1800)  # Cache for 30 minutes - reviewer accuracy data changes infrequently
def get_cached_reviewer_accuracy(project_id: int, session_id: str) -> Dict[int, Dict[int, Dict[str, int]]]:
    """Cache reviewer accuracy data - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_reviewer_accuracy: {e}")
            return {}


###############################################################################
# Data Fetching Functions
###############################################################################

def get_optimized_annotator_user_ids(display_names: List[str], project_id: int, session: Session) -> List[int]:
    """Optimized version using cached annotator data"""
    if not display_names:
        return []
    
    annotators = get_optimized_all_project_annotators(project_id, session)
    user_ids = []
    
    for display_name in display_names:
        if display_name in annotators:
            user_id = annotators[display_name].get('id')
            if user_id:
                user_ids.append(user_id)
    
    return user_ids

def get_project_has_full_ground_truth_cached(project_id: int, session: Session) -> bool:
    """Get project full ground truth status with caching"""
    session_id = get_session_cache_key()
    return get_cached_project_has_full_ground_truth(project_id, session_id)

def get_annotator_accuracy_cached(project_id: int, session: Session) -> Dict[int, Dict[int, Dict[str, int]]]:
    """Get annotator accuracy data with caching"""
    session_id = get_session_cache_key()
    return get_cached_annotator_accuracy(project_id, session_id)

def get_reviewer_accuracy_cached(project_id: int, session: Session) -> Dict[int, Dict[int, Dict[str, int]]]:
    """Get reviewer accuracy data with caching"""
    session_id = get_session_cache_key()
    return get_cached_reviewer_accuracy(project_id, session_id)


def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
    """Calculate user's overall progress"""
    try:
        return AnnotatorService.calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error calculating user progress: {str(e)}")
        return 0.0

def get_optimized_all_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Optimized version of get_all_project_annotators using caching"""
    session_id = get_session_cache_key()
    return get_cached_project_annotators(project_id, session_id)


def get_session_cached_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Get project annotators with session state caching"""
    cache_key = f"cached_annotators_{project_id}"
    
    if cache_key not in st.session_state:
        st.session_state[cache_key] = get_optimized_all_project_annotators(project_id=project_id, session=session)
    
    return st.session_state[cache_key]

def get_schema_question_groups(schema_id: int, session: Session) -> List[Dict]:
    """Get question groups in a schema - with caching"""
    cache_key = f"schema_groups_{schema_id}"
    
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = SchemaService.get_schema_question_groups_list(schema_id=schema_id, session=session)
        except ValueError as e:
            st.error(f"Error loading schema question groups: {str(e)}")
            return []
    
    return st.session_state[cache_key]

def get_project_videos(project_id: int, session: Session) -> List[Dict]:
    """Get videos in a project - with caching"""
    session_id = get_session_cache_key()
    return get_cached_project_videos(project_id, session_id)


def get_project_groups_with_projects(user_id: int, role: str, session: Session) -> Dict:
    """Get project groups with their projects for a user"""
    try:
        return ProjectGroupService.get_grouped_projects_for_user(user_id=user_id, role=role, session=session)
    except ValueError as e:
        st.error(f"Error getting grouped projects: {str(e)}")
        return {}

def get_user_assignment_dates(user_id: int, session: Session) -> Dict[int, Dict[str, str]]:
    """Get assignment dates for all projects for a specific user"""
    try:
        assignments_df = AuthService.get_project_assignments(session=session)
        user_assignments = {}
        
        for _, assignment in assignments_df.iterrows():
            if assignment["User ID"] == user_id:
                project_id = assignment["Project ID"]
                role = assignment["Role"]
                
                if project_id not in user_assignments:
                    user_assignments[project_id] = {}
                
                assigned_at = assignment.get("Assigned At")
                if assigned_at:
                    try:
                        date_str = assigned_at.strftime("%Y-%m-%d") if hasattr(assigned_at, 'strftime') else str(assigned_at)[:10]
                        user_assignments[project_id][role] = date_str
                    except:
                        user_assignments[project_id][role] = "Unknown"
                else:
                    user_assignments[project_id][role] = "Not set"
        
        return user_assignments
    except Exception as e:
        st.error(f"Error getting assignment dates: {str(e)}")
        return {}


###############################################################################
# Cache Management Functions
###############################################################################

def clear_accuracy_cache_for_project(project_id: int):
    """Clear accuracy cache for a specific project"""
    session_id = get_session_cache_key()
    
    # Try to clear each function individually
    try:
        get_cached_project_has_full_ground_truth.clear(project_id, session_id)
    except Exception:
        print(f"Error clearing project has full ground truth cache: {e}")
        pass  # Function may not have been called yet
    
    try:
        get_cached_annotator_accuracy.clear(project_id, session_id)
    except Exception:
        print(f"Error clearing annotator accuracy cache: {e}")
        pass  # Function may not have been called yet
    
    try:
        get_cached_reviewer_accuracy.clear(project_id, session_id)
    except Exception:
        print(f"Error clearing reviewer accuracy cache: {e}")
        pass  # Function may not have been called yet

# # Update the existing clear_project_cache function
# def clear_project_cache(project_id: int):
#     """Clear all cached data for a specific project"""
#     cache_keys_to_clear = []
#     for key in st.session_state.keys():
#         if (f"cache_project_{project_id}" in key or 
#             f"cached_annotators_{project_id}" in key or
#             f"completion_progress_{project_id}" in key or
#             f"question_groups_{project_id}" in key or
#             f"custom_display_data_{project_id}" in key):
#             cache_keys_to_clear.append(key)
    
#     for key in cache_keys_to_clear:
#         del st.session_state[key]
    
#     # Clear accuracy cache for this project
#     clear_accuracy_cache_for_project(project_id)

def clear_project_cache(project_id: int):
    """Clear all cached data for a specific project"""
    cache_keys_to_clear = []
    for key in st.session_state.keys():
        if any(pattern in key for pattern in [
            f"cache_project_{project_id}",
            f"cached_annotators_{project_id}",
            f"completion_progress_{project_id}",
            f"question_groups_{project_id}",
            f"custom_display_data_{project_id}",
            # Sorting/filtering state (no role)
            f"video_sort_by_{project_id}",
            f"video_sort_order_{project_id}",
            f"sort_config_{project_id}",
            f"sort_applied_{project_id}",
            f"video_filters_{project_id}",
            # Cached results (has role suffix)
            f"applied_sorted_and_filtered_videos_{project_id}_",  # Will match all roles
            # Annotator-specific sorting
            f"annotator_video_sort_by_{project_id}",
            f"annotator_video_sort_order_{project_id}",
            f"annotator_sort_applied_{project_id}",
            # Pagination (has role)
            f"_current_page_{project_id}",  # Will match annotator_current_page_X, reviewer_current_page_X
        ]):
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        del st.session_state[key]
    
    # Clear accuracy cache
    clear_accuracy_cache_for_project(project_id)
    
    # Clear streamlit cache for this project
    session_id = get_session_cache_key()
    try:
        get_cached_project_metadata.clear(project_id, session_id)
    except:
        pass

def get_session_cache_key():
    """Generate a session-based cache key that changes when user logs in/out"""
    user_id = st.session_state.get('user', {}).get('id', 'anonymous')
    return f"session_{user_id}"

###############################################################################
# ANSWER OR GROUND TRUTH CHECKS
###############################################################################

def check_project_has_full_ground_truth(project_id: int, session: Session) -> bool:
    """Check if project has complete ground truth for ALL questions and videos"""
    try:
        return get_project_has_full_ground_truth_cached(project_id, session)
    except Exception as e:
        print(f"Error checking project has full ground truth: {e}")
        return False

def check_all_questions_have_ground_truth(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    try:
        return GroundTruthService.check_all_questions_have_ground_truth_for_group(video_id, project_id, question_group_id, session)
    except Exception as e:
        print(f"Error checking all questions have ground truth: {e}")
        return False

def check_ground_truth_exists_for_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return False
        
        # Check if ANY question in the group has ground truth
        for question in questions:
            if GroundTruthService.check_ground_truth_exists_for_question(video_id=video_id, project_id=project_id, question_id=question["id"], session=session):
                return True
        return False
    except Exception as e:
        print(f"Error checking ground truth exists for group: {e}")
        return False