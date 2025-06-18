import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Any
from datetime import datetime
from sqlalchemy.orm import Session
from contextlib import contextmanager
import hashlib

# Import database
from db import SessionLocal
from services import (
    AuthService, AnnotatorService, GroundTruthService, 
    QuestionService, QuestionGroupService, SchemaService,
    ProjectService, VideoService
)

###############################################################################
# CONSTANTS AND SHARED STYLES
###############################################################################

# Common color schemes for consistency
COLORS = {
    'primary': '#9553FE',
    'success': '#28a745',
    'warning': '#ffc107', 
    'danger': '#dc3545',
    'info': '#3498db',
    'secondary': '#6c757d'
}

def get_card_style(color, opacity=0.15):
    """Generate consistent card styling"""
    return f"""
    background: linear-gradient(135deg, {color}{opacity}, {color}08);
    border: 2px solid {color};
    border-radius: 12px;
    padding: 16px;
    margin: 12px 0;
    box-shadow: 0 2px 8px {color}20;
    """

def custom_info(text: str):
    """Custom info box with purple styling"""
    st.markdown(f"""
    <div style="background: #EAE1F9; border-radius: 8px; padding: 12px 16px; margin: 8px 0;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            {text}
        </div>
    </div>
    """, unsafe_allow_html=True)

###############################################################################
# CACHING UTILITIES
###############################################################################

def get_cache_key(*args):
    """Generate a cache key from arguments"""
    key_str = "_".join(str(arg) for arg in args)
    return hashlib.md5(key_str.encode()).hexdigest()[:16]

def clear_project_cache(project_id: int):
    """Clear all cached data for a specific project"""
    cache_keys_to_clear = []
    for key in st.session_state.keys():
        if f"cache_project_{project_id}" in key:
            cache_keys_to_clear.append(key)
    
    for key in cache_keys_to_clear:
        del st.session_state[key]

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

def get_session_cache_key():
    """Generate a session-based cache key that changes when user logs in/out"""
    user_id = st.session_state.get('user', {}).get('id', 'anonymous')
    return f"session_{user_id}"

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def get_cached_project_videos(project_id: int, session_id: str) -> List[Dict]:
    """Cache project videos - changes infrequently"""
    with SessionLocal() as session:  # Use SessionLocal directly
        try:
            return VideoService.get_project_videos(project_id=project_id, session=session)
        except Exception as e:
            print(f"Error in get_cached_project_videos: {e}")
            return []

def get_project_videos(project_id: int, session: Session) -> List[Dict]:
    """Get videos in a project - with caching"""
    session_id = get_session_cache_key()
    return get_cached_project_videos(project_id, session_id)

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
###############################################################################
# OPTIMIZED HELPER FUNCTIONS
###############################################################################

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

def get_optimized_all_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Optimized version of get_all_project_annotators using caching"""
    session_id = get_session_cache_key()
    return get_cached_project_annotators(project_id, session_id)

def get_optimized_question_answers(question_id: int, project_id: int, session: Session) -> pd.DataFrame:
    """Get answers for a specific question from cached project answers"""
    session_id = get_session_cache_key()
    all_answers_df = get_cached_question_answers(project_id, session_id)
    
    if all_answers_df.empty:
        return pd.DataFrame()
    
    # Filter for the specific question
    question_answers = all_answers_df[all_answers_df["Question ID"] == question_id]
    return question_answers

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

def calculate_overall_accuracy(accuracy_data: Dict[int, Dict[int, Dict[str, int]]]) -> Dict[int, Optional[float]]:
    """Calculate overall accuracy for each user from accuracy data"""
    overall_accuracy = {}
    
    for user_id, question_data in accuracy_data.items():
        total_questions = sum(stats["total"] for stats in question_data.values() if stats["total"] > 0)
        total_correct = sum(stats["correct"] for stats in question_data.values() if stats["total"] > 0)
        
        overall_accuracy[user_id] = (total_correct / total_questions) * 100 if total_questions > 0 else None
    
    return overall_accuracy

def calculate_per_question_accuracy(accuracy_data: Dict[int, Dict[int, Dict[str, int]]]) -> Dict[int, Dict[int, Optional[float]]]:
    """Calculate per-question accuracy for each user from accuracy data"""
    per_question_accuracy = {}
    
    for user_id, question_data in accuracy_data.items():
        per_question_accuracy[user_id] = {
            question_id: (stats["correct"] / stats["total"]) * 100 if stats["total"] > 0 else None
            for question_id, stats in question_data.items()
        }
    
    return per_question_accuracy

###############################################################################
# OPTIMIZED DISPLAY FUNCTIONS
###############################################################################

def _display_enhanced_helper_text_answers(video_id: int, project_id: int, question_id: int, question_text: str, text_key: str, gt_value: str, role: str, answer_reviews: Optional[Dict], session: Session, selected_annotators: List[str] = None):
    """OPTIMIZED: Display helper text showing other annotator answers"""
    
    try:
        annotator_user_ids = []
        text_answers = []
        
        # Get annotator answers if annotators are selected - OPTIMIZED
        if selected_annotators:
            annotator_user_ids = get_optimized_annotator_user_ids(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            
            text_answers = GroundTruthService.get_question_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                annotator_user_ids=annotator_user_ids, session=session
            )
        
        all_answers = []
        
        # Add Ground Truth for meta-reviewer OR if we have existing GT (search portal)
        if role == "meta_reviewer" and gt_value:
            all_answers.append({
                "name": "Ground Truth",
                "full_text": gt_value,
                "has_answer": bool(gt_value.strip()),
                "is_gt": True,
                "display_name": "Ground Truth"
            })
        # For search portal - check if we have ground truth even without annotators
        elif not selected_annotators or len(selected_annotators) == 0:
            try:
                gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                if not gt_df.empty:
                    question_gt = gt_df[gt_df["Question ID"] == question_id]
                    if not question_gt.empty:
                        gt_answer = question_gt.iloc[0]["Answer Value"]
                        if gt_answer and str(gt_answer).strip():
                            all_answers.append({
                                "name": "Ground Truth",
                                "full_text": str(gt_answer),
                                "has_answer": True,
                                "is_gt": True,
                                "display_name": "Ground Truth"
                            })
            except Exception as e:
                print(f"Error getting ground truth: {e}")
                pass
        
        # Add annotator answers
        if text_answers:
            unique_answers = []
            seen_answers = set()
            
            for answer in text_answers:
                answer_key = f"{answer['initials']}:{answer['answer_value']}"
                if answer_key not in seen_answers:
                    seen_answers.add(answer_key)
                    unique_answers.append(answer)
            
            for answer_info in unique_answers:
                annotator_name = answer_info['name']
                answer_value = answer_info['answer_value']
                initials = answer_info['initials']
                
                all_answers.append({
                    "name": annotator_name,
                    "full_text": answer_value,
                    "has_answer": bool(answer_value.strip()),
                    "is_gt": False,
                    "display_name": f"{annotator_name} ({initials})",
                    "initials": initials
                })
        
        if all_answers:
            # Smart tab naming
            if len(all_answers) > 6:
                tab_names = []
                for answer in all_answers:
                    if answer["is_gt"]:
                        tab_names.append("🏆 GT")
                    else:
                        tab_names.append(answer["initials"])
            else:
                tab_names = []
                for answer in all_answers:
                    if answer["is_gt"]:
                        tab_names.append("Ground Truth")
                    else:
                        name = answer["name"]
                        tab_names.append(name[:17] + "..." if len(name) > 20 else name)
            
            tabs = st.tabs(tab_names)
            
            for tab, answer in zip(tabs, all_answers):
                with tab:
                    _display_single_answer_elegant(
                        answer, text_key, question_text, answer_reviews, 
                        video_id, project_id, question_id, session
                    )
                            
    except Exception as e:
        st.caption(f"⚠️ Could not load answer information: {str(e)}")

def _display_clean_sticky_single_choice_question(
    question: Dict, 
    video_id: int, 
    project_id: int, 
    group_id: int,
    role: str, 
    existing_value: str, 
    is_modified_by_admin: bool, 
    admin_info: Optional[Dict], 
    form_disabled: bool, 
    session: Session, 
    gt_value: str = "", 
    mode: str = "", 
    selected_annotators: List[str] = None, 
    key_prefix: str = "", 
    preloaded_answers: Dict = None
) -> str:
    """OPTIMIZED: Display a single choice question with preloaded answer support"""
    question_id = question["id"]
    question_text = question["display_text"]
    options = question["options"]
    display_values = question.get("display_values", options)
    
    display_to_value = dict(zip(display_values, options))
    value_to_display = dict(zip(options, display_values))
    
    # Get preloaded answer
    preloaded_value = ""
    if preloaded_answers:
        key = (video_id, group_id, question_text)
        preloaded_value = preloaded_answers.get(key, "")
    
    # Use preloaded value if available, otherwise use existing value
    current_value = preloaded_value if preloaded_value else existing_value

    if not current_value:
        # Try both possible field names for default option
        default_option = question.get("default_option")
        if default_option and default_option in options:
            current_value = default_option
    
    # Calculate default index using current_value (which includes preloaded)
    default_idx = 0
    if current_value and current_value in value_to_display:
        display_val = value_to_display[current_value]
        if display_val in display_values:
            default_idx = display_values.index(display_val)
    
    # Question header
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    elif role == "meta_reviewer" and is_modified_by_admin:
        st.warning(f"❓ (Overridden by Admin) {question_text}")
    else:
        custom_info(f"❓ {question_text}")
    
    # Training mode feedback
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value == gt_value:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['success'])}">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! You selected the correct answer.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['danger'])}">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        ❌ Incorrect. The ground truth answer is highlighted below.
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Show unified status for reviewers/meta-reviewers - OPTIMIZED
    if role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        _display_unified_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or []
        )
    
    # Question content with UNIQUE KEYS using key_prefix
    if role == "reviewer" and is_modified_by_admin and admin_info:
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]

        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            options=options, display_values=display_values, session=session,
            selected_annotators=selected_annotators or []
        )
        
        admin_idx = default_idx
        if current_value and current_value in value_to_display:
            admin_display_val = value_to_display[current_value]
            if admin_display_val in display_values:
                admin_idx = display_values.index(admin_display_val)
        
        st.warning(f"🔒 **Overridden by {admin_name}**")
        
        st.radio(
            "Admin's selection:",
            options=enhanced_options,
            index=admin_idx,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_locked",
            disabled=True,
            label_visibility="collapsed",
            horizontal=True
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        # OPTIMIZED: Use cached enhanced options
        enhanced_options = _get_enhanced_options_for_reviewer(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            options=options, display_values=display_values, session=session,
            selected_annotators=selected_annotators or []
        )
        
        radio_key = f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_stable"
        
        # Use default_idx which now includes preloaded values
        current_idx = default_idx
        if current_value:
            for i, opt in enumerate(options):
                if opt == current_value:
                    current_idx = i
                    break
        
        selected_idx = st.radio(
            "Select your answer:",
            options=range(len(enhanced_options)),
            format_func=lambda x: enhanced_options[x],
            index=current_idx,
            key=radio_key,
            disabled=form_disabled,
            label_visibility="collapsed",
            horizontal=True
        )
        result = options[selected_idx]
        
    else:
        # Regular display for annotators
        if mode == "Training" and form_disabled and gt_value:
            enhanced_display_values = []
            for i, display_val in enumerate(display_values):
                actual_val = options[i] if i < len(options) else display_val
                if actual_val == gt_value:
                    enhanced_display_values.append(f"🏆 {display_val} (Ground Truth)")
                elif actual_val == existing_value and actual_val != gt_value:
                    enhanced_display_values.append(f"❌ {display_val} (Your Answer)")
                else:
                    enhanced_display_values.append(display_val)
            
            selected_display = st.radio(
                "Select your answer:",
                options=enhanced_display_values,
                index=default_idx,
                key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            
            result = display_to_value.get(
                selected_display.replace("🏆 ", "").replace(" (Ground Truth)", "").replace("❌ ", "").replace(" (Your Answer)", ""), 
                existing_value
            )
        else:
            selected_display = st.radio(
                "Select your answer:",
                options=display_values,
                index=default_idx,
                key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
                disabled=form_disabled,
                label_visibility="collapsed",
                horizontal=True
            )
            result = display_to_value[selected_display]
    
    return result

def _display_clean_sticky_description_question(
    question: Dict, 
    video_id: int, 
    project_id: int, 
    group_id: int,
    role: str, 
    existing_value: str, 
    is_modified_by_admin: bool, 
    admin_info: Optional[Dict], 
    form_disabled: bool, 
    session: Session, 
    gt_value: str = "", 
    mode: str = "", 
    answer_reviews: Optional[Dict] = None, 
    selected_annotators: List[str] = None, 
    key_prefix: str = "", 
    preloaded_answers: Dict = None
) -> str:
    """OPTIMIZED: Display a description question with preloaded answer support"""
    
    question_id = question["id"]
    question_text = question["display_text"]
    question_key = question["text"]
    
    # Get preloaded answer
    preloaded_value = ""
    if preloaded_answers:
        key = (video_id, group_id, question_text)
        preloaded_value = preloaded_answers.get(key, "")
    
    # Use preloaded value if available, otherwise use existing value
    current_value = preloaded_value if preloaded_value else existing_value
    
    # Question header
    if role == "reviewer" and is_modified_by_admin:
        st.error(f"🔒 {question_text}")
    elif role == "meta_reviewer" and is_modified_by_admin:
        st.warning(f"❓ (Overridden by Admin) {question_text}")
    else:
        custom_info(f"❓ {question_text}")
    
    # Training mode feedback
    if mode == "Training" and form_disabled and gt_value and role == "annotator":
        if existing_value.strip().lower() == gt_value.strip().lower():
            st.markdown(f"""
                <div style="{get_card_style(COLORS['success'])}">
                    <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
                        ✅ Excellent! Your answer matches the ground truth exactly.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        elif existing_value.strip():
            st.markdown(f"""
                <div style="{get_card_style(COLORS['info'])}">
                    <span style="color: #2980b9; font-weight: 600; font-size: 0.95rem;">
                        📚 Great work! Check the ground truth below to learn from this example.
                    </span>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['danger'])}">
                    <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
                        📝 Please provide an answer next time. See the ground truth below to learn!
                    </span>
                </div>
            """, unsafe_allow_html=True)
    
    # Question content with UNIQUE KEYS using key_prefix
    if role == "reviewer" and is_modified_by_admin and admin_info:
        current_value = admin_info["current_value"]
        admin_name = admin_info["admin_name"]
        
        st.warning(f"🔒 **Overridden by {admin_name}**")
        
        answer = st.text_area(
            "Admin's answer:",
            value=current_value,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_locked",
            disabled=True,
            height=120,
            label_visibility="collapsed"
        )
        
        result = current_value
        
    elif role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        text_key = f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}_stable"
        
        answer = st.text_area(
            "Enter your answer:",
            value=current_value,
            key=text_key,
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        # Show unified status - OPTIMIZED
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        _display_unified_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or []
        )
        
        # Show enhanced helper text if annotators selected OR if we have ground truth (for search portal)
        if show_annotators or (role in ["meta_reviewer", "reviewer_resubmit"] and existing_value):
            _display_enhanced_helper_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                question_text=question_key, text_key=text_key,
                gt_value=existing_value if role in ["meta_reviewer", "reviewer_resubmit"] else "",
                role=role, answer_reviews=answer_reviews, session=session,
                selected_annotators=selected_annotators or []
            )
        
        result = answer
        
    else:
        result = st.text_area(
            "Enter your answer:",
            value=current_value,
            key=f"{key_prefix}q_{video_id}_{project_id}_{question_id}_{role}",
            disabled=form_disabled,
            height=120,
            label_visibility="collapsed"
        )
        
        if mode == "Training" and form_disabled and gt_value:
            st.markdown(f"""
                <div style="{get_card_style(COLORS['info'])}">
                    <div style="color: #2980b9; font-weight: 700; font-size: 0.95rem; margin-bottom: 8px;">
                        🏆 Ground Truth Answer (for learning):
                    </div>
                    <div style="color: #34495e; font-size: 0.9rem; line-height: 1.4; background: white; padding: 12px; border-radius: 6px; border-left: 4px solid {COLORS['info']};">
                        {gt_value}
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    return result

def _get_enhanced_options_for_reviewer(video_id: int, project_id: int, question_id: int, options: List[str], display_values: List[str], session: Session, selected_annotators: List[str] = None) -> List[str]:
    """OPTIMIZED: Get enhanced options showing who selected what for reviewers with model confidence scores"""
    
    try:
        # Get annotator selections if annotators are provided - OPTIMIZED
        annotator_user_ids = []
        if selected_annotators:
            annotator_user_ids = get_optimized_annotator_user_ids(
                display_names=selected_annotators, project_id=project_id, session=session
            )
        
        # Get user info from cached data - OPTIMIZED
        session_id = get_session_cache_key()
        users_df = get_cached_all_users(session_id)
        user_lookup = {row["ID"]: {"role": row["Role"], "name": row["User ID"]} for _, row in users_df.iterrows()}
        
        # Create reverse lookup: user_name -> user_info
        name_to_user_lookup = {row["User ID"]: {"id": row["ID"], "role": row["Role"]} for _, row in users_df.iterrows()}
        
        option_selections = GroundTruthService.get_question_option_selections(
            video_id=video_id, project_id=project_id, question_id=question_id, 
            annotator_user_ids=annotator_user_ids, session=session
        )
        
        # Get answers with confidence scores for models - OPTIMIZED
        answers_df = get_optimized_question_answers(question_id=question_id, project_id=project_id, session=session)
        
        confidence_map = {}
        if not answers_df.empty:
            video_answers = answers_df[answers_df["Video ID"] == video_id]
            for _, answer_row in video_answers.iterrows():
                user_id = answer_row["User ID"]
                confidence = answer_row["Confidence Score"]
                if confidence is not None:
                    confidence_map[int(user_id)] = confidence
        
        # Also get ground truth info for search portal
        gt_selection = None
        try:
            gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
            if not gt_df.empty:
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                if not question_gt.empty:
                    gt_selection = question_gt.iloc[0]["Answer Value"]
        except Exception as e:
            print(f"Error getting ground truth: {e}")
            pass
        
    except Exception as e:
        # Fallback: still check for ground truth even if annotator data fails
        gt_selection = None
        option_selections = {}
        try:
            gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
            if not gt_df.empty:
                question_gt = gt_df[gt_df["Question ID"] == question_id]
                if not question_gt.empty:
                    gt_selection = question_gt.iloc[0]["Answer Value"]
        except:
            pass
        confidence_map = {}
        user_lookup = {}
        name_to_user_lookup = {}
    
    total_annotators = len(annotator_user_ids) if selected_annotators else 0
    enhanced_options = []
    
    for i, display_val in enumerate(display_values):
        actual_val = options[i] if i < len(options) else display_val
        option_text = display_val
        
        selection_info = []
        
        # Add annotator selection info if available with confidence scores for models
        if actual_val in option_selections:
            annotators = []
            for selector in option_selections[actual_val]:
                if selector["type"] != "ground_truth":  # Don't double-count GT here
                    user_name = selector.get("name", "Unknown User")
                    
                    # Get initials using centralized function
                    _, initials = AuthService.get_user_display_name_with_initials(user_name)
                    
                    # Look up user info from user_name
                    user_info = name_to_user_lookup.get(user_name, {})
                    user_id_int = user_info.get("id")
                    user_role = user_info.get("role", "human")
                    
                    # Add confidence score inside initials for model users
                    if user_role == "model" and user_id_int in confidence_map:
                        annotator_text = f"{initials} ({confidence_map[user_id_int]:.2f})"
                    elif user_role == "model":
                        annotator_text = f"{initials} (🤖)"
                    else:
                        annotator_text = initials
                    
                    annotators.append(annotator_text)
            
            if annotators and total_annotators > 0:
                count = len(annotators)
                percentage = (count / total_annotators) * 100
                percentage_str = f"{int(percentage)}%" if percentage == int(percentage) else f"{percentage:.1f}%"
                selection_info.append(f"{percentage_str}: {', '.join(annotators)}")
            elif annotators:
                selection_info.append(f"{', '.join(annotators)}")
        
        # Add ground truth indicator if this option is the GT
        if gt_selection and str(actual_val) == str(gt_selection):
            selection_info.append("🏆 GT")
        
        if selection_info:
            option_text += f" — {' | '.join(selection_info)}"
        
        enhanced_options.append(option_text)
    
    return enhanced_options

def _display_unified_status(video_id: int, project_id: int, question_id: int, session: Session, show_annotators: bool = False, selected_annotators: List[str] = None):
    """OPTIMIZED: Display ground truth status and optionally annotator status with clean user display names"""
    
    status_parts = []
    
    # Get annotator status first if requested - OPTIMIZED
    if show_annotators and selected_annotators:
        try:
            # Step 1: Get annotator user IDs
            annotator_user_ids = get_optimized_annotator_user_ids(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            
            if not annotator_user_ids:
                status_parts.append("⚠️ No annotator IDs found")
            else:
                # Step 2: Get answers for this question/video - OPTIMIZED
                answers_df = get_optimized_question_answers(question_id=question_id, project_id=project_id, session=session)
                
                # Step 3: Get user info from cached data - OPTIMIZED
                session_id = get_session_cache_key()
                users_df = get_cached_all_users(session_id)
                
                if users_df.empty:
                    status_parts.append("⚠️ No user data available")
                else:
                    user_lookup = {row["ID"]: {"role": row["Role"], "name": row["User ID"]} for _, row in users_df.iterrows()}
                    
                    # Find which annotators have answered this specific question/video
                    annotators_with_answers = set()
                    annotators_missing = set()
                    
                    if not answers_df.empty:
                        video_answers = answers_df[answers_df["Video ID"] == video_id]
                        answered_user_ids = set(video_answers["User ID"].tolist())
                        
                        for user_id in annotator_user_ids:
                            user_info = user_lookup.get(user_id, {})
                            user_name = user_info.get("name", f"User {user_id}")
                            
                            if user_id in answered_user_ids:
                                annotators_with_answers.add(user_name)
                            else:
                                annotators_missing.add(user_name)
                    else:
                        # No answers at all - all are missing
                        for user_id in annotator_user_ids:
                            user_info = user_lookup.get(user_id, {})
                            user_name = user_info.get("name", f"User {user_id}")
                            annotators_missing.add(user_name)
                    
                    # Add annotator status parts with initials, no truncation
                    if annotators_with_answers:
                        display_names = []
                        for user_name in annotators_with_answers:
                            display_name, _ = AuthService.get_user_display_name_with_initials(user_name)
                            display_names.append(display_name)
                        status_parts.append(f"📊 Answered: {', '.join(display_names)}")
                    
                    if annotators_missing:
                        display_names = []
                        for user_name in annotators_missing:
                            display_name, _ = AuthService.get_user_display_name_with_initials(user_name)
                            display_names.append(display_name)
                        status_parts.append(f"⚠️ Missing: {', '.join(display_names)}")
                        
        except Exception as e:
            print(f"Detailed error in annotator status: {e}")
            import traceback
            traceback.print_exc()  # This will help debug the exact issue
            status_parts.append(f"⚠️ Annotator status error: {str(e)[:50]}")
    
    # Ground truth status (existing code remains the same)
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        
        if not gt_df.empty:
            question_id_int = int(question_id)
            question_gt = gt_df[gt_df["Question ID"] == question_id_int]
            
            if not question_gt.empty:
                gt_row = question_gt.iloc[0]
                
                try:
                    reviewer_info = AuthService.get_user_info_by_id(user_id=int(gt_row["Reviewer ID"]), session=session)
                    reviewer_name = reviewer_info["user_id_str"]
                    
                    modified_by_admin = gt_row["Modified By Admin"] is not None
                    
                    if modified_by_admin:
                        status_parts.append(f"🏆 GT by: {reviewer_name} (Admin)")
                    else:
                        status_parts.append(f"🏆 GT by: {reviewer_name}")
                except Exception:
                    status_parts.append("🏆 GT exists")
            else:
                status_parts.append("📭 No GT")
        else:
            status_parts.append("📭 No GT")
            
    except Exception as e:
        print(f"Error getting ground truth status: {e}")
        status_parts.append("📭 GT error")
    
    # Display single combined status line
    if status_parts:
        st.caption(" | ".join(status_parts))

def get_weighted_votes_for_question_with_custom_weights(
    video_id: int, 
    project_id: int, 
    question_id: int,
    include_user_ids: List[int],
    virtual_responses: List[Dict],
    session: Session,
    user_weights: Dict[int, float] = None,
    custom_option_weights: Dict[str, float] = None
) -> Dict[str, float]:
    """
    OPTIMIZED: MINIMAL MODIFICATION of the original function to use custom option weights for reviewers.
    """
    try:
        from models import User, ProjectUserRole
        from sqlalchemy import select
        
        # Get question details
        question = QuestionService.get_question_by_id(question_id=question_id, session=session)
        if not question:
            return {}
        
        user_weights = user_weights or {}
        vote_weights = {}
        
        # Get real user answers - OPTIMIZED
        answers_df = get_optimized_question_answers(question_id=question_id, project_id=project_id, session=session)
        
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
                
                # MINIMAL CHANGE: Use custom option weights if provided (for reviewers)
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

###############################################################################
# REMAINING UNCHANGED UTILITY FUNCTIONS
###############################################################################

def _display_single_answer_elegant(answer, text_key, question_text, answer_reviews, video_id, project_id, question_id, session):
    """Display a single answer with elegant left-right layout"""
    desc_col, controls_col = st.columns([2.6, 1.4])
    
    with desc_col:
        if answer['has_answer']:
            st.text_area(
                f"{answer['name']}'s Answer:",
                value=answer['full_text'],
                height=200,
                disabled=True,
                key=f"display_{video_id}_{question_id}_{answer['name'].replace(' ', '_')}",
                label_visibility="collapsed"
            )
        else:
            custom_info(f"{answer['name']}: No answer provided")
    
    with controls_col:
        st.markdown(f"**{answer['name']}**")
        
        if not answer['is_gt'] and answer_reviews is not None:
            if question_text not in answer_reviews:
                answer_reviews[question_text] = {}
            
            display_name = answer['display_name']
            current_review_data = answer_reviews[question_text].get(display_name, {
                "status": "pending", "reviewer_id": None, "reviewer_name": None
            })
            
            current_status = current_review_data.get("status", "pending") if isinstance(current_review_data, dict) else current_review_data
            existing_reviewer_name = current_review_data.get("reviewer_name") if isinstance(current_review_data, dict) else None
            
            status_emoji = {"pending": "⏳", "approved": "✅", "rejected": "❌"}[current_status]
            
            if current_status != "pending" and existing_reviewer_name:
                st.caption(f"**Status:** {status_emoji} {current_status.title()}")
                st.caption(f"**Reviewed by:** {existing_reviewer_name}")
            else:
                st.caption(f"**Status:** {status_emoji} {current_status.title()}")
            
            review_key = f"review_{text_key}_{answer['name'].replace(' ', '_')}_{video_id}"
            selected_status = st.segmented_control(
                "Review",
                options=["pending", "approved", "rejected"],
                format_func=lambda x: {"pending": "⏳", "approved": "✅", "rejected": "❌"}[x],
                default=current_status,
                key=review_key,
                label_visibility="collapsed"
            )
            
            answer_reviews[question_text][display_name] = selected_status
            
            if selected_status == current_status:
                if current_status == "pending":
                    st.caption("💭 Don't forget to submit your review.")
                elif existing_reviewer_name:
                    st.caption(f"📝 Click submit to override {existing_reviewer_name}'s review.")
                else:
                    st.caption("📝 Click submit to override the previous review.")
            else:
                if existing_reviewer_name:
                    st.caption(f"📝 Click submit to override {existing_reviewer_name}'s review.")
                else:
                    st.caption("📝 Click submit to save your review.")

def _submit_answer_reviews(answer_reviews: Dict, video_id: int, project_id: int, user_id: int, session: Session):
    """Submit answer reviews for annotators using proper service API"""
    for question_text, reviews in answer_reviews.items():
        for annotator_display, review_data in reviews.items():
            review_status = review_data.get("status", "pending") if isinstance(review_data, dict) else review_data
            
            if review_status in ["approved", "rejected", "pending"]:
                try:
                    # OPTIMIZED: Use cached annotator lookup
                    annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
                    annotator_user_id = None
                    
                    for display_name, annotator_info in annotators.items():
                        if display_name == annotator_display:
                            annotator_user_id = annotator_info.get('id')
                            break
                    
                    if annotator_user_id:
                        question = QuestionService.get_question_by_text(text=question_text, session=session)
                        answers_df = AnnotatorService.get_answers(video_id=video_id, project_id=project_id, session=session)
                        
                        if not answers_df.empty:
                            answer_row = answers_df[
                                (answers_df["Question ID"] == int(question["id"])) & 
                                (answers_df["User ID"] == int(annotator_user_id))
                            ]
                            
                            if not answer_row.empty:
                                answer_id = int(answer_row.iloc[0]["Answer ID"])
                                GroundTruthService.submit_answer_review(
                                    answer_id=answer_id, reviewer_id=user_id, 
                                    status=review_status, session=session
                                )
                except Exception as e:
                    print(f"Error submitting review for {annotator_display}: {e}")
                    continue

def _load_existing_answer_reviews(video_id: int, project_id: int, question_id: int, session: Session) -> Dict[str, Dict]:
    """OPTIMIZED: Load existing answer reviews for a description question from the database"""
    reviews = {}
    
    try:
        selected_annotators = st.session_state.get("selected_annotators", [])
        annotator_user_ids = get_optimized_annotator_user_ids(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        for user_id in annotator_user_ids:
            answers_df = AnnotatorService.get_answers(video_id=video_id, project_id=project_id, session=session)
            
            if not answers_df.empty:
                answer_row = answers_df[
                    (answers_df["Question ID"] == int(question_id)) & 
                    (answers_df["User ID"] == int(user_id))
                ]
                
                if not answer_row.empty:
                    answer_id = int(answer_row.iloc[0]["Answer ID"])
                    existing_review = GroundTruthService.get_answer_review(answer_id=answer_id, session=session)
                    
                    user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
                    name_parts = user_info["user_id_str"].split()
                    initials = f"{name_parts[0][0]}{name_parts[-1][0]}".upper() if len(name_parts) >= 2 else user_info["user_id_str"][:2].upper()
                    display_name = f"{user_info['user_id_str']} ({initials})"
                    
                    if existing_review:
                        reviews[display_name] = {
                            "status": existing_review["status"],
                            "reviewer_id": existing_review.get("reviewer_id"),
                            "reviewer_name": None
                        }
                        
                        if existing_review.get("reviewer_id"):
                            try:
                                reviewer_info = AuthService.get_user_info_by_id(
                                    user_id=int(existing_review["reviewer_id"]), session=session
                                )
                                reviews[display_name]["reviewer_name"] = reviewer_info["user_id_str"]
                            except ValueError:
                                reviews[display_name]["reviewer_name"] = f"User {existing_review['reviewer_id']} (Error loading user info)"
                    else:
                        reviews[display_name] = {"status": "pending", "reviewer_id": None, "reviewer_name": None}
    except Exception as e:
        print(f"Error loading answer reviews: {e}")
    
    return reviews