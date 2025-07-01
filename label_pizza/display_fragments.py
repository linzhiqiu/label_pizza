import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List, Any, Tuple
from sqlalchemy.orm import Session
from contextlib import contextmanager

from label_pizza.custom_video_player import custom_video_player
from label_pizza.services import (
    AuthService, AnnotatorService, GroundTruthService, 
    QuestionService, QuestionGroupService, SchemaService,
    ProjectService, VideoService
)

from label_pizza.ui_components import (
    custom_info, get_card_style, COLORS,
    display_pagination_controls
)
from label_pizza.database_utils import (
    get_db_session, clear_project_cache, get_questions_by_group_cached, get_project_videos,
    get_session_cached_project_annotators, get_optimized_annotator_user_ids,
    get_cached_video_reviewer_data, get_session_cache_key, get_schema_question_groups,
    check_project_has_full_ground_truth, check_all_questions_have_ground_truth,
    check_ground_truth_exists_for_group, get_user_assignment_dates,
    get_project_groups_with_projects, calculate_user_overall_progress,
    get_cached_user_completion_progress, get_optimized_all_project_annotators
)
from label_pizza.autosubmit_features import (
    display_manual_auto_submit_controls, run_project_wide_auto_submit_on_entry,
    display_annotator_checkboxes
)
from label_pizza.accuracy_analytics import display_user_accuracy_simple

###############################################################################
# Video Display Functions
###############################################################################

@st.fragment
def display_video_answer_pair(video: Dict, project_id: int, user_id: int, role: str, mode: str, session: Session):
    """Display a single video-answer pair in side-by-side layout with tabs"""
    try:
        project = ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
        
        # Add transaction recovery for question groups
        try:
            question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
        except Exception as qg_error:
            # Try to recover by creating a new session
            st.error(f"Database error occurred. Refreshing...")
            print(f"Database error occurred: {qg_error}")
            st.rerun()
            return
        
        # Apply custom question group order if set
        order_key = f"question_order_{project_id}_{role}"
        if order_key in st.session_state:
            custom_order = st.session_state[order_key]
            group_lookup = {group["ID"]: group for group in question_groups}
            question_groups = [group_lookup[group_id] for group_id in custom_order if group_id in group_lookup]
        
        if not question_groups:
            custom_info("No question groups found for this project.")
            return
        
        # Check completion status
        completion_status = {}
        for group in question_groups:
            completion_status[group["ID"]] = check_question_group_completion(
                video_id=video["id"], project_id=project_id, user_id=user_id, 
                question_group_id=group["ID"], role=role, session=session
            )
        
        completed_count = sum(completion_status.values())
        total_count = len(question_groups)
        
        completion_details = [
            f"✅ {group['Display Title']}" if completion_status[group["ID"]] 
            else f"<span style='color: #A1A1A1;'>{group['Display Title']}</span>"
            for group in question_groups
        ]
        
        # Progress display format
        st.markdown(f"""
        <div style="{get_card_style('#B180FF')}text-align: center;">
            <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                {video['uid']} - {' | '.join(completion_details)} - Progress: {completed_count}/{total_count} Complete
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Two columns layout
        video_col, answer_col = st.columns([1, 1])
        
        with video_col:
            autoplay = st.session_state.get(f"{role}_autoplay", True)
            loop = st.session_state.get(f"{role}_loop", True)
            video_height = custom_video_player(video["url"], autoplay=autoplay, loop=loop, show_share_button=True)
        
        with answer_col:
            tab_names = [group['Display Title'] for group in question_groups]
            tabs = st.tabs(tab_names)
            
            for tab, group in zip(tabs, question_groups):
                with tab:
                    display_question_group_in_fixed_container(
                        video=video, project_id=project_id, user_id=user_id, 
                        group_id=group["ID"], role=role, mode=mode, 
                        session=session, container_height=video_height
                    )
                    
    except ValueError as e:
        st.error(f"Error loading project data: {str(e)}")
        if st.button("🔄 Refresh Page", key=f"refresh_{video['id']}_{project_id}"):
            st.rerun()


def display_question_group_in_fixed_container(video: Dict, project_id: int, user_id: int, group_id: int, role: str, mode: str, session: Session, container_height: int):
    """Display question group content with preloaded answers support"""

    try:
        questions = get_questions_by_group_cached(group_id=group_id, session=session)
        
        if not questions:
            custom_info("No questions in this group.")
            # Create empty form to prevent missing submit button error
            with st.form(f"empty_form_{video['id']}_{group_id}_{role}"):
                custom_info("No questions available in this group.")
                st.form_submit_button("No Actions Available", disabled=True)
            return
        
        # Get preloaded answers if available - SIMPLIFIED
        preloaded_answers = st.session_state.get(f"current_preloaded_answers_{role}_{project_id}", {})
        
        # Get selected annotators for reviewer/meta-reviewer roles
        selected_annotators = None
        cache_data = None

        if role in ["reviewer", "meta_reviewer"]:
            selected_annotators = st.session_state.get("selected_annotators", [])
            if selected_annotators:
                annotator_user_ids = get_optimized_annotator_user_ids(
                    display_names=selected_annotators, project_id=project_id, session=session
                )
                if annotator_user_ids:
                    session_id = get_session_cache_key()
                    cache_data = get_cached_video_reviewer_data(
                        video_id=video["id"], project_id=project_id, 
                        annotator_user_ids=annotator_user_ids, session_id=session_id
                    )
        
        # Check admin modifications
        has_any_admin_modified_questions = False
        if role == "reviewer":
            for question in questions:
                if GroundTruthService.check_question_modified_by_admin(
                    video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                ):
                    has_any_admin_modified_questions = True
                    break
        
        display_data = _get_question_display_data(
            video["id"], project_id, user_id, group_id, role, mode, session,
            has_any_admin_modified_questions
        )
        
        if display_data["error"]:
            custom_info(display_data["error"])
            # Create empty form to prevent missing submit button error
            with st.form(f"error_form_{video['id']}_{group_id}_{role}"):
                st.error(display_data["error"])
                st.form_submit_button("Unable to Load", disabled=True)
            return
        
        questions = display_data["questions"]
        all_questions_modified_by_admin = display_data["all_questions_modified_by_admin"]
        existing_answers = display_data["existing_answers"]
        form_disabled = display_data["form_disabled"]
        
        # Get ground truth for training mode
        gt_answers = {}
        if mode == "Training" and role == "annotator":
            try:
                gt_df = GroundTruthService.get_ground_truth_for_question_group(video_id=video["id"], project_id=project_id, question_group_id=group_id, session=session)
                if not gt_df.empty:
                    question_map = {q["id"]: q for q in questions}
                    for _, gt_row in gt_df.iterrows():
                        question_id = gt_row["Question ID"]
                        if question_id in question_map:
                            question_text = question_map[question_id]["text"]
                            gt_answers[question_text] = gt_row["Answer Value"]
            except Exception as e:
                print(f"Error getting ground truth: {e}")
                pass
        
        # Check if we have editable questions
        has_any_editable_questions = False
        for question in questions:
            if role == "reviewer":
                is_admin_modified = GroundTruthService.check_question_modified_by_admin(
                    video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                )
                if not is_admin_modified:
                    has_any_editable_questions = True
                    break
            elif role == "meta_reviewer":
                has_any_editable_questions = True
                break
            else:
                has_any_editable_questions = True
                break
        
        is_group_complete = check_question_group_completion(
            video_id=video["id"], project_id=project_id, user_id=user_id, 
            question_group_id=group_id, role=role, session=session
        )
        
        ground_truth_exists = False
        if role == "meta_reviewer":
            ground_truth_exists = check_ground_truth_exists_for_group(
                video_id=video["id"], project_id=project_id, question_group_id=group_id, session=session
            )
        
        button_text, button_disabled = _get_submit_button_config(
            role, form_disabled, all_questions_modified_by_admin, 
            has_any_editable_questions, is_group_complete, mode, ground_truth_exists,
            has_any_admin_modified_questions
        )
        
        # Initialize answer review states
        answer_reviews = {}
        if role in ["reviewer", "meta_reviewer"]:
            for question in questions:
                if question["type"] == "description":
                    question_text = question["text"]
                    existing_review_data = load_existing_answer_reviews(
                        video_id=video["id"], project_id=project_id, 
                        question_id=question["id"], session=session,
                        cache_data=cache_data
                    )
                    answer_reviews[question_text] = existing_review_data
        
        # Create form - ensure it always has questions and a submit button
        form_key = f"form_{video['id']}_{group_id}_{role}"
        with st.form(form_key):
            answers = {}
            
            content_height = max(350, container_height - 121)
            
            # Ensure we always display content even if questions fail to load
            try:
                with st.container(height=content_height, border=False):
                    for i, question in enumerate(questions):
                        question_id = question["id"]
                        question_text = question["text"]
                        existing_value = existing_answers.get(question_text, "")
                        gt_value = gt_answers.get(question_text, "")
                        
                        is_modified_by_admin = False
                        admin_info = None
                        if role in ["reviewer", "meta_reviewer"]:
                            is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                            )
                            if is_modified_by_admin:
                                admin_info = GroundTruthService.get_admin_modification_details(
                                    video_id=video["id"], project_id=project_id, question_id=question_id, session=session
                                )
                        
                        if i > 0:
                            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
                        
                        # FIXED: Pass group_id directly to avoid lookup issues
                        if question["type"] == "single":
                            answers[question_text] = display_single_choice_question(
                                question=question,
                                video_id=video["id"],
                                project_id=project_id,
                                group_id=group_id,  # ← PASS GROUP_ID DIRECTLY
                                role=role,
                                existing_value=existing_value,
                                is_modified_by_admin=is_modified_by_admin,
                                admin_info=admin_info,
                                form_disabled=form_disabled,
                                session=session,
                                gt_value=gt_value,
                                mode=mode,
                                selected_annotators=selected_annotators,
                                preloaded_answers=preloaded_answers,
                                cache_data=cache_data
                            )
                        else:
                            answers[question_text] = display_description_question(
                                question=question,
                                video_id=video["id"],
                                project_id=project_id,
                                group_id=group_id,  # ← PASS GROUP_ID DIRECTLY
                                role=role,
                                existing_value=existing_value,
                                is_modified_by_admin=is_modified_by_admin,
                                admin_info=admin_info,
                                form_disabled=form_disabled,
                                session=session,
                                gt_value=gt_value,
                                mode=mode,
                                answer_reviews=answer_reviews,
                                selected_annotators=selected_annotators,
                                preloaded_answers=preloaded_answers,
                                cache_data=cache_data
                            )
            except Exception as e:
                st.error(f"Error displaying questions: {str(e)}")
                # Still provide empty answers dict for form submission
                answers = {}
            
            # st.markdown('<div style="margin: 0px 0;"></div>', unsafe_allow_html=True)
            
            # ALWAYS include a submit button
            submitted = st.form_submit_button(button_text, use_container_width=True, disabled=button_disabled)
            
            # Handle form submission
            if submitted and not button_disabled:
                # Clear preloaded answers after successful submission
                if f"current_preloaded_answers_{role}_{project_id}" in st.session_state:
                    del st.session_state[f"current_preloaded_answers_{role}_{project_id}"]
                
                try:
                    if role == "annotator":
                        AnnotatorService.submit_answer_to_question_group(
                            video_id=video["id"], project_id=project_id, user_id=user_id,
                            question_group_id=group_id, answers=answers, session=session
                        )
                        
                        try:
                            overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                            if overall_progress >= 100 and not is_group_complete:
                                show_annotator_completion()
                                return
                        except Exception as e:
                            print(f"Error calculating user overall progress: {e}")
                            pass
                        
                        if mode == "Training":
                            show_training_feedback(video_id=video["id"], project_id=project_id, group_id=group_id, user_answers=answers, session=session)
                        else:
                            st.success("✅ Answers submitted!")
                    
                    elif role == "meta_reviewer":
                        try:
                            GroundTruthService.override_ground_truth_to_question_group(
                                video_id=video["id"], project_id=project_id, question_group_id=group_id,
                                admin_id=user_id, answers=answers, session=session
                            )
                            
                            if answer_reviews:
                                submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
                            st.success("✅ Ground truth overridden!")
                            
                        except ValueError as e:
                            st.error(f"Error overriding ground truth: {str(e)}")
                    
                    else:  # reviewer
                        if has_any_admin_modified_questions:
                            st.warning("Cannot submit: Some questions have been overridden by admin.")
                            return
                        
                        editable_answers = {
                            question["text"]: answers[question["text"]]
                            for question in questions
                            if not GroundTruthService.check_question_modified_by_admin(
                                video_id=video["id"], project_id=project_id, question_id=question["id"], session=session
                            )
                        }
                        
                        if editable_answers:
                            GroundTruthService.submit_ground_truth_to_question_group(
                                video_id=video["id"], project_id=project_id, reviewer_id=user_id,
                                question_group_id=group_id, answers=editable_answers, session=session
                            )
                            
                            if answer_reviews:
                                submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
                            try:
                                project_progress = ProjectService.progress(project_id=project_id, session=session)
                                if project_progress['completion_percentage'] >= 100 and not is_group_complete:
                                    show_reviewer_completion()
                                    return
                            except Exception as e:
                                print(f"Error showing reviewer completion: {e}")
                                pass
                            
                            st.success("✅ Ground truth submitted!")
                        else:
                            st.warning("No editable questions to submit.")
                    
                    st.rerun(scope="fragment")
                    
                except ValueError as e:
                    st.error(f"Error: {str(e)}")
                    
    except ValueError as e:
        st.error(f"Error loading question group: {str(e)}")
        # Create fallback form to prevent missing submit button error
        with st.form(f"fallback_form_{video['id']}_{group_id}_{role}"):
            st.error("Failed to load question group properly")
            st.form_submit_button("Unable to Load Questions", disabled=True)


###############################################################################
# QUESTION DISPLAY FUNCTIONS
###############################################################################

def display_single_choice_question(
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
    preloaded_answers: Dict = None,
    cache_data: Dict = None
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
            # st.markdown(f"""
            #     <div style="{get_card_style(COLORS['success'])}">
            #         <span style="color: #1e8449; font-weight: 600; font-size: 0.95rem;">
            #             ✅ Excellent! You selected the correct answer.
            #         </span>
            #     </div>
            # """, unsafe_allow_html=True)
            st.success(f"✅ Excellent! You selected the correct answer.")
        else:
            # st.markdown(f"""
            #     <div style="{get_card_style(COLORS['danger'])}">
            #         <span style="color: #c0392b; font-weight: 600; font-size: 0.95rem;">
            #             ❌ Incorrect. The ground truth answer is highlighted below.
            #         </span>
            #     </div>
            # """, unsafe_allow_html=True)
            st.error(f"❌ Incorrect. The ground truth answer is highlighted below.")
    
    # Show unified status for reviewers/meta-reviewers - OPTIMIZED
    if role in ["reviewer", "meta_reviewer", "reviewer_resubmit"]:
        show_annotators = selected_annotators is not None and len(selected_annotators) > 0
        display_question_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or [],
            cache_data=cache_data
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

def display_description_question(
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
    preloaded_answers: Dict = None,
    cache_data: Dict = None
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
        display_question_status(
            video_id=video_id, 
            project_id=project_id, 
            question_id=question_id, 
            session=session,
            show_annotators=show_annotators,
            selected_annotators=selected_annotators or [],
            cache_data=cache_data
        )
        
        # Show enhanced helper text if annotators selected OR if we have ground truth (for search portal)
        if show_annotators or (role in ["meta_reviewer", "reviewer_resubmit"] and existing_value):
            _display_enhanced_helper_text_answers(
                video_id=video_id, project_id=project_id, question_id=question_id, 
                question_text=question_key, text_key=text_key,
                gt_value=existing_value if role in ["meta_reviewer", "reviewer_resubmit"] else "",
                role=role, answer_reviews=answer_reviews, session=session,
                selected_annotators=selected_annotators or [],
                cache_data=cache_data
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

###############################################################################
# DISPLAY HELPER FUNCTIONS
###############################################################################

def show_training_feedback(video_id: int, project_id: int, group_id: int, user_answers: Dict[str, str], session: Session):
    """Show training feedback comparing user answers to ground truth"""
    try:
        gt_df = GroundTruthService.get_ground_truth_for_question_group(video_id=video_id, project_id=project_id, question_group_id=group_id, session=session)
        questions = get_questions_by_group_cached(group_id=group_id, session=session)
        question_map = {q["id"]: q for q in questions}
        
        st.subheader("📊 Training Feedback")
        
        for _, gt_row in gt_df.iterrows():
            question_id = gt_row["Question ID"]
            if question_id not in question_map:
                continue
                
            question = question_map[question_id]
            question_text = question["text"]
            gt_answer = gt_row["Answer Value"]
            user_answer = user_answers.get(question_text, "")
            
            is_correct = user_answer == gt_answer
            
            if is_correct:
                st.success(f"✅ **{question_text}**: Correct!")
            else:
                st.error(f"❌ **{question_text}**: Incorrect")
                st.write(f"Your answer: {user_answer}")
                st.write(f"Correct answer: {gt_answer}")
                
    except ValueError as e:
        st.error(f"Error showing training feedback: {str(e)}")


def display_question_status(
    video_id: int, project_id: int, question_id: int, 
    session: Session, show_annotators: bool = False, 
    selected_annotators: List[str] = None, cache_data: Dict = None
):
    """OPTIMIZED: Display status using cached data"""
    
    status_parts = []
    
    # Get annotator status using cache
    if show_annotators and selected_annotators and cache_data:
        try:
            annotator_user_ids = get_optimized_annotator_user_ids(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            
            if annotator_user_ids:
                question_answers = cache_data["annotator_answers"].get(question_id, [])
                answered_user_ids = set(record["User ID"] for record in question_answers)
                
                annotators_with_answers = []
                annotators_missing = []
                
                for user_id in annotator_user_ids:
                    user_info = cache_data["user_info"].get(user_id, {})
                    user_name = user_info.get("name", f"User {user_id}")
                    
                    if user_id in answered_user_ids:
                        annotators_with_answers.append(user_name)
                    else:
                        annotators_missing.append(user_name)
                
                # Format status parts
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
            print(f"Error in annotator status: {e}")
            status_parts.append(f"⚠️ Status error")
    
    # Ground truth status (always fresh)
    try:
        # gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        gt_row = GroundTruthService.get_ground_truth_for_question(video_id=video_id, project_id=project_id, question_id=question_id, session=session)
        
        # if not gt_df.empty:
        #     question_gt = gt_df[gt_df["Question ID"] == question_id]
            
            # if not question_gt.empty:
            #     gt_row = question_gt.iloc[0]
        if gt_row:
            try:
                reviewer_info = AuthService.get_user_info_by_id(
                    user_id=int(gt_row["Reviewer ID"]), session=session
                )
                reviewer_name = reviewer_info["user_id_str"]
                
                modified_by_admin = gt_row["Modified By Admin"] is not None
                
                if modified_by_admin:
                    admin_info = AuthService.get_user_info_by_id(
                        user_id=int(gt_row["Modified By Admin"]), session=session
                    )
                    admin_name = admin_info["user_id_str"]
                    status_parts.append(f"🏆 GT by: {reviewer_name} (Overridden by {admin_name})")
                else:
                    status_parts.append(f"🏆 GT by: {reviewer_name}")
            except Exception:
                status_parts.append("🏆 GT exists")
            # else:
            #     status_parts.append("📭 No GT")
        else:
            status_parts.append("📭 No GT")
            
    except Exception as e:
        print(f"Error getting ground truth status: {e}")
        status_parts.append("📭 GT error")
    
    # Display combined status
    if status_parts:
        st.caption(" | ".join(status_parts))



def _get_enhanced_options_for_reviewer(
    video_id: int, project_id: int, question_id: int, 
    options: List[str], display_values: List[str], 
    session: Session, selected_annotators: List[str] = None
) -> List[str]:
    """OPTIMIZED: Get enhanced options using cached video data"""
    
    try:
        # Get annotator user IDs
        annotator_user_ids = []
        if selected_annotators:
            annotator_user_ids = get_optimized_annotator_user_ids(
                display_names=selected_annotators, project_id=project_id, session=session
            )
        
        if not annotator_user_ids:
            # Still check for ground truth even without annotators
            enhanced_options = display_values.copy()
            try:
                # gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                gt_row = GroundTruthService.get_ground_truth_for_question(video_id=video_id, project_id=project_id, question_id=question_id, session=session)
                if gt_row:
                    # question_gt = gt_df[gt_df["Question ID"] == question_id]
                    # if not question_gt.empty:
                    gt_selection = gt_row["Answer Value"]
                    for i, option in enumerate(options):
                        if str(option) == str(gt_selection):
                            enhanced_options[i] += " — 🏆 GT"
            except:
                pass
            return enhanced_options
        
        # Use cached data
        session_id = get_session_cache_key()
        cache_data = get_cached_video_reviewer_data(
            video_id=video_id, project_id=project_id, 
            annotator_user_ids=annotator_user_ids, session_id=session_id
        )
        
        # Build option selections from cache
        option_selections = {}
        question_answers = cache_data["annotator_answers"].get(question_id, [])
        
        for answer_record in question_answers:
            answer_value = answer_record["Answer Value"]
            user_id = answer_record["User ID"]
            
            if answer_value not in option_selections:
                option_selections[answer_value] = []
            
            user_info = cache_data["user_info"].get(user_id, {})
            user_name = user_info.get("name", "Unknown User")
            user_role = user_info.get("role", "human")
            
            _, initials = AuthService.get_user_display_name_with_initials(user_name)
            
            # Add confidence score for models
            confidence_text = initials
            if user_role == "model" and user_id in cache_data["confidence_scores"]:
                confidence = cache_data["confidence_scores"][user_id].get(question_id)
                if confidence is not None:
                    confidence_text = f"{initials} ({confidence:.2f})"
            
            option_selections[answer_value].append({
                "name": user_name,
                "initials": confidence_text,
                "type": "annotator"
            })
        
        # Check ground truth
        gt_selection = None
        # try:
        #     gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        #     if not gt_df.empty:
        #         question_gt = gt_df[gt_df["Question ID"] == question_id]
        #         if not question_gt.empty:
        #             gt_selection = question_gt.iloc[0]["Answer Value"]
        # except:
        #     pass
        try:
            gt_df = GroundTruthService.get_ground_truth_for_question(video_id=video_id, project_id=project_id, question_id=question_id, session=session)
            if gt_df:
                gt_selection = gt_df["Answer Value"]
        except:
            pass
        
        # Build enhanced options
        total_annotators = len(annotator_user_ids)
        enhanced_options = []
        
        for i, display_val in enumerate(display_values):
            actual_val = options[i] if i < len(options) else display_val
            option_text = display_val
            
            selection_info = []
            
            # Add annotator info
            if actual_val in option_selections:
                annotators = [sel["initials"] for sel in option_selections[actual_val]]
                count = len(annotators)
                
                if total_annotators > 0:
                    percentage = (count / total_annotators) * 100
                    percentage_str = f"{int(percentage)}%" if percentage == int(percentage) else f"{percentage:.1f}%"
                    selection_info.append(f"{percentage_str}: {', '.join(annotators)}")
                else:
                    selection_info.append(f"{', '.join(annotators)}")
            
            # Add ground truth indicator
            if gt_selection and str(actual_val) == str(gt_selection):
                selection_info.append("🏆 GT")
            
            if selection_info:
                option_text += f" — {' | '.join(selection_info)}"
            
            enhanced_options.append(option_text)
        
        return enhanced_options
        
    except Exception as e:
        print(f"Error in _get_enhanced_options_for_reviewer: {e}")
        # Fallback to original
        return display_values


def _display_enhanced_helper_text_answers(
    video_id: int, project_id: int, question_id: int, 
    question_text: str, text_key: str, gt_value: str, 
    role: str, answer_reviews: Optional[Dict], 
    session: Session, selected_annotators: List[str] = None,
    cache_data: Dict = None
):
    """OPTIMIZED: Display helper text using cached data"""
    
    try:
        all_answers = []
        
        # Add Ground Truth for meta-reviewer or if exists
        if role == "meta_reviewer" and gt_value:
            all_answers.append({
                "name": "Ground Truth",
                "full_text": gt_value,
                "has_answer": bool(gt_value.strip()),
                "is_gt": True,
                "display_name": "Ground Truth"
            })
        elif not selected_annotators or len(selected_annotators) == 0:
            try:
                # gt_df = GroundTruthService.get_ground_truth(
                #     video_id=video_id, project_id=project_id, session=session
                # )
                gt_row = GroundTruthService.get_ground_truth_for_question(
                    video_id=video_id, project_id=project_id, question_id=question_id, session=session
                )
                if gt_row:
                    # question_gt = gt_df[gt_df["Question ID"] == question_id]
                    # if not question_gt.empty:
                    gt_answer = gt_row["Answer Value"]
                    if gt_answer and str(gt_answer).strip():
                        all_answers.append({
                            "name": "Ground Truth",
                            "full_text": str(gt_answer),
                            "has_answer": True,
                            "is_gt": True,
                            "display_name": "Ground Truth"
                        })
            except:
                pass
        
        # Add annotator answers from cache
        if selected_annotators and cache_data:
            text_answers = cache_data["text_answers"].get(question_id, [])
            
            if text_answers:
                unique_answers = []
                seen_answers = set()
                
                for answer in text_answers:
                    answer_key = f"{answer['initials']}:{answer['answer_value']}"
                    if answer_key not in seen_answers:
                        seen_answers.add(answer_key)
                        unique_answers.append(answer)
                
                for answer_info in unique_answers:
                    all_answers.append({
                        "name": answer_info['name'],
                        "full_text": answer_info['answer_value'],
                        "has_answer": bool(answer_info['answer_value'].strip()),
                        "is_gt": False,
                        "display_name": f"{answer_info['name']} ({answer_info['initials']})",
                        "initials": answer_info['initials']
                    })
        
        if all_answers:
            # Display logic remains the same
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


def submit_answer_reviews(answer_reviews: Dict, video_id: int, project_id: int, user_id: int, session: Session):
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

def load_existing_answer_reviews(video_id: int, project_id: int, question_id: int, session: Session, cache_data: Dict = None) -> Dict[str, Dict]:
    """OPTIMIZED: Load existing answer reviews for a description question from the database"""
    reviews = {}
    
    try:
        selected_annotators = st.session_state.get("selected_annotators", [])
        if cache_data and selected_annotators:
            # Use cached user info instead of querying
            annotator_user_ids = []
            for annotator_name in selected_annotators:
                for user_id, user_info in cache_data.get("user_info", {}).items():
                    if user_info.get("name") == annotator_name:
                        annotator_user_ids.append(user_id)
                        break
        else:
            # Fallback to original method
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


###############################################################################
# TAB DISPLAY FUNCTIONS
###############################################################################

@st.fragment
def display_enhanced_sort_tab(project_id: int, session: Session):
    """Enhanced sort tab with improved UI/UX and proper validation"""
    st.markdown("#### 🔄 Video Sorting Options")
    
    # Revert to original style to match other tabs
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}text-align: center;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            📊 Sort videos by different criteria to optimize your review workflow
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Main configuration in a clean layout
    config_col1, config_col2 = st.columns([2, 1])
    
    with config_col1:
        sort_options = ["Default", "Model Confidence", "Annotator Consensus", "Completion Rate", "Accuracy Rate"]
        sort_by = st.selectbox(
            "Sort method",
            sort_options,
            key=f"video_sort_by_{project_id}",
            help="Choose sorting criteria"
        )
    
    with config_col2:
        sort_order = st.selectbox(
            "Order",
            ["Ascending", "Descending"],  # Default to Ascending first
            key=f"video_sort_order_{project_id}",
            help="Sort direction"
        )
    
    # Configuration and validation
    sort_config = {}
    config_valid = True
    config_messages = []
    
    if sort_by != "Default":
        st.markdown("**Configuration:**")
        
        if sort_by == "Model Confidence":
            # Model users selection
            try:
                users_df = AuthService.get_all_users(session=session)
                assignments_df = AuthService.get_project_assignments(session=session)
                project_assignments = assignments_df[assignments_df["Project ID"] == project_id]
                
                model_users = []
                for _, assignment in project_assignments.iterrows():
                    if assignment["Role"] == "model":
                        user_id = assignment["User ID"]
                        user_info = users_df[users_df["ID"] == user_id]
                        if not user_info.empty:
                            model_users.append({
                                "id": user_id,
                                "name": user_info.iloc[0]["User ID"]
                            })
                
                if not model_users:
                    config_messages.append(("error", "No model users found in this project."))
                    config_valid = False
                else:
                    selected_models = st.multiselect(
                        "Model users:",
                        [f"{user['name']} (ID: {user['id']})" for user in model_users],
                        key=f"model_users_{project_id}"
                    )
                    sort_config["model_user_ids"] = [
                        user["id"] for user in model_users 
                        if f"{user['name']} (ID: {user['id']})" in selected_models
                    ]
                    
                    if not sort_config["model_user_ids"]:
                        config_messages.append(("warning", "Select at least one model user."))
                        config_valid = False
            except Exception as e:
                config_messages.append(("error", f"Error loading model users: {str(e)}"))
                config_valid = False
            
            # Questions selection for model confidence
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"confidence_questions_{project_id}",
                    help="Select questions with confidence scores"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Annotator Consensus":
            # Check annotator selection first
            selected_annotators = st.session_state.get("selected_annotators", [])
            if len(selected_annotators) < 2:
                if len(selected_annotators) == 1:
                    config_messages.append(("error", "Consensus requires at least 2 annotators. Only 1 annotator selected."))
                else:
                    config_messages.append(("error", "No annotators selected. Go to Annotators tab first."))
                config_valid = False
            
            # Questions selection
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"consensus_questions_{project_id}",
                    help="Select questions for consensus calculation"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Completion Rate":
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"completion_questions_{project_id}",
                    help="Select questions for completion tracking"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
        
        elif sort_by == "Accuracy Rate":
            # Check annotator selection
            selected_annotators = st.session_state.get("selected_annotators", [])
            if not selected_annotators:
                config_messages.append(("error", "No annotators selected. Go to Annotators tab first."))
                config_valid = False
            
            # Questions selection
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"accuracy_questions_{project_id}",
                    help="Select questions for accuracy comparison"
                )
                sort_config["question_ids"] = [
                    q["id"] for q in single_choice_questions 
                    if f"{q['text']} (ID: {q['id']})" in selected_questions
                ]
                
                if not sort_config["question_ids"]:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
    
    # Show configuration messages compactly
    if config_messages:
        for msg_type, msg in config_messages:
            if msg_type == "error":
                st.error(msg)
            elif msg_type == "warning":
                st.warning(msg)
            else:
                custom_info(msg)
    
    # Action buttons in a compact row
    action_col1, action_col2 = st.columns([1, 1])
    
    with action_col1:
        if st.button("🔄 Apply", 
                    key=f"apply_sort_{project_id}", 
                    disabled=not config_valid,
                    use_container_width=True,
                    type="primary"):
            st.session_state[f"sort_config_{project_id}"] = sort_config
            st.session_state[f"sort_applied_{project_id}"] = True
            st.success("✅ Applied!")
            st.rerun()
    
    with action_col2:
        if st.button("🔄 Reset", 
                    key=f"reset_sort_{project_id}",
                    use_container_width=True):
            st.session_state[f"video_sort_by_{project_id}"] = "Default"
            st.session_state[f"sort_config_{project_id}"] = {}
            st.session_state[f"sort_applied_{project_id}"] = False
            st.success("✅ Reset!")
            st.rerun()
    
    # with action_col3:
    # Status indicator
    current_sort = st.session_state.get(f"video_sort_by_{project_id}", "Default")
    sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
    
    if current_sort != "Default" and sort_applied:
        custom_info("Status: ✅ Active")
    elif current_sort != "Default":
        custom_info("Status: ⏳ Ready")
    else:
        custom_info("Status: Default")
    
    # st.markdown(f"""
    # <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid {COLORS['primary']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
    #     💡 <strong>Tip:</strong> Configure your sorting options above, then click "Apply Sorting" to sort the videos accordingly.
    # </div>
    # """, unsafe_allow_html=True)
    custom_info("💡 Configure your sorting options above, then click <strong>Apply</strong> to sort the videos accordingly.")

@st.fragment
def display_enhanced_sort_tab_annotator(project_id: int, session: Session):
    """Enhanced sort tab for annotators - only relevant options"""
    st.markdown("#### 🔄 Video Sorting Options")
    
    # Check if this is training mode
    is_training_mode = check_project_has_full_ground_truth(project_id=project_id, session=session)
    
    if not is_training_mode:
        st.markdown(f"""
        <div style="{get_card_style('#B180FF')}text-align: center;">
            <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                📝 Annotation Mode - Only default sorting available.
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Only show default sorting in annotation mode
        sort_options = ["Default"]
    else:
        st.markdown(f"""
        <div style="{get_card_style('#B180FF')}text-align: center;">
            <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                🎓 Training Mode - Sort videos by your completion status or accuracy
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        sort_options = ["Default", "Completion Rate", "Accuracy Rate"]
    
    # Main configuration
    config_col1, config_col2 = st.columns([2, 1])
    
    with config_col1:
        sort_by = st.selectbox(
            "Sort method",
            sort_options,
            key=f"annotator_video_sort_by_{project_id}",
            help="Choose sorting criteria"
        )
    
    with config_col2:
        sort_order = st.selectbox(
            "Order",
            ["Ascending", "Descending"],
            key=f"annotator_video_sort_order_{project_id}",
            help="Sort direction"
        )
    
    config_valid = True
    config_messages = []
    
    # Only show configuration for training mode sorts
    if sort_by != "Default" and is_training_mode:
        st.markdown("**Configuration:**")
        
        if sort_by in ["Completion Rate", "Accuracy Rate"]:
            questions = ProjectService.get_project_questions(project_id=project_id, session=session)
            single_choice_questions = [q for q in questions if q["type"] == "single"]
            
            if not single_choice_questions:
                config_messages.append(("error", "No single-choice questions available."))
                config_valid = False
            else:
                selected_questions = st.multiselect(
                    "Questions:",
                    [f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    default=[f"{q['text']} (ID: {q['id']})" for q in single_choice_questions],
                    key=f"annotator_{sort_by.lower().replace(' ', '_')}_questions_{project_id}",
                    help=f"Select questions for {sort_by.lower()} calculation"
                )
                
                if not selected_questions:
                    config_messages.append(("warning", "Select at least one question."))
                    config_valid = False
    
    # Show configuration messages
    if config_messages:
        for msg_type, msg in config_messages:
            if msg_type == "error":
                st.error(msg)
            elif msg_type == "warning":
                st.warning(msg)
            else:
                custom_info(msg)
    
    # Action buttons
    action_col1, action_col2 = st.columns([1, 1])
    
    with action_col1:
        if st.button("🔄 Apply", 
                    key=f"apply_annotator_sort_{project_id}", 
                    disabled=not config_valid,
                    use_container_width=True,
                    type="primary"):
            # Store sort configuration for annotators
            st.session_state[f"annotator_sort_applied_{project_id}"] = True
            st.success("✅ Applied!")
            st.rerun()
    
    with action_col2:
        if st.button("🔄 Reset", 
                    key=f"reset_annotator_sort_{project_id}",
                    use_container_width=True):
            st.session_state[f"annotator_video_sort_by_{project_id}"] = "Default"
            st.session_state[f"annotator_sort_applied_{project_id}"] = False
            st.success("✅ Reset!")
            st.rerun()
    
    # Status indicator
    current_sort = st.session_state.get(f"annotator_video_sort_by_{project_id}", "Default")
    sort_applied = st.session_state.get(f"annotator_sort_applied_{project_id}", False)
    
    if current_sort != "Default" and sort_applied:
        custom_info("Status: ✅ Active")
    elif current_sort != "Default":
        custom_info("Status: ⏳ Ready")
    else:
        custom_info("Status: Default")
    
    custom_info("💡 Configure your sorting options above, then click <strong>Apply</strong> to sort the videos accordingly.")

@st.fragment
def display_enhanced_filter_tab(project_id: int, session: Session):
    """Enhanced filter tab with proper ground truth detection and full question text"""
    st.markdown("#### 🔍 Video Filtering Options")
    
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}text-align: center;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            🎯 Filter videos by specific ground truth answers to focus your review
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Get available ground truth options using enhanced function
    gt_options = get_ground_truth_option_filters_enhanced(project_id=project_id, session=session)
    
    if gt_options:
        st.markdown("**Filter by Ground Truth Answers:**")
        
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        # Get current filters to show current state
        current_filters = st.session_state.get(f"video_filters_{project_id}", {})
        
        # Collect new filter selections (don't store in session state yet)
        new_filters = {}
        
        for question_id, available_answers in gt_options.items():
            question_text = question_lookup.get(question_id, f"Question {question_id}")
            display_question = question_text
            
            # Get current selection for this question
            current_selection = current_filters.get(question_id, "Any")
            
            # Make sure current selection is still valid
            if current_selection not in ["Any"] + available_answers:
                current_selection = "Any"
            
            filter_key = f"video_filter_q_{question_id}_{project_id}"
            selected_answer = st.selectbox(
                f"**{display_question}**",
                ["Any"] + sorted(available_answers),
                index=(["Any"] + sorted(available_answers)).index(current_selection) if current_selection in ["Any"] + sorted(available_answers) else 0,
                key=filter_key,
                help=f"Filter videos where this question has the selected ground truth answer"
            )
            
            if selected_answer != "Any":
                new_filters[question_id] = selected_answer
        
        # Show what filters would be applied
        if new_filters:
            filter_summary = []
            for q_id, answer in new_filters.items():
                q_text = question_lookup.get(q_id, f"Q{q_id}")
                display_text = q_text[:80] + "..." if len(q_text) > 80 else q_text
                filter_summary.append(f"{display_text} = {answer}")
            
            custom_info(f"🔍 **Ready to apply:** {' | '.join(filter_summary)}")
        else:
            custom_info("ℹ️ No filters selected - will show all videos")
        
        # Check if filters have changed
        filters_changed = new_filters != current_filters
        
        # Apply button
        apply_col1, apply_col2 = st.columns([1, 1])
        
        with apply_col1:
            if st.button("🔍 Apply Filters", 
                        type="primary", 
                        use_container_width=True,
                        disabled=not filters_changed,
                        help="Apply the selected filters to videos"):
                st.session_state[f"video_filters_{project_id}"] = new_filters
                st.rerun()  # This triggers a rerun of the parent page
        
        with apply_col2:
            if st.button("🗑️ Clear All Filters", 
                        use_container_width=True,
                        disabled=not current_filters,
                        help="Remove all active filters"):
                st.session_state[f"video_filters_{project_id}"] = {}
                st.rerun()
        
        # Show current active filters if any
        if current_filters:
            st.markdown("**Currently Active Filters:**")
            active_summary = []
            for q_id, answer in current_filters.items():
                q_text = question_lookup.get(q_id, f"Q{q_id}")
                display_text = q_text[:60] + "..." if len(q_text) > 60 else q_text
                active_summary.append(f"• {display_text} = **{answer}**")
            
            st.markdown("\n".join(active_summary))
            
            if filters_changed:
                st.warning("⚠️ **Filters have been modified** - click 'Apply Filters' to update the video list")
            else:
                st.success("✅ **Filters are active** - video list is filtered")
        
    else:
        custom_info("No ground truth data available for filtering yet. Complete ground truth annotation to enable filtering.")
        # Still ensure the session state key exists
        if f"video_filters_{project_id}" not in st.session_state:
            st.session_state[f"video_filters_{project_id}"] = {}
    
    custom_info("💡 Filters only work on questions that have ground truth answers. Complete annotation first to see more filter options.")


def display_order_tab(project_id: int, role: str, project: Dict, session: Session):
    """Display question group order tab - shared between reviewer and meta-reviewer"""
    st.markdown("#### 📋 Question Group Display Order")
    
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}text-align: center;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            🔄 Customize the order of question groups for this session
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Get question groups for this project
    question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
    
    if question_groups:
        order_key = f"question_order_{project_id}_{role}"
        if order_key not in st.session_state:
            st.session_state[order_key] = [group["ID"] for group in question_groups]
        
        working_order = st.session_state[order_key]
        group_lookup = {group["ID"]: group for group in question_groups}
        
        custom_info("💡 Use the ⬆️ and ⬇️ buttons to reorder question groups. This only affects your current session.")
        
        for i, group_id in enumerate(working_order):
            if group_id in group_lookup:
                group = group_lookup[group_id]
                group_display_title = group["Display Title"]
                
                order_col1, order_col2, order_col3 = st.columns([0.1, 0.8, 0.1])
                
                with order_col1:
                    if st.button("⬆️", key=f"group_up_{project_id}_{group_id}_{i}", 
                                disabled=(i == 0), help="Move up"):
                        st.session_state[order_key][i], st.session_state[order_key][i-1] = \
                            st.session_state[order_key][i-1], st.session_state[order_key][i]
                        st.rerun()
                
                with order_col2:
                    st.write(f"**{i+1}.** {group_display_title}")
                    st.caption(f"Group ID: {group_id}")
                
                with order_col3:
                    if st.button("⬇️", key=f"group_down_{project_id}_{group_id}_{i}", 
                                disabled=(i == len(working_order) - 1), help="Move down"):
                        st.session_state[order_key][i], st.session_state[order_key][i+1] = \
                            st.session_state[order_key][i+1], st.session_state[order_key][i]
                        st.rerun()
        
        # order_action_col1, order_action_col2 = st.columns(2)
        # with order_action_col1:
        if st.button("🔄 Reset to Default", key=f"reset_group_order_{project_id}"):
            st.session_state[order_key] = [group["ID"] for group in question_groups]
            st.rerun()
    
        # with order_action_col2:
        original_order = [group["ID"] for group in question_groups]
        if working_order != original_order:
            custom_info("⚠️ Order changed from default")
        else:
            custom_info("✅ Default order")
    else:
        custom_info("No question groups found for this project.")
    
    custom_info("💡 Reorder groups to match your preferred workflow. Changes only apply to your current session.")


@st.fragment
def _display_video_layout_controls(videos: List[Dict], role: str):
    """Display video layout controls"""
    
    # Get current settings from session state
    current_pairs_per_row = st.session_state.get(f"{role}_pairs_per_row", 1)
    current_autoplay = st.session_state.get(f"{role}_autoplay", True)
    current_loop = st.session_state.get(f"{role}_loop", True)
    
    # Calculate current videos per page settings
    min_videos_per_page = current_pairs_per_row
    max_videos_per_page = max(min(20, len(videos)), min_videos_per_page + 1)
    default_videos_per_page = min(min(4, len(videos)), max_videos_per_page)
    current_per_page = st.session_state.get(f"{role}_per_page", default_videos_per_page)
    
    # Collect new settings (don't store in session state yet)
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🔄 Video Pairs Per Row**")
        new_pairs_per_row = st.slider(
            "Choose layout", 
            1, 2, 
            current_pairs_per_row, 
            key=f"temp_{role}_pairs_per_row",
            help="Choose how many video-answer pairs to display side by side"
        )
    
    with col2:
        st.markdown("**📄 Videos Per Page**")
        
        # Recalculate based on new pairs per row
        new_min_videos_per_page = new_pairs_per_row
        new_max_videos_per_page = max(min(20, len(videos)), new_min_videos_per_page + 1)
        new_default_videos_per_page = min(min(4, len(videos)), new_max_videos_per_page)
        
        # Adjust current per page if it's now invalid
        adjusted_per_page = max(new_min_videos_per_page, min(current_per_page, new_max_videos_per_page))
        
        if len(videos) == 1:
            st.write("**1** (only video in project)")
            new_per_page = 1
        elif new_max_videos_per_page > new_min_videos_per_page:
            new_per_page = st.slider(
                "Pagination setting", 
                new_min_videos_per_page, 
                new_max_videos_per_page, 
                adjusted_per_page,
                key=f"temp_{role}_per_page",
                help="Set how many videos to show on each page"
            )
        else:
            st.write(f"**{len(videos)}** (showing all videos)")
            new_per_page = len(videos)
    
    st.markdown("**🎬 Video Playback Settings**")
    col3, col4 = st.columns(2)
    
    with col3:
        new_autoplay = st.checkbox(
            "🚀 Auto-play videos on load",
            value=current_autoplay,
            key=f"temp_{role}_autoplay",
            help="Automatically start playing videos when they load"
        )
    
    with col4:
        new_loop = st.checkbox(
            "🔄 Loop videos",
            value=current_loop,
            key=f"temp_{role}_loop",
            help="Automatically restart videos when they finish"
        )
    
    # Check if any settings have changed
    settings_changed = (
        new_pairs_per_row != current_pairs_per_row or
        new_per_page != current_per_page or
        new_autoplay != current_autoplay or
        new_loop != current_loop
    )
    
    # Show pending changes if any
    if settings_changed:
        st.markdown("**📋 Pending Changes:**")
        changes = []
        
        if new_pairs_per_row != current_pairs_per_row:
            changes.append(f"• Pairs per row: {current_pairs_per_row} → **{new_pairs_per_row}**")
        
        if new_per_page != current_per_page:
            changes.append(f"• Videos per page: {current_per_page} → **{new_per_page}**")
        
        if new_autoplay != current_autoplay:
            autoplay_text = "enabled" if new_autoplay else "disabled"
            changes.append(f"• Auto-play: **{autoplay_text}**")
        
        if new_loop != current_loop:
            loop_text = "enabled" if new_loop else "disabled"
            changes.append(f"• Loop videos: **{loop_text}**")
        
        st.markdown("\n".join(changes))
        st.warning("⚠️ **Settings modified** - click 'Apply Layout Settings' to update the interface")
    else:
        st.success("✅ **Current layout settings** - no changes pending")
    
    # Apply and Reset buttons
    apply_col1, apply_col2 = st.columns([1, 1])
    
    with apply_col1:
        if st.button("🎛️ Apply Layout Settings", 
                    type="primary", 
                    use_container_width=True,
                    disabled=not settings_changed,
                    help="Apply the selected layout settings"):
            # Update all session state values
            st.session_state[f"{role}_pairs_per_row"] = new_pairs_per_row
            st.session_state[f"{role}_per_page"] = new_per_page
            st.session_state[f"{role}_autoplay"] = new_autoplay
            st.session_state[f"{role}_loop"] = new_loop
            st.rerun()  # Trigger parent page rerun
    
    with apply_col2:
        # Check if we can reset (any current settings differ from defaults)
        default_settings = {
            "pairs_per_row": 1,
            "per_page": min(4, len(videos)),
            "autoplay": True,
            "loop": True
        }
        
        can_reset = (
            current_pairs_per_row != default_settings["pairs_per_row"] or
            current_per_page != default_settings["per_page"] or
            current_autoplay != default_settings["autoplay"] or
            current_loop != default_settings["loop"]
        )
        
        if st.button("🔄 Reset to Defaults", 
                    use_container_width=True,
                    disabled=not can_reset,
                    help="Reset all layout settings to default values"):
            st.session_state[f"{role}_pairs_per_row"] = default_settings["pairs_per_row"]
            st.session_state[f"{role}_per_page"] = default_settings["per_page"]
            st.session_state[f"{role}_autoplay"] = default_settings["autoplay"]
            st.session_state[f"{role}_loop"] = default_settings["loop"]
            st.rerun()
    
    # Show current active settings summary
    st.markdown("**📊 Current Settings:**")
    current_summary = [
        f"• **{current_pairs_per_row}** video pair(s) per row",
        f"• **{current_per_page}** videos per page",
        f"• Auto-play: **{'enabled' if current_autoplay else 'disabled'}**",
        f"• Loop videos: **{'enabled' if current_loop else 'disabled'}**"
    ]
    
    st.markdown("\n".join(current_summary))


def display_layout_tab_content(videos: List[Dict], role: str):
    """Display layout tab content"""
    st.markdown("#### 🎛️ Video Layout Settings")
    
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}text-align: center;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander("⚙️ Layout Configuration", expanded=False):
        _display_video_layout_controls(videos, role)
        custom_info("💡 Adjust layout to optimize your workflow.")


@st.fragment
def display_auto_submit_tab(project_id: int, user_id: int, role: str, videos: List[Dict], session: Session):
    """Display auto-submit interface - different logic for annotator vs reviewer"""
    
    st.markdown("#### ⚡ Auto-Submit Controls")
    
    # Check if we're in training mode (for annotators only)
    is_training_mode = False
    if role == "annotator":
        is_training_mode = check_project_has_full_ground_truth(project_id=project_id, session=session)
    
    if role == "annotator":
        # Original annotator logic with auto-submit groups
        if is_training_mode:
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎓 Training Mode - Auto-submit is disabled during training
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    ⚡ Auto-submit using weighted majority voting with configurable thresholds
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        # Get project details
        try:
            project = ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
            question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
        except Exception as e:
            st.error(f"Error loading project details: {str(e)}")
            return
        
        if not question_groups:
            st.warning("No question groups found in this project.")
            return
        
        # Get ALL project videos for "Entire project" scope
        all_project_videos = get_project_videos(project_id=project_id, session=session)
        
        # CALCULATE CURRENT PAGE VIDEOS
        videos_per_page = st.session_state.get(f"{role}_per_page", min(4, len(videos)))
        page_key = f"{role}_current_page_{project_id}"
        current_page = st.session_state.get(page_key, 0)
        
        start_idx = current_page * videos_per_page
        end_idx = min(start_idx + videos_per_page, len(videos))
        current_page_videos = videos[start_idx:end_idx]
        
        # Separate auto-submit and manual groups
        auto_submit_groups = []
        manual_groups = []
        
        for group in question_groups:
            try:
                group_details = QuestionGroupService.get_group_details_with_verification(
                    group_id=group["ID"], session=session
                )
                if group_details.get("is_auto_submit", False):
                    auto_submit_groups.append(group)
                else:
                    manual_groups.append(group)
            except:
                manual_groups.append(group)
        
        # Scope selection
        st.markdown("### 🎯 Scope Selection")
        scope_options = ["Current page of videos", "Entire project"]
        
        default_scope_index = 1 if auto_submit_groups else 0
        
        selected_scope = st.radio(
            "Auto-submit scope:",
            scope_options,
            index=default_scope_index,
            key=f"auto_submit_scope_{role}_{project_id}",
            help="Choose whether to apply auto-submit to current page or all videos",
            disabled=is_training_mode
        )
        
        # Get videos based on scope
        if selected_scope == "Current page of videos":
            target_videos = current_page_videos
            page_info = f" (page {current_page + 1})" if len(videos) > videos_per_page else ""
            custom_info(f"📊 Target: {len(target_videos)} videos on current page{page_info}")
        else:
            target_videos = all_project_videos
            custom_info(f"📊 Target: {len(target_videos)} videos in entire project")
        
        # Show auto-submit groups status
        if auto_submit_groups:
            st.markdown("### 🤖 Automatic Processing")
            
            auto_group_names = [group["Display Title"] for group in auto_submit_groups]
            if len(auto_group_names) == 1:
                custom_info(f"Found **{auto_group_names[0]}** with auto-submit enabled")
            else:
                group_list = ", ".join(auto_group_names[:-1]) + f" and {auto_group_names[-1]}"
                custom_info(f"Found **{group_list}** with auto-submit enabled")
            
            st.success("✅ These groups automatically submit default answers when you enter the project")
        
        # Manual controls for other groups
        if manual_groups:
            st.markdown("### 🎛️ Manual Auto-Submit Controls")
            
            selected_group_names = st.multiselect(
                "Select question groups for manual auto-submit:",
                [group["Display Title"] for group in manual_groups],
                default=[group["Display Title"] for group in manual_groups],  # ADD THIS LINE
                key=f"manual_groups_{role}_{project_id}",
                disabled=is_training_mode,
                help="All groups are preselected. Deselect any you don't want to configure."  # ADD THIS LINE
            )
            
            selected_groups = [group for group in manual_groups if group["Display Title"] in selected_group_names]
            
            if selected_groups:
                display_manual_auto_submit_controls(selected_groups, target_videos, project_id, user_id, role, session, is_training_mode)
            else:
                custom_info("💡 All question groups were deselected. Select groups above to configure auto-submit settings.")
    
    else:  # reviewer role - NO AUTO-SUBMIT GROUPS
        st.markdown(f"""
        <div style="{get_card_style('#B180FF')}text-align: center;">
            <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                🔍 Reviewer Auto-Submit - Create ground truth using weighted majority voting
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Get project details
        try:
            project = ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
            question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
        except Exception as e:
            st.error(f"Error loading project details: {str(e)}")
            return
        
        if not question_groups:
            st.warning("No question groups found in this project.")
            return
        
        # Get ALL project videos
        all_project_videos = get_project_videos(project_id=project_id, session=session)
        
        # Calculate current page videos  
        videos_per_page = st.session_state.get(f"{role}_per_page", min(4, len(videos)))
        page_key = f"{role}_current_page_{project_id}"
        current_page = st.session_state.get(page_key, 0)
        
        start_idx = current_page * videos_per_page
        end_idx = min(start_idx + videos_per_page, len(videos))
        current_page_videos = videos[start_idx:end_idx]
        
        # Scope selection for reviewers
        st.markdown("### 🎯 Scope Selection")
        scope_options = ["Current page of videos", "Entire project"]
        
        selected_scope = st.radio(
            "Auto-submit scope:",
            scope_options,
            index=1,  # Default to entire project for reviewers
            key=f"auto_submit_scope_{role}_{project_id}",
            help="Choose whether to apply auto-submit to current page or all videos"
        )
        
        if selected_scope == "Current page of videos":
            target_videos = current_page_videos
            page_info = f" (page {current_page + 1})" if len(videos) > videos_per_page else ""
            custom_info(f"📊 Target: {len(target_videos)} videos on current page{page_info}")
        else:
            target_videos = all_project_videos
            custom_info(f"📊 Target: {len(target_videos)} videos in entire project")
        
        # Manual controls for ALL groups (no auto-submit groups for reviewers)
        st.markdown("### 🎛️ Ground Truth Auto-Submit Controls")
        
        selected_group_names = st.multiselect(
            "Select question groups for ground truth auto-submit:",
            [group["Display Title"] for group in question_groups],
            default=[group["Display Title"] for group in question_groups],  # ADD THIS LINE
            key=f"manual_groups_{role}_{project_id}",
            help="All groups are preselected. Deselect any you don't want to configure."  # ADD THIS LINE
        )
        
        selected_groups = [group for group in question_groups if group["Display Title"] in selected_group_names]
        
        if selected_groups:
            display_manual_auto_submit_controls(selected_groups, target_videos, project_id, user_id, role, session, False)


###############################################################################
# SMART ANNOTATOR SELECTION
###############################################################################

def display_smart_annotator_selection(annotators: Dict[str, Dict], project_id: int):
    """Modern, compact annotator selection with completion checks and confidence scores for model users"""
    with st.expander("⚙️ Annotator Selection (Expand to see options)", expanded=False):
        if not annotators:
            custom_info("No annotators have submitted answers for this project yet.")
            return []
        
        # Check completion status for each annotator
        try:
            with get_db_session() as session:
                completion_progress = get_cached_user_completion_progress(project_id=project_id, session=session)
        
            completed_annotators = {}
            incomplete_annotators = {}
            
            for annotator_display, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id and user_id in completion_progress:
                    progress = completion_progress[user_id]
                    if progress >= 100:
                        completed_annotators[annotator_display] = annotator_info
                    else:
                        incomplete_annotators[annotator_display] = annotator_info
                else:
                    incomplete_annotators[annotator_display] = annotator_info
        except:
            # Fallback: treat none as completed if we can't check
            print("Error checking completion status for annotators")
            completed_annotators = {}
            incomplete_annotators = annotators
        
        if "selected_annotators" not in st.session_state:
            # Only select completed annotators by default
            completed_options = list(completed_annotators.keys())
            st.session_state.selected_annotators = completed_options[:3] if len(completed_options) > 3 else completed_options
        
        all_annotator_options = list(annotators.keys())
        
        with st.container():
            st.markdown("#### 🎯 Quick Actions")
            
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("✅ Select All Completed", key=f"select_completed_{project_id}", help="Select all annotators who completed the project", use_container_width=True):
                    st.session_state.selected_annotators = list(completed_annotators.keys())
                    st.rerun()
            
            with btn_col2:
                if st.button("❌ Clear All", key=f"clear_all_{project_id}", help="Deselect all annotators", use_container_width=True):
                    st.session_state.selected_annotators = []
                    st.rerun()
            
            # Status display
            selected_count = len(st.session_state.selected_annotators)
            completed_count = len(completed_annotators)
            total_count = len(annotators)
            
            status_color = COLORS['success'] if selected_count > 0 else COLORS['secondary']
            status_text = f"📊 {selected_count} selected • {completed_count} completed • {total_count} total"
            
            # st.markdown(f"""
            # <div style="background: linear-gradient(135deg, {status_color}15, {status_color}08); border: 1px solid {status_color}40; border-radius: 8px; padding: 8px 16px; margin: 12px 0; text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,0.1);">
            #     <div style="color: {status_color}; font-weight: 600; font-size: 0.9rem;">{status_text}</div>
            # </div>
            # """, unsafe_allow_html=True)
            custom_info(status_text)
        
        with st.container():
            st.markdown("#### 👥 Choose Annotators")
            st.caption("✅ = Completed project • ⏳ = In progress • 🤖 = AI Model")
            
            updated_selection = []
            
            # Display completed annotators first
            if completed_annotators:
                st.markdown("**✅ Completed Annotators:**")
                display_annotator_checkboxes(completed_annotators, project_id, "completed", updated_selection, disabled=False)
            
            # Display incomplete annotators (NO LONGER DISABLED)
            if incomplete_annotators:
                st.markdown("**⏳ Incomplete Annotators:**")
                display_annotator_checkboxes(incomplete_annotators, project_id, "incomplete", updated_selection, disabled=False)
            
            if set(updated_selection) != set(st.session_state.selected_annotators):
                st.session_state.selected_annotators = updated_selection
                st.rerun()
        
        # Selection summary with model user indicators and completion status
        if st.session_state.selected_annotators:
            initials_list = []
            for annotator in st.session_state.selected_annotators:
                annotator_info = annotators.get(annotator, {})
                user_role = annotator_info.get('role', 'human')
                
                if " (" in annotator and annotator.endswith(")"):
                    initials = annotator.split(" (")[1][:-1]
                else:
                    initials = annotator[:2].upper()
                
                # Show completion status
                if annotator in completed_annotators:
                    status_icon = "🤖✅" if user_role == "model" else "✅"
                else:
                    status_icon = "🤖⏳" if user_role == "model" else "⏳"
                
                initials_list.append(f"{status_icon}{initials}")
            
            if len(initials_list) <= 8:
                initials_text = " • ".join(initials_list)
            else:
                shown = initials_list[:6]
                remaining = len(initials_list) - 6
                initials_text = f"{' • '.join(shown)} + {remaining} more"
            
            # st.markdown(f"""
            # <div style="background: linear-gradient(135deg, #e8f5e8, #d4f1d4); border: 2px solid #28a745; border-radius: 12px; padding: 12px 16px; margin: 16px 0; text-align: center; box-shadow: 0 2px 8px rgba(40, 167, 69, 0.2);">
            #     <div style="color: #155724; font-weight: 600; font-size: 0.95rem;">
            #         ✅ Currently Selected: {initials_text}
            #     </div>
            # </div>
            # """, unsafe_allow_html=True)
            custom_info(f"Currently Selected: {initials_text}")
        else:
            # st.markdown(f"""
            # <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border: 2px solid #ffc107; border-radius: 12px; padding: 12px 16px; margin: 16px 0; text-align: center; box-shadow: 0 2px 8px rgba(255, 193, 7, 0.2);">
            #     <div style="color: #856404; font-weight: 600; font-size: 0.95rem;">
            #         ⚠️ No annotators selected - results will only show ground truth
            #     </div>
            # </div>
            # """, unsafe_allow_html=True)
            custom_info("⚠️ No annotators selected - results will only show ground truth")
        

        return st.session_state.selected_annotators


###############################################################################
# REMAINING UNCHANGED UTILITY FUNCTIONS
###############################################################################

def _get_submit_button_config(role: str, form_disabled: bool, all_questions_modified_by_admin: bool, has_any_editable_questions: bool, is_group_complete: bool, mode: str, ground_truth_exists: bool = False, has_any_admin_modified_questions: bool = False) -> Tuple[str, bool]:
    """Get the submit button text and disabled state with improved logic"""
    if role == "annotator":
        if form_disabled:
            return "🔒 Already Submitted", True
        elif is_group_complete and mode != "Training":
            return "✅ Re-submit Answers", False
        elif is_group_complete:
            return "✅ Completed", True
        else:
            return "Submit Answers", False
    elif role == "meta_reviewer":
        if not ground_truth_exists:
            return "🚫 No Ground Truth Yet", True
        else:
            return "🎯 Override Ground Truth", False
    else:  # reviewer
        if has_any_admin_modified_questions:
            return "🔒 Overridden by Admin", True
        elif not has_any_editable_questions:
            return "🔒 No Editable Questions", True
        elif is_group_complete:
            return "✅ Re-submit Ground Truth", False
        else:
            return "Submit Ground Truth", False





def _get_question_display_data(video_id: int, project_id: int, user_id: int, group_id: int, role: str, mode: str, session: Session, has_any_admin_modified_questions: bool) -> Dict:
    """Get all the data needed to display a question group"""
    questions = get_questions_by_group_cached(group_id=group_id, session=session)
    
    if not questions:
        return {"questions": [], "error": "No questions in this group."}
    
    all_questions_modified_by_admin = GroundTruthService.check_all_questions_modified_by_admin(
        video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
    )
    
    if role == "annotator":
        existing_answers = AnnotatorService.get_user_answers_for_question_group(
            video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
        )
    else:
        existing_answers = GroundTruthService.get_ground_truth_dict_for_question_group(
            video_id=video_id, project_id=project_id, question_group_id=group_id, session=session
        )
    
    form_disabled = False
    if role == "annotator":
        form_disabled = (mode == "Training" and 
                       AnnotatorService.check_user_has_submitted_answers(
                           video_id=video_id, project_id=project_id, user_id=user_id, question_group_id=group_id, session=session
                       ))
    elif role == "reviewer":
        form_disabled = has_any_admin_modified_questions
    
    return {
        "questions": questions,
        "all_questions_modified_by_admin": all_questions_modified_by_admin,
        "existing_answers": existing_answers,
        "form_disabled": form_disabled,
        "error": None
    }

def check_question_group_completion(video_id: int, project_id: int, user_id: int, question_group_id: int, role: str, session: Session) -> bool:
    """Check if a question group is complete for the user/role"""
    try:
        if role == "annotator":
            return AnnotatorService.check_user_has_submitted_answers(
                video_id=video_id, project_id=project_id, user_id=user_id, 
                question_group_id=question_group_id, session=session
            )
        elif role == "meta_reviewer":
            return (GroundTruthService.check_all_questions_modified_by_admin(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ) or check_all_questions_have_ground_truth(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ))
        else:  # reviewer
            return (GroundTruthService.check_all_questions_modified_by_admin(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ) or check_all_questions_have_ground_truth(
                video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
            ))
    except:
        return False



def get_ground_truth_option_filters_enhanced(project_id: int, session: Session) -> Dict[int, List[str]]:
    """Get available ground truth options for filtering"""
    try:
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        single_choice_questions = [q for q in questions if q["type"] == "single"]
        
        if not single_choice_questions:
            return {}
        
        gt_options = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for question in single_choice_questions:
            question_id = question["id"]
            unique_answers = set()
            
            # Check each video for ground truth answers
            for video in videos:
                try:
                    gt_df = GroundTruthService.get_ground_truth(
                        video_id=video["id"], project_id=project_id, session=session
                    )
                    if not gt_df.empty:
                        question_gt = gt_df[gt_df["Question ID"] == question_id]
                        if not question_gt.empty:
                            unique_answers.add(question_gt.iloc[0]["Answer Value"])
                except:
                    continue
            
            if unique_answers:
                gt_options[question_id] = list(unique_answers)
        
        return gt_options
    except Exception as e:
        st.error(f"Error getting ground truth options: {str(e)}")
        return {}



###############################################################################
# PROJECT VIEW FUNCTIONS
###############################################################################

def get_model_confidence_scores_enhanced(project_id: int, model_user_ids: List[int], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Get confidence scores for specific model users on specific questions"""
    try:
        if not model_user_ids or not question_ids:
            return {}
        
        confidence_scores = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_confidence = 0.0
            answer_count = 0
            
            for model_user_id in model_user_ids:
                for question_id in question_ids:
                    try:
                        # Use AnnotatorService to get answers
                        answers_df = AnnotatorService.get_question_answers(
                            question_id=question_id, project_id=project_id, session=session
                        )
                        
                        if not answers_df.empty:
                            # Filter for this model user and video
                            user_video_answers = answers_df[
                                (answers_df["User ID"] == model_user_id) & 
                                (answers_df["Video ID"] == video_id)
                            ]
                            
                            if not user_video_answers.empty:
                                confidence = user_video_answers.iloc[0]["Confidence Score"]
                                if confidence is not None:
                                    total_confidence += confidence
                                    answer_count += 1
                    except Exception:
                        continue
            
            if answer_count > 0:
                confidence_scores[video_id] = total_confidence / answer_count
            else:
                confidence_scores[video_id] = 0.0
        
        return confidence_scores
    except Exception as e:
        st.error(f"Error getting model confidence scores: {str(e)}")
        return {}

def get_annotator_consensus_rates_enhanced(project_id: int, selected_annotators: List[str], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Calculate consensus rates with proper handling of incomplete annotations"""
    try:
        if not selected_annotators or not question_ids or len(selected_annotators) < 2:
            return {}
        
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        if len(annotator_user_ids) < 2:
            return {}
        
        consensus_rates = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_questions_with_multiple_answers = 0
            consensus_count = 0
            
            for question_id in question_ids:
                try:
                    answers_df = AnnotatorService.get_question_answers(
                        question_id=question_id, project_id=project_id, session=session
                    )
                    
                    if not answers_df.empty:
                        video_answers = answers_df[
                            (answers_df["Video ID"] == video_id) & 
                            (answers_df["User ID"].isin(annotator_user_ids))
                        ]
                        
                        # Only consider questions where at least 2 annotators answered
                        if len(video_answers) >= 2:
                            total_questions_with_multiple_answers += 1
                            answer_values = video_answers["Answer Value"].tolist()
                            
                            from collections import Counter
                            answer_counts = Counter(answer_values)
                            most_common_count = answer_counts.most_common(1)[0][1]
                            consensus_rate = most_common_count / len(answer_values)
                            
                            if consensus_rate >= 0.5:  # Majority agreement
                                consensus_count += 1
                except Exception:
                    continue
            
            if total_questions_with_multiple_answers > 0:
                consensus_rates[video_id] = consensus_count / total_questions_with_multiple_answers
            else:
                consensus_rates[video_id] = 0.0
        
        return consensus_rates
    except Exception as e:
        st.error(f"Error calculating consensus rates: {str(e)}")
        return {}

def get_video_accuracy_rates(project_id: int, selected_annotators: List[str], question_ids: List[int], session: Session) -> Dict[int, float]:
    """Calculate accuracy rates with proper handling of incomplete annotations"""
    try:
        if not selected_annotators or not question_ids:
            return {}
        
        annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        
        if not annotator_user_ids:
            return {}
        
        accuracy_rates = {}
        videos = get_project_videos(project_id=project_id, session=session)
        
        for video in videos:
            video_id = video["id"]
            total_comparisons = 0
            correct_comparisons = 0
            
            try:
                gt_df = GroundTruthService.get_ground_truth(
                    video_id=video_id, project_id=project_id, session=session
                )
                
                if gt_df.empty:
                    accuracy_rates[video_id] = 0.0
                    continue
                
                for question_id in question_ids:
                    question_gt = gt_df[gt_df["Question ID"] == question_id]
                    if question_gt.empty:
                        continue
                    
                    gt_answer = question_gt.iloc[0]["Answer Value"]
                    
                    answers_df = AnnotatorService.get_question_answers(
                        question_id=question_id, project_id=project_id, session=session
                    )
                    
                    if not answers_df.empty:
                        video_answers = answers_df[
                            (answers_df["Video ID"] == video_id) & 
                            (answers_df["User ID"].isin(annotator_user_ids))
                        ]
                        
                        # Count all annotator answers for this question/video
                        for _, answer_row in video_answers.iterrows():
                            total_comparisons += 1
                            if answer_row["Answer Value"] == gt_answer:
                                correct_comparisons += 1
                
                if total_comparisons > 0:
                    accuracy_rates[video_id] = correct_comparisons / total_comparisons
                else:
                    accuracy_rates[video_id] = 0.0
                    
            except Exception:
                accuracy_rates[video_id] = 0.0
        
        return accuracy_rates
    except Exception as e:
        st.error(f"Error calculating accuracy rates: {str(e)}")
        return {}
    
def get_video_completion_rates_enhanced(project_id: int, question_ids: List[int], session: Session) -> Dict[int, float]:
    """Get completion rates for each video based on specific questions"""
    try:
        if not question_ids:
            return {}
            
        videos = get_project_videos(project_id=project_id, session=session)
        completion_rates = {}
        
        for video in videos:
            video_id = video["id"]
            completed_questions = 0
            
            for question_id in question_ids:
                try:
                    # Check if this question has ground truth for this video
                    gt_df = GroundTruthService.get_ground_truth(
                        video_id=video_id, project_id=project_id, session=session
                    )
                    if not gt_df.empty:
                        question_answers = gt_df[gt_df["Question ID"] == question_id]
                        if not question_answers.empty:
                            completed_questions += 1
                except:
                    continue
            
            completion_rates[video_id] = (completed_questions / len(question_ids)) * 100 if question_ids else 0.0
        
        return completion_rates
    except Exception as e:
        st.error(f"Error calculating video completion rates: {str(e)}")
        return {}

def apply_video_sorting_and_filtering_enhanced(videos: List[Dict], sort_by: str, sort_order: str, 
                                             filter_by_gt: Dict[int, str], project_id: int, 
                                             selected_annotators: List[str], session: Session,
                                             sort_config: Dict[str, Any] = None) -> List[Dict]:
    """Apply sorting and filtering with proper ascending/descending logic"""
    try:
        sort_config = sort_config or {}
        
        # For Default sorting, we still need to respect ascending/descending order
        # Sort by video ID as a stable sort key
        if sort_by == "Default":
            reverse = (sort_order == "Descending")
            videos.sort(key=lambda x: x.get("id", 0), reverse=reverse)
        else:
            # Only apply advanced sorting if it was explicitly applied by user
            sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
            
            if not sort_applied:
                # If sort hasn't been applied yet, use default order
                reverse = (sort_order == "Descending")
                videos.sort(key=lambda x: x.get("id", 0), reverse=reverse)
            else:
                # Get sorting data based on configuration
                if sort_by == "Model Confidence":
                    model_user_ids = sort_config.get("model_user_ids", [])
                    question_ids = sort_config.get("question_ids", [])
                    if model_user_ids and question_ids:
                        confidence_scores = get_model_confidence_scores_enhanced(
                            project_id=project_id, model_user_ids=model_user_ids, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        confidence_scores = {}
                elif sort_by == "Annotator Consensus":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids and len(selected_annotators) >= 2:
                        consensus_rates = get_annotator_consensus_rates_enhanced(
                            project_id=project_id, selected_annotators=selected_annotators, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        consensus_rates = {}
                elif sort_by == "Completion Rate":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids:
                        completion_rates = get_video_completion_rates_enhanced(
                            project_id=project_id, question_ids=question_ids, session=session
                        )
                    else:
                        completion_rates = {}
                elif sort_by == "Accuracy Rate":
                    question_ids = sort_config.get("question_ids", [])
                    if question_ids and selected_annotators:
                        accuracy_rates = get_video_accuracy_rates(
                            project_id=project_id, selected_annotators=selected_annotators, 
                            question_ids=question_ids, session=session
                        )
                    else:
                        accuracy_rates = {}
        
        # Apply ground truth filtering
        filtered_videos = []
        for video in videos:
            video_id = video["id"]
            include_video = True
            
            if filter_by_gt:
                try:
                    gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                    if gt_df.empty:
                        include_video = False
                    else:
                        for question_id, required_answer in filter_by_gt.items():
                            question_gt = gt_df[gt_df["Question ID"] == question_id]
                            if question_gt.empty or question_gt.iloc[0]["Answer Value"] != required_answer:
                                include_video = False
                                break
                except:
                    include_video = False
            
            if include_video:
                # Add sorting score to video (only for non-default sorts)
                if sort_by != "Default" and st.session_state.get(f"sort_applied_{project_id}", False):
                    if sort_by == "Model Confidence":
                        video["sort_score"] = confidence_scores.get(video_id, 0)
                    elif sort_by == "Annotator Consensus":
                        video["sort_score"] = consensus_rates.get(video_id, 0)
                    elif sort_by == "Completion Rate":
                        video["sort_score"] = completion_rates.get(video_id, 0)
                    elif sort_by == "Accuracy Rate":
                        video["sort_score"] = accuracy_rates.get(video_id, 0)
                
                filtered_videos.append(video)
        
        # Sort videos by score if not default
        if sort_by != "Default" and st.session_state.get(f"sort_applied_{project_id}", False):
            reverse = (sort_order == "Descending")
            filtered_videos.sort(key=lambda x: x.get("sort_score", 0), reverse=reverse)
        
        return filtered_videos
    except Exception as e:
        st.error(f"Error applying sorting and filtering: {str(e)}")
        return videos


def apply_annotator_video_sorting(videos: List[Dict], sort_by: str, sort_order: str, 
                                project_id: int, user_id: int, session: Session) -> List[Dict]:
    """Apply sorting for annotators - only completion rate and accuracy rate vs ground truth"""
    try:
        if sort_by == "Default":
            reverse = (sort_order == "Descending")
            videos.sort(key=lambda x: x.get("id", 0), reverse=reverse)
            return videos
        
        # Get selected questions for sorting
        if sort_by == "Completion Rate":
            question_key = f"annotator_completion_rate_questions_{project_id}"
        else:  # Accuracy Rate
            question_key = f"annotator_accuracy_rate_questions_{project_id}"
        
        selected_questions = st.session_state.get(question_key, [])
        if not selected_questions:
            return videos
        
        # Extract question IDs
        question_ids = []
        for q_display in selected_questions:
            try:
                q_id = int(q_display.split("(ID: ")[1].split(")")[0])
                question_ids.append(q_id)
            except:
                continue
        
        if not question_ids:
            return videos
        
        # Calculate scores for each video
        video_scores = {}
        
        for video in videos:
            video_id = video["id"]
            
            if sort_by == "Completion Rate":
                # Calculate completion rate for this user
                completed_questions = 0
                for question_id in question_ids:
                    try:
                        answers_df = AnnotatorService.get_question_answers(
                            question_id=question_id, project_id=project_id, session=session
                        )
                        if not answers_df.empty:
                            user_answers = answers_df[
                                (answers_df["User ID"] == user_id) & 
                                (answers_df["Video ID"] == video_id)
                            ]
                            if not user_answers.empty:
                                completed_questions += 1
                    except:
                        continue
                
                video_scores[video_id] = (completed_questions / len(question_ids)) * 100 if question_ids else 0
                
            else:  # Accuracy Rate
                # Calculate accuracy rate vs ground truth for this user
                correct_count = 0
                total_count = 0
                
                try:
                    gt_df = GroundTruthService.get_ground_truth(
                        video_id=video_id, project_id=project_id, session=session
                    )
                    
                    if not gt_df.empty:
                        for question_id in question_ids:
                            # Get ground truth for this question
                            question_gt = gt_df[gt_df["Question ID"] == question_id]
                            if question_gt.empty:
                                continue
                            
                            gt_answer = question_gt.iloc[0]["Answer Value"]
                            
                            # Get user's answer
                            answers_df = AnnotatorService.get_question_answers(
                                question_id=question_id, project_id=project_id, session=session
                            )
                            
                            if not answers_df.empty:
                                user_answers = answers_df[
                                    (answers_df["User ID"] == user_id) & 
                                    (answers_df["Video ID"] == video_id)
                                ]
                                
                                if not user_answers.empty:
                                    user_answer = user_answers.iloc[0]["Answer Value"]
                                    total_count += 1
                                    if user_answer == gt_answer:
                                        correct_count += 1
                    
                    video_scores[video_id] = (correct_count / total_count) * 100 if total_count > 0 else 0
                except:
                    video_scores[video_id] = 0
        
        # Add scores to videos and sort
        for video in videos:
            video["sort_score"] = video_scores.get(video["id"], 0)
        
        reverse = (sort_order == "Descending")
        videos.sort(key=lambda x: x.get("sort_score", 0), reverse=reverse)
        
        return videos
        
    except Exception as e:
        st.error(f"Error applying annotator sorting: {str(e)}")
        return videos

def display_project_progress(user_id: int, project_id: int, role: str, session: Session):
    """Display project progress in a refreshable fragment"""
    if role == "annotator":
        overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
        st.progress(overall_progress / 100)
        st.markdown(f"**Your Overall Progress:** {overall_progress:.1f}%")
        display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        
    elif role == "meta_reviewer":
        try:
            project_progress = ProjectService.progress(project_id=project_id, session=session)
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
            display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")
    else:
        try:
            project_progress = ProjectService.progress(project_id=project_id, session=session)
            st.progress(project_progress['completion_percentage'] / 100)
            st.markdown(f"**Ground Truth Progress:** {project_progress['completion_percentage']:.1f}%")
            display_user_accuracy_simple(user_id=user_id, project_id=project_id, role=role, session=session)
        except ValueError as e:
            st.error(f"Error loading project progress: {str(e)}")

def display_project_view(user_id: int, role: str, session: Session):
    """Display the selected project with modern, compact layout and enhanced sorting/filtering"""

    project_id = st.session_state.selected_project_id
    
    if st.button("← Back to Dashboard", key="back_to_dashboard"):
        st.session_state.current_view = "dashboard"
        st.session_state.selected_project_id = None
        if "selected_annotators" in st.session_state:
            del st.session_state.selected_annotators
        # Clear any sorting/filtering state
        for key in list(st.session_state.keys()):
            if key.startswith(f"video_sort_") or key.startswith(f"video_filter_") or key.startswith(f"question_order_"):
                del st.session_state[key]
        st.rerun()
    
    try:
        project = ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
        try:
            schema_details = SchemaService.get_schema_details(schema_id=project["schema_id"], session=session)
            instructions_url = schema_details.get("instructions_url")
        except Exception as e:
            print(f"Error getting schema details: {e}")
            instructions_url = None

    except ValueError as e:
        st.error(f"Error loading project: {str(e)}")
        return
    
    # 🔥 ADD THE CACHE CLEARING CODE HERE - RIGHT AFTER PROJECT LOADS SUCCESSFULLY
    # Clear cache when entering a new project for fresh data
    if st.session_state.get('last_project_id') != project_id:
        clear_project_cache(project_id)
        st.session_state.last_project_id = project_id
    
    
    mode = "Training" if check_project_has_full_ground_truth(project_id=project_id, session=session) else "Annotation"
    
    st.markdown(f"## 📁 {project['name']}")
    
    # Mode display
    if role == "annotator":
        if mode == "Training":
            custom_info("🎓 Training Mode - Try your best! You'll get immediate feedback after each submission.")
        else:
            custom_info("📝 Annotation Mode - Try your best to answer the questions accurately.")
    elif role == "meta_reviewer":
        custom_info("🎯 Meta-Reviewer Mode - Override ground truth answers as needed. No completion tracking.")
    else:
        custom_info("🔍 Review Mode - Help create the ground truth dataset!")
    
    # RUN AUTO-SUBMIT ONCE AT PROJECT ENTRY FOR ANNOTATORS
    if role == "annotator" and mode == "Annotation":
        auto_submit_key = f"auto_submit_done_{project_id}_{user_id}"
        if auto_submit_key not in st.session_state:
            # Run auto-submit for entire project
            run_project_wide_auto_submit_on_entry(project_id=project_id, user_id=user_id, session=session)
            st.session_state[auto_submit_key] = True
    
    display_project_progress(user_id=user_id, project_id=project_id, role=role, session=session)
    
    videos = get_project_videos(project_id=project_id, session=session)
    
    if not videos:
        st.error("No videos found in this project.")
        return
        
    # Role-specific control panels - NO AUTO-SUBMIT FOR META-REVIEWER
    if role == "reviewer":
        if mode == "Training":
            analytics_tab, annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, auto_submit_tab, instruction_tab = st.tabs([
                "📊 Analytics", "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "⚡ Auto-Submit", "📖 Instructions"
            ])
            
            with analytics_tab:
                st.markdown("#### 🎯 Performance Insights")
                
                st.markdown(f"""
                <div style="{get_card_style('#B180FF')}text-align: center;">
                    <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                        📈 Access detailed accuracy analytics for all participants in this training project
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                display_accuracy_button_for_project(project_id=project_id, role=role, session=session)
                
                # st.markdown(f"""
                # <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid {COLORS['info']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
                #     💡 <strong>Tip:</strong> Use analytics to identify patterns in annotator performance and areas for improvement.
                # </div>
                # """, unsafe_allow_html=True)
                custom_info("💡 Use analytics to identify patterns in annotator performance and areas for improvement.")
        else:
            annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, auto_submit_tab, instruction_tab = st.tabs([
                "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "⚡ Auto-Submit", "📖 Instructions"
            ])
        
        with annotator_tab:
            st.markdown("#### 👥 Annotator Management")
            
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎯 Select which annotators' responses to display during your review process
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
                display_smart_annotator_selection(annotators=annotators, project_id=project_id)
            except Exception as e:
                st.error(f"Error loading annotators: {str(e)}")
                st.session_state.selected_annotators = []
            
            # st.markdown(f"""
            # <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid #9c27b0; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
            #     💡 <strong>Tip:</strong> Select annotators whose responses you want to see alongside your review interface.
            # </div>
            # """, unsafe_allow_html=True)
            custom_info("💡 Select annotators whose responses you want to see alongside your review interface.")
        
        with sort_tab:
            display_enhanced_sort_tab(project_id=project_id, session=session)

        with filter_tab:
            display_enhanced_filter_tab(project_id=project_id, session=session)
        
        with order_tab:
            display_order_tab(project_id=project_id, role=role, project=project, session=session)
        
        with layout_tab:
            display_layout_tab_content(videos=videos, role=role)
        
        with auto_submit_tab:
            display_auto_submit_tab(project_id=project_id, user_id=user_id, role=role, videos=videos, session=session)
        
        with instruction_tab:
            display_instruction_tab_content(instructions_url=instructions_url)
    
    elif role == "meta_reviewer":
        # st.markdown("---")
        
        # NO AUTO-SUBMIT TAB FOR META-REVIEWER
        if mode == "Training":
            analytics_tab, annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, instruction_tab = st.tabs([
                "📊 Analytics", "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "📖 Instructions"
            ])
            
            with analytics_tab:
                st.markdown("#### 🎯 Performance Insights")
                
                st.markdown(f"""
                <div style="{get_card_style('#B180FF')}text-align: center;">
                    <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                        📈 Access detailed accuracy analytics for all participants in this training project
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                display_accuracy_button_for_project(project_id=project_id, role=role, session=session)
                
                # st.markdown(f"""
                # <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid {COLORS['info']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
                #     💡 <strong>Tip:</strong> Use analytics to identify patterns in annotator performance and areas for improvement.
                # </div>
                # """, unsafe_allow_html=True)
                custom_info("💡 Use analytics to identify patterns in annotator performance and areas for improvement.")
        else:
            annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, instruction_tab = st.tabs([
                "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "📖 Instructions"
            ])
        
        with annotator_tab:
            st.markdown("#### 👥 Annotator Management")
            
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎯 Select which annotators' responses to display during your review process
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
                display_smart_annotator_selection(annotators=annotators, project_id=project_id)
            except Exception as e:
                st.error(f"Error loading annotators: {str(e)}")
                st.session_state.selected_annotators = []
            
            # st.markdown(f"""
            # <div style="background: linear-gradient(135deg, #f0f8ff, #e6f3ff); border-left: 4px solid #9c27b0; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
            #     💡 <strong>Tip:</strong> Select annotators whose responses you want to see alongside your review interface.
            # </div>
            # """, unsafe_allow_html=True)
            custom_info("💡 Select annotators whose responses you want to see alongside your review interface.")
        
        with sort_tab:
            display_enhanced_sort_tab(project_id=project_id, session=session)

        with filter_tab:
            display_enhanced_filter_tab(project_id=project_id, session=session)
        
        with order_tab:
            display_order_tab(project_id=project_id, role=role, project=project, session=session)
        
        with layout_tab:
            display_layout_tab_content(videos=videos, role=role)
        
        with instruction_tab:
            display_instruction_tab_content(instructions_url=instructions_url)
    
    else:  # Annotator role
        # st.markdown("---")
        
        instruction_tab, layout_tab, sort_tab, auto_submit_tab = st.tabs(["📖 Instructions", "🎛️ Layout Settings", "🔄 Sort", "⚡ Auto-Submit"])
        
        with instruction_tab:
            display_instruction_tab_content(instructions_url=instructions_url)
        
        with layout_tab:
            display_layout_tab_content(videos=videos, role=role)
        
        with sort_tab:
            display_enhanced_sort_tab_annotator(project_id=project_id, session=session)
    
        with auto_submit_tab:
            display_auto_submit_tab(project_id=project_id, user_id=user_id, role=role, videos=videos, session=session)
    
    # Apply sorting and filtering to videos
    if role in ["reviewer", "meta_reviewer"]:
        sort_by = st.session_state.get(f"video_sort_by_{project_id}", "Default")
        sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Descending")
        filter_by_gt = st.session_state.get(f"video_filters_{project_id}", {})
        selected_annotators = st.session_state.get("selected_annotators", [])
        sort_config = st.session_state.get(f"sort_config_{project_id}", {})
        
        videos = apply_video_sorting_and_filtering_enhanced(
            videos=videos, sort_by=sort_by, sort_order=sort_order,
            filter_by_gt=filter_by_gt, project_id=project_id,
            selected_annotators=selected_annotators, session=session,
            sort_config=sort_config
        )
    elif role == "annotator":
        sort_by = st.session_state.get(f"annotator_video_sort_by_{project_id}", "Default")
        sort_order = st.session_state.get(f"annotator_video_sort_order_{project_id}", "Ascending")
        sort_applied = st.session_state.get(f"annotator_sort_applied_{project_id}", False)
        
        if sort_by != "Default" and sort_applied:
            videos = apply_annotator_video_sorting(
                videos=videos, sort_by=sort_by, sort_order=sort_order,
                project_id=project_id, user_id=user_id, session=session
            )
    
    # Get layout settings
    video_pairs_per_row = st.session_state.get(f"{role}_pairs_per_row", 1)
    videos_per_page = st.session_state.get(f"{role}_per_page", min(4, len(videos)))
    
    st.markdown("---")
    
    # Show sorting/filtering summary
    if role in ["reviewer", "meta_reviewer"]:
        sort_by = st.session_state.get(f"video_sort_by_{project_id}", "Default")
        sort_applied = st.session_state.get(f"sort_applied_{project_id}", False)
        filter_by_gt = st.session_state.get(f"video_filters_{project_id}", {})
        
        summary_parts = []
        if sort_by != "Default" and sort_applied:
            sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Ascending")
            summary_parts.append(f"🔄 {sort_by} ({sort_order})")
        elif sort_by != "Default" and not sort_applied:
            summary_parts.append(f"⚙️ {sort_by} configured")
        elif sort_by == "Default":
            sort_order = st.session_state.get(f"video_sort_order_{project_id}", "Ascending")
            summary_parts.append(f"📋 Default order ({sort_order})")
        
        if filter_by_gt:
            summary_parts.append(f"🔍 {len(filter_by_gt)} filter(s)")
        
        if summary_parts:
            custom_info(" • ".join(summary_parts))
    elif role == "annotator":
        sort_by = st.session_state.get(f"annotator_video_sort_by_{project_id}", "Default")
        sort_applied = st.session_state.get(f"annotator_sort_applied_{project_id}", False)
        
        if sort_by != "Default" and sort_applied:
            sort_order = st.session_state.get(f"annotator_video_sort_order_{project_id}", "Ascending")
            custom_info(f"🔄 {sort_by} ({sort_order})")
        elif sort_by != "Default" and not sort_applied:
            custom_info(f"⚙️ {sort_by} configured")
        else:
            sort_order = st.session_state.get(f"annotator_video_sort_order_{project_id}", "Ascending")
            custom_info(f"📋 Default order ({sort_order})")
    # Calculate pagination
    total_pages = (len(videos) - 1) // videos_per_page + 1 if videos else 1
    
    page_key = f"{role}_current_page_{project_id}"
    if page_key not in st.session_state:
        st.session_state[page_key] = 0
    
    current_page = st.session_state[page_key]
    
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(videos))
    page_videos = videos[start_idx:end_idx]
    
    # st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(videos)}**")
    st.markdown('<div id="video-list-section"></div>', unsafe_allow_html=True)
    video_list_info_str = f"Showing videos {start_idx + 1}-{end_idx} of {len(videos)}"
    display_pagination_controls(current_page, total_pages, page_key, role, project_id, "top", video_list_info_str)

    
    # Display videos
    for i in range(0, len(page_videos), video_pairs_per_row):
        row_videos = page_videos[i:i + video_pairs_per_row]
        
        if video_pairs_per_row == 1:
            display_video_answer_pair(row_videos[0], project_id, user_id, role, mode, session)
        else:
            pair_cols = st.columns(video_pairs_per_row)
            for j, video in enumerate(row_videos):
                with pair_cols[j]:
                    display_video_answer_pair(video, project_id, user_id, role, mode, session)
        
        st.markdown("---")
    
    display_pagination_controls(current_page, total_pages, page_key, role, project_id, "bottom", video_list_info_str)

def display_instruction_tab_content(instructions_url: Optional[str]):
    """Display instruction tab content with external URL button"""
    st.markdown("#### 📖 Project Instructions")
    
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}text-align: center;">
        <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
            📚 Access detailed instructions and guidelines for this annotation project
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    if instructions_url:
        st.link_button("🔗 Open Instructions", 
                      url=instructions_url,
                      use_container_width=True,
                      help="Open the project instructions in a new tab")
        
    else:
        st.button("🔗 Open Instructions", 
                 disabled=True, 
                 use_container_width=True,
                 help="No instructions URL configured for this project")
        
        st.markdown(f"""
        <div style="margin-top: 16px; padding: 12px; background: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
            <p style="margin: 0; color: #856404; font-size: 0.9rem;">
                ⚠️ <strong>No instructions available</strong><br>
                Contact your project administrator to add instructions for this project.
            </p>
        </div>
        """, unsafe_allow_html=True)
    
###############################################################################
# PROJECT DASHBOARD FUNCTIONS
###############################################################################

def display_project_dashboard(user_id: int, role: str, session: Session) -> Optional[int]:
    """Display project group dashboard with enhanced clarity and pagination - FIXED SEARCH ISSUE"""
    st.markdown("## 📂 Project Dashboard")
    
    backend_role = "admin" if role == "meta_reviewer" else role
    grouped_projects = get_project_groups_with_projects(user_id=user_id, role=backend_role, session=session)
    
    if not grouped_projects:
        st.warning(f"No projects assigned to you as {role}.")
        return None
    
    assignment_dates = get_user_assignment_dates(user_id=user_id, session=session)
    
    st.markdown("### 🔍 Search & Filter")
    col1, col2, col3 = st.columns(3)
    with col1:
        search_term = st.text_input("🔍 Search projects", placeholder="Enter project name...")
    with col2:
        sort_by = st.selectbox("Sort by", ["Completion Rate", "Name", "Assignment Date"])
    with col3:
        sort_order = st.selectbox("Order", ["Ascending", "Descending"])
    
    st.markdown("---")
    
    selected_project_id = None
    
    for group_index, (group_name, projects) in enumerate(grouped_projects.items()):
        if not projects:
            continue
        
        filtered_projects = [p for p in projects if not search_term or search_term.lower() in p["name"].lower()]
        if not filtered_projects:
            continue
        
        # Calculate completion rates and assignment dates for sorting
        for project in filtered_projects:
            try:
                if role == "annotator":
                    project["completion_rate"] = calculate_user_overall_progress(user_id=user_id, project_id=project["id"], session=session)
                else:
                    project_progress = ProjectService.progress(project_id=project["id"], session=session)
                    project["completion_rate"] = project_progress['completion_percentage']
                
                project_assignments = assignment_dates.get(project["id"], {})
                project_assignment_date = project_assignments.get(backend_role, "Not set")
                project["assignment_date"] = project_assignment_date
                
                if project_assignment_date and project_assignment_date not in ["Not set", "Unknown"]:
                    try:
                        project["assignment_datetime"] = datetime.strptime(project_assignment_date, "%Y-%m-%d")
                    except:
                        project["assignment_datetime"] = datetime.min
                else:
                    project["assignment_datetime"] = datetime.min
                    
            except:
                project["completion_rate"] = 0.0
                project["assignment_date"] = "Unknown"
                project["assignment_datetime"] = datetime.min
        
        # Sort projects
        if sort_by == "Completion Rate":
            filtered_projects.sort(key=lambda x: x["completion_rate"], reverse=(sort_order == "Descending"))
        elif sort_by == "Name":
            filtered_projects.sort(key=lambda x: x["name"], reverse=(sort_order == "Descending"))
        elif sort_by == "Assignment Date":
            filtered_projects.sort(key=lambda x: x["assignment_datetime"], reverse=(sort_order == "Descending"))
        else:
            filtered_projects.sort(key=lambda x: x["completion_rate"])
        
        total_projects = len(filtered_projects)
        projects_per_page = 6
        total_pages = (total_projects - 1) // projects_per_page + 1 if total_projects > 0 else 1
        
        page_key = f"group_page_{group_name}_{user_id}_{role}"
        
        # RESET PAGINATION WHEN SEARCH IS ACTIVE - THIS IS THE FIX!
        if search_term:
            # If there's a search term, always start from page 0
            current_page = 0
        else:
            # Normal pagination behavior when no search
            if page_key not in st.session_state:
                st.session_state[page_key] = 0
            current_page = st.session_state[page_key]
        
        # Ensure current page is valid for the filtered results
        if current_page >= total_pages:
            current_page = 0
            if not search_term:  # Only update session state if not searching
                st.session_state[page_key] = 0
        
        # group_color = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6", "#1abc9c"][group_index % 6]
        group_color = "#9553FE"
        display_group_name = group_name
        truncated_group_name = group_name[:67] + "..." if len(group_name) > 70 else group_name
        
        # Enhanced group header with search indicator
        search_indicator = ""
        if search_term:
            search_indicator = f"🔍 Filtered by '{search_term}' • "
        
        st.markdown(f"""
        <div style="{get_card_style(group_color)}position: relative;">
            <div style="position: absolute; top: -8px; left: 20px; background: {group_color}; color: white; padding: 4px 12px; border-radius: 10px; font-size: 0.8rem; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                PROJECT GROUP
            </div>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 8px;">
                <div>
                    <h2 style="margin: 0; color: {group_color}; font-size: 1.8rem;" title="{display_group_name}">📁 {truncated_group_name}</h2>
                    <p style="margin: 8px 0 0 0; color: #34495e; font-size: 1.1rem; font-weight: 500;">
                        {search_indicator}{total_projects} projects in this group {f"• Page {current_page + 1} of {total_pages}" if total_pages > 1 else ""}
                    </p>
                </div>
                <div style="text-align: right;">
                    <span style="background: {group_color}; color: white; padding: 10px 18px; border-radius: 20px; font-weight: bold; font-size: 1.1rem; box-shadow: 0 3px 6px rgba(0,0,0,0.2);">{total_projects} Projects</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Pagination controls - only show if not searching and multiple pages
        if total_pages > 1 and not search_term:
            page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
            
            with page_col1:
                if st.button("◀ Prev", disabled=(current_page == 0), key=f"prev_{page_key}", use_container_width=True):
                    st.session_state[page_key] = max(0, current_page - 1)
                    st.rerun()
            
            with page_col2:
                page_options = [f"Page {i+1}" for i in range(total_pages)]
                selected_page_name = st.selectbox(
                    f"Page for {group_name}",
                    page_options,
                    index=current_page,
                    key=f"page_select_{page_key}",
                    label_visibility="collapsed"
                )
                new_page = page_options.index(selected_page_name)
                if new_page != current_page:
                    st.session_state[page_key] = new_page
                    st.rerun()
            
            with page_col3:
                if st.button("Next ▶", disabled=(current_page == total_pages - 1), key=f"next_{page_key}", use_container_width=True):
                    st.session_state[page_key] = min(total_pages - 1, current_page + 1)
                    st.rerun()
        
        elif search_term and total_pages > 1:
            # Show search pagination info but no controls
            custom_info(f"🔍 Search results span {total_pages} pages. Showing page 1 of search results.")
        
        # Display projects
        start_idx = current_page * projects_per_page
        end_idx = min(start_idx + projects_per_page, total_projects)
        page_projects = filtered_projects[start_idx:end_idx]
        
        if page_projects:
            cols = st.columns(3)
            
            for i, project in enumerate(page_projects):
                with cols[i % 3]:
                    has_full_gt = check_project_has_full_ground_truth(project_id=project["id"], session=session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    completion_rate = project.get("completion_rate", 0.0)
                    assignment_date = project.get("assignment_date", "Unknown")
                    progress_text = f"{completion_rate:.1f}% Complete"
                    
                    project_name = project["name"]
                    truncated_tag_group_name = group_name[:57] + "..." if len(group_name) > 60 else group_name
                    
                    # Highlight search matches
                    highlighted_name = project_name
                    if search_term:
                        # Simple highlighting - could be enhanced further
                        highlighted_name = project_name.replace(
                            search_term, 
                            f"🔍 {search_term}"
                        )
                    
                    with st.container():
                        st.markdown(f"""
                        <div style="border: 2px solid {group_color}; border-radius: 12px; padding: 18px; margin: 8px 0; background: linear-gradient(135deg, white, {group_color}05); box-shadow: 0 4px 8px rgba(0,0,0,0.1); min-height: 200px; position: relative;" title="Group: {display_group_name}">
                            <div style="position: absolute; top: -6px; right: 10px; background: {group_color}; color: white; padding: 2px 6px; border-radius: 6px; font-size: 0.7rem; font-weight: bold;" title="{display_group_name}">
                                {truncated_tag_group_name}
                            </div>
                            <h4 style="margin: 10px 0 8px 0; color: black; font-size: 1.1rem; line-height: 1.3; word-wrap: break-word;" title="{project_name}">{highlighted_name}</h4>
                            <p style="margin: 8px 0; color: #666; font-size: 0.9rem; min-height: 50px;">
                                {project["description"] or 'No description'}
                            </p>
                            <div style="margin: 12px 0;">
                                <p style="margin: 4px 0;"><strong>Mode:</strong> {mode}</p>
                                <p style="margin: 4px 0;"><strong>Progress:</strong> {progress_text}</p>
                                <p style="margin: 4px 0; color: #666; font-size: 0.85rem;">
                                    <strong>Assigned:</strong> {assignment_date}
                                </p>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        if st.button("Open Project", 
                                   key=f"select_project_{project['id']}", 
                                   use_container_width=True,
                                   help=f"Open '{project_name}' from {display_group_name} group"):
                            selected_project_id = project["id"]
                            st.session_state.selected_project_id = project["id"]
                            st.session_state.current_view = "project"
                            st.rerun()
        else:
            # This should now be much less likely to happen
            if search_term:
                custom_info(f"🔍 No projects matching '{search_term}' found in {group_name}")
            else:
                custom_info(f"No projects found in {group_name}")
        
        if group_index < len(grouped_projects) - 1:
            st.markdown("""<div style="height: 2px; background: linear-gradient(90deg, transparent, #ddd, transparent); margin: 30px 0;"></div>""", unsafe_allow_html=True)
        
        if selected_project_id:
            break
    
    return None
