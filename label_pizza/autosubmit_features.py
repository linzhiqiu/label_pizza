import streamlit as st
from typing import Dict, Optional, List, Any, Tuple
from sqlalchemy.orm import Session
import time

from label_pizza.services import (
    AuthService, AnnotatorService, 
    QuestionService, QuestionGroupService,
    ProjectService, AutoSubmitService, ReviewerAutoSubmitService
)
from label_pizza.ui_components import (
    custom_info, COLORS
)
from label_pizza.database_utils import (
    get_db_session, get_questions_by_group_cached, get_project_videos,
    get_session_cached_project_annotators, get_optimized_annotator_user_ids,
    get_cached_video_reviewer_data, get_session_cache_key,
    get_cached_user_completion_progress, get_schema_question_groups
)

###############################################################################
# Auto-Submit Controls
###############################################################################

def display_manual_auto_submit_controls(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session, is_training_mode: bool):
    """Display manual auto-submit controls - MINIMAL FIX for reviewer virtual responses"""
    
    # Get annotators for filtering (reviewer only)
    available_annotators = {}
    if role == "reviewer":
        try:
            available_annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
        except:
            available_annotators = {}
    
    # Persistent state keys
    virtual_responses_key = f"virtual_responses_{role}_{project_id}"
    thresholds_key = f"thresholds_{role}_{project_id}"
    selected_annotators_key = f"auto_submit_annotators_{role}_{project_id}"
    user_weights_key = f"user_weights_{role}_{project_id}"
    # MINIMAL ADDITION: Store option weights and description annotator selections separately
    option_weights_key = f"option_weights_{role}_{project_id}"  # For single choice weight adjustments
    description_selections_key = f"description_selections_{role}_{project_id}"  # For description annotator choices
    
    # Initialize state
    if virtual_responses_key not in st.session_state:
        st.session_state[virtual_responses_key] = {}
    if thresholds_key not in st.session_state:
        st.session_state[thresholds_key] = {}
    if selected_annotators_key not in st.session_state:
        st.session_state[selected_annotators_key] = []
    if user_weights_key not in st.session_state:
        st.session_state[user_weights_key] = {}
    # MINIMAL ADDITION: Initialize new state keys
    if option_weights_key not in st.session_state:
        st.session_state[option_weights_key] = {}
    if description_selections_key not in st.session_state:
        st.session_state[description_selections_key] = {}
    
    # Annotator selection with weights (reviewer only) - UNCHANGED
    if role == "reviewer" and available_annotators:
        selected_annotators = display_smart_annotator_selection_for_auto_submit(
            annotators=available_annotators, project_id=project_id
        )
        st.session_state[selected_annotators_key] = selected_annotators
        
        # Get default user weights from service
        try:
            default_weights = AuthService.get_user_weights_for_project(project_id=project_id, session=session)
        except:
            default_weights = {}
        
        # Weight controls for selected annotators
        if selected_annotators:
            st.markdown("#### ⚖️ Annotator Weights")
            custom_info("💡 Adjust weights to influence voting. Higher weights = more influence. 0 weights = no influence.")
            
            weight_cols = st.columns(min(3, len(selected_annotators)))
            
            for i, annotator_name in enumerate(selected_annotators):
                annotator_info = available_annotators.get(annotator_name, {})
                user_id_for_weight = annotator_info.get('id', '')
                
                with weight_cols[i % len(weight_cols)]:
                    current_weight = st.session_state[user_weights_key].get(
                        annotator_name, 
                        default_weights.get(user_id_for_weight, 1.0)
                    )
                    
                    display_name = annotator_name if len(annotator_name) <= 20 else f"{annotator_name[:17]}..."
                    
                    new_weight = st.number_input(
                        f"Weight for {display_name}",
                        min_value=0.0,
                        value=float(current_weight),
                        step=0.1,
                        key=f"auto_submit_weight_{annotator_name}_{project_id}",
                        disabled=is_training_mode,
                        help=f"Weight for {annotator_name}'s answers in voting"
                    )
                    
                    st.session_state[user_weights_key][annotator_name] = new_weight
    
    # Get all questions for the selected groups
    all_questions_by_group = {}
    for group in selected_groups:
        questions = get_questions_by_group_cached(group_id=group["ID"], session=session)
        all_questions_by_group[group["ID"]] = questions
    
    # Configuration interface - KEEP ORIGINAL STRUCTURE
    st.markdown("#### 🤖 Auto-Submit Configuration")    
    
    if role == "reviewer":
        st.caption("Configure all available answer options with their weights")
        
        config_tabs = st.tabs(["🎯 All Answer Options", "📊 Consensus Required"])
        
        with config_tabs[0]:
            st.markdown("##### Configure All Available Options")
            custom_info("💡 Info: All possible answer options for each question with adjustable weights")
            
            # KEEP ORIGINAL LAYOUT but fix the logic
            for group in selected_groups:
                group_id = group["ID"]
                group_display_title = group["Display Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_display_title}**")
                
                # Two column layout - UNCHANGED
                cols = st.columns(2)
                
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["display_text"]
                    
                    with cols[i % 2]:
                        short_text = question_text[:50] + "..." if len(question_text) > 50 else question_text
                        st.markdown(f"*{short_text}*")
                        
                        if question["type"] == "single":
                            # FIXED: Store option weights separately, don't create virtual responses
                            question_data = QuestionService.get_question_by_id(question_id=question_id, session=session)
                            options = question_data.get("options", [])
                            default_option_weights = question_data.get("option_weights")
                            if not default_option_weights:
                                default_option_weights = [1.0] * len(options)
                            
                            # Initialize option weights if not set
                            if question_id not in st.session_state[option_weights_key]:
                                weights_dict = {}
                                for j, option in enumerate(options):
                                    weight = default_option_weights[j] if j < len(default_option_weights) else 1.0
                                    weights_dict[option] = weight
                                st.session_state[option_weights_key][question_id] = weights_dict
                            
                            current_weights = st.session_state[option_weights_key][question_id]
                            
                            # Show options with weights - KEEP ORIGINAL UI
                            if options:
                                st.caption("**Available answer options with adjustable weights:**")
                                
                                for j, option in enumerate(options):
                                    option_col1, option_col2 = st.columns([3, 1])
                                    
                                    with option_col1:
                                        # Keep the same disabled textbox
                                        st.text_input(
                                            "Option",
                                            value=str(option),
                                            disabled=True,
                                            key=f"reviewer_opt_display_{question_id}_{j}",
                                            label_visibility="collapsed"
                                        )
                                    
                                    with option_col2:
                                        # Weight adjustment - KEEP SAME UI
                                        current_weight = current_weights.get(option, 1.0)
                                        new_weight = st.number_input(
                                            "Weight",
                                            min_value=0.0,
                                            value=float(current_weight),
                                            step=0.1,
                                            key=f"reviewer_opt_wt_{question_id}_{j}",
                                            disabled=is_training_mode,
                                            label_visibility="collapsed"
                                        )
                                        # FIXED: Store in option weights, not virtual responses
                                        st.session_state[option_weights_key][question_id][option] = new_weight
                            
                            # FIXED: Clear any virtual responses for single choice questions
                            if question_id in st.session_state[virtual_responses_key]:
                                del st.session_state[virtual_responses_key][question_id]
                        
                        else:
                            # Description type - KEEP ORIGINAL UI but fix logic
                            st.caption("**Select annotator answer to use:**")
                            
                            # KEEP ORIGINAL selectbox UI
                            try:
                                if st.session_state[selected_annotators_key]:
                                    annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                                        display_names=st.session_state[selected_annotators_key], 
                                        project_id=project_id, session=session
                                    )
                                    
                                    # Get answers from these annotators for this specific question
                                    answers_df = AnnotatorService.get_question_answers(
                                        question_id=question_id, project_id=project_id, session=session
                                    )
                                    
                                    annotator_options = ["Auto (use user weights)"]
                                    annotator_data = {}  # Store user_id -> user_name mapping
                                    
                                    if not answers_df.empty:
                                        annotator_answers = answers_df[answers_df["User ID"].isin(annotator_user_ids)]
                                        # Get unique user IDs to avoid duplicates
                                        unique_user_ids = annotator_answers["User ID"].unique()
                                        
                                        for user_id_resp in unique_user_ids:
                                            try:
                                                user_info = AuthService.get_user_info_by_id(user_id=int(user_id_resp), session=session)
                                                user_name = user_info["user_id_str"]
                                                # Apply same naming convention as get_session_cached_project_annotators
                                                display_name_with_initials, _ = AuthService.get_user_display_name_with_initials(user_name)
                                                annotator_options.append(display_name_with_initials)
                                                annotator_data[display_name_with_initials] = int(user_id_resp)
                                            except Exception as e:
                                                print(f"Error getting user info for {user_id_resp}: {e}")
                                                continue
                                    
                                    # Get current selection
                                    current_selection = st.session_state[description_selections_key].get(question_id, "Auto (use user weights)")
                                    if current_selection not in annotator_options:
                                        current_selection = "Auto (use user weights)"
                                    
                                    selected_annotator = st.selectbox(
                                        "Choose annotator answer",
                                        annotator_options,
                                        index=annotator_options.index(current_selection),
                                        key=f"desc_annotator_{question_id}",
                                        help="Select which annotator's answer to use for this description question"
                                    )
                                    
                                    # FIXED: Store selection instead of creating virtual response
                                    st.session_state[description_selections_key][question_id] = selected_annotator
                                    
                                    if selected_annotator != "Auto (use user weights)":
                                        st.success(f"✅ Using {selected_annotator}'s answer")
                                    
                                    # FIXED: Clear any virtual responses for description questions
                                    if question_id in st.session_state[virtual_responses_key]:
                                        del st.session_state[virtual_responses_key][question_id]
                                else:
                                    custom_info("Select annotators first to see their answers")
                            except Exception as e:
                                st.warning(f"Could not load annotator answers: {str(e)}")
        
        with config_tabs[1]:
            # Consensus thresholds tab - COMPLETELY UNCHANGED
            st.markdown("##### Consensus Thresholds")
            custom_info("🎯 Tip: 100% = requires full consensus, 50% = requires majority vote")
            
            for group in selected_groups:
                group_id = group["ID"]
                group_display_title = group["Display Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_display_title}**")
                
                cols = st.columns(2)
                
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["display_text"]
                    
                    if question_id not in st.session_state[thresholds_key]:
                        st.session_state[thresholds_key][question_id] = 100.0  # DEFAULT TO 100% FOR REVIEWERS TOO
                    
                    with cols[i % 2]:
                        short_text = question_text[:50] + "..." if len(question_text) > 50 else question_text
                        
                        new_threshold = st.slider(
                            short_text,
                            min_value=0.0,
                            max_value=100.0,
                            value=st.session_state[thresholds_key][question_id],
                            step=10.0,
                            key=f"reviewer_thresh_{question_id}",
                            disabled=is_training_mode,
                            format="%g%%"
                        )
                        st.session_state[thresholds_key][question_id] = new_threshold
    
    else:  # Annotator role - COMPLETELY UNCHANGED
        st.caption("Configure default answers and consensus thresholds for automated submission")
        
        config_tabs = st.tabs(["🎯 Default Answers", "📊 Consensus Required"])
        
        with config_tabs[0]:
            st.markdown("##### Set Default Answers")
            custom_info("💡 Info: Add default answers that will be used as 'votes' in the auto-submission process")
            
            for group in selected_groups:
                group_id = group["ID"]
                group_display_title = group["Display Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_display_title}**")
                
                cols = st.columns(2)
                
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["display_text"]
                    
                    if question_id not in st.session_state[virtual_responses_key]:
                        st.session_state[virtual_responses_key][question_id] = []
                    
                    virtual_responses = st.session_state[virtual_responses_key][question_id]
                    
                    with cols[i % 2]:
                        short_text = question_text[:50] + "..." if len(question_text) > 50 else question_text
                        st.markdown(f"*{short_text}*")
                        
                        # Current default answers
                        if virtual_responses:
                            for j, vr in enumerate(virtual_responses):
                                answer_col1, answer_col2 = st.columns([4, 0.5])
                                
                                with answer_col1:
                                    if question["type"] == "single":
                                        options = question["options"]
                                        display_values = question.get("display_values", options)
                                        display_to_value = dict(zip(display_values, options))
                                        value_to_display = dict(zip(options, display_values))
                                        
                                        current_display = value_to_display.get(vr["answer"], display_values[0] if display_values else "")
                                        current_index = display_values.index(current_display) if current_display in display_values else 0
                                        
                                        selected_display = st.selectbox(
                                            "Answer",
                                            display_values,
                                            index=current_index,
                                            key=f"annotator_ans_{question_id}_{j}",
                                            disabled=is_training_mode,
                                            label_visibility="collapsed"
                                        )
                                        vr["answer"] = display_to_value[selected_display]
                                    else:
                                        new_answer = st.text_input(
                                            "Answer",
                                            value=vr["answer"],
                                            key=f"annotator_ans_{question_id}_{j}",
                                            disabled=is_training_mode,
                                            label_visibility="collapsed",
                                            placeholder="Default answer..."
                                        )
                                        vr["answer"] = new_answer
                                
                                vr["user_weight"] = 1.0
                                
                                with answer_col2:
                                    if st.button("🗑️", key=f"annotator_rm_{question_id}_{j}", disabled=is_training_mode, help="Remove"):
                                        st.session_state[virtual_responses_key][question_id].pop(j)
                                        st.rerun()
                        
                        if len(virtual_responses) == 0 and st.button(f"+ Add Default", key=f"annotator_add_{question_id}", disabled=is_training_mode, use_container_width=True):
                            default_answer = question.get("default_option") or (question["options"][0] if question["type"] == "single" and question["options"] else "")
                            st.session_state[virtual_responses_key][question_id].append({
                                "answer": default_answer,
                                "user_weight": 1.0
                            })
                            st.rerun()
        
        with config_tabs[1]:
            st.markdown("##### Consensus Thresholds")
            custom_info("🎯 Tip: 100% = requires full consensus, 50% = requires majority vote")
            
            for group in selected_groups:
                group_id = group["ID"]
                group_display_title = group["Display Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_display_title}**")
                
                cols = st.columns(2)
                
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["display_text"]
                    
                    if question_id not in st.session_state[thresholds_key]:
                        st.session_state[thresholds_key][question_id] = 100.0
                    
                    with cols[i % 2]:
                        short_text = question_text[:50] + "..." if len(question_text) > 50 else question_text
                        
                        new_threshold = st.slider(
                            short_text,
                            min_value=0.0,
                            max_value=100.0,
                            value=st.session_state[thresholds_key][question_id],
                            step=10.0,
                            key=f"annotator_thresh_{question_id}",
                            disabled=is_training_mode,
                            format="%g%%"
                        )
                        st.session_state[thresholds_key][question_id] = new_threshold
    
    # Action buttons section - COMPLETELY UNCHANGED
    st.markdown("#### ⚡ Execute Auto-Submit")
    
    # Summary of current configuration
    total_selected_annotators = len(st.session_state[selected_annotators_key]) if role == "reviewer" else 0
    total_questions = sum(len(questions) for questions in all_questions_by_group.values())

    summary_text = f"📊 Configuration: {total_questions} questions"
    if role == "reviewer":
        summary_text += f" • {total_selected_annotators} annotators selected"

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #e8f5e8, #d4f1d4); border: 2px solid #28a745; border-radius: 12px; padding: 16px; margin: 16px 0; text-align: center;">
        <div style="color: #155724; font-weight: 600; font-size: 1rem;">
            {summary_text}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ACTION BUTTONS
    action_col1, action_col2, action_col3 = st.columns(3)

    with action_col1:
        if st.button("⚙️ Preload Options", 
                    key=f"preload_options_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    help="Preload options without auto-submitting"):
            run_preload_options_only(selected_groups, videos, project_id, user_id, role, session)

    with action_col2:
        if st.button("🔍 Preview Auto-Submit", 
                    key=f"preview_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    help="Preview what would be auto-submitted"):
            st.session_state[f"show_preview_{role}_{project_id}"] = True
            st.rerun()

    with action_col3:
        if st.button("🚀 Execute Auto-Submit", 
                    key=f"auto_submit_{role}_{project_id}",
                    use_container_width=True,
                    disabled=is_training_mode,
                    type="primary",
                    help="Run auto-submit with current configuration"):
            st.session_state[f"auto_submit_in_progress_{role}_{project_id}"] = True
            st.rerun()

    # Check if preview button was clicked and show preview outside of columns for full width
    if st.session_state.get(f"show_preview_{role}_{project_id}", False):
        run_preload_preview(selected_groups, videos, project_id, user_id, role, session)
        st.session_state[f"show_preview_{role}_{project_id}"] = False
    
    # Check if auto-submit button was clicked and show auto-submit outside of columns for full width
    if st.session_state.get(f"auto_submit_in_progress_{role}_{project_id}", False):
        run_manual_auto_submit(selected_groups, videos, project_id, user_id, role, session)
        st.session_state[f"auto_submit_in_progress_{role}_{project_id}"] = False


def run_manual_auto_submit(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session):
    """MINIMAL CHANGE: Use custom option weights and description selections for reviewers"""
    
    virtual_responses_key = f"virtual_responses_{role}_{project_id}"
    thresholds_key = f"thresholds_{role}_{project_id}"
    selected_annotators_key = f"auto_submit_annotators_{role}_{project_id}"
    user_weights_key = f"user_weights_{role}_{project_id}"
    option_weights_key = f"option_weights_{role}_{project_id}"
    description_selections_key = f"description_selections_{role}_{project_id}"
    
    virtual_responses_by_question = st.session_state.get(virtual_responses_key, {})
    thresholds = st.session_state.get(thresholds_key, {})
    selected_annotators = st.session_state.get(selected_annotators_key, [])
    user_weights = st.session_state.get(user_weights_key, {})
    option_weights = st.session_state.get(option_weights_key, {})
    description_selections = st.session_state.get(description_selections_key, {})
    
    # Get user IDs and weights (unchanged)
    include_user_ids = []
    user_weight_map = {}
    
    if role == "reviewer":
        try:
            available_annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            include_user_ids = annotator_user_ids
            
            for annotator_name in selected_annotators:
                try:
                    if annotator_name in available_annotators:
                        user_id_for_weight = available_annotators[annotator_name].get('id')
                        if user_id_for_weight:
                            weight = user_weights.get(annotator_name, 1.0)
                            user_weight_map[user_id_for_weight] = weight
                except:
                    continue
        except:
            include_user_ids = []
    else:
        include_user_ids = [user_id]
    
    # Progress tracking (unchanged)
    total_operations = len(selected_groups) * len(videos)
    st.markdown(f"### 🚀 Executing Auto-Submit")
    st.caption(f"Processing {len(videos)} videos across {len(selected_groups)} question groups ({total_operations} total operations)")
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    total_submitted = 0
    total_skipped = 0
    total_threshold_failures = 0
    total_verification_failures = 0
    
    threshold_failure_details = []
    verification_failure_details = []
    
    operation_count = 0
    
    for group in selected_groups:
        group_id = group["ID"]
        group_display_title = group["Display Title"]
        
        for video in videos:
            video_id = video["id"]
            video_uid = video["uid"]
            
            try:
                # MINIMAL CHANGE: Add dynamic virtual responses for this video
                dynamic_virtual_responses = virtual_responses_by_question.copy()
                video_specific_responses = build_virtual_responses_for_video(
                    video_id=video_id, project_id=project_id, role=role, session=session
                )
                dynamic_virtual_responses.update(video_specific_responses)
                
                if role == "annotator":
                    result = AutoSubmitService.auto_submit_question_group(
                        video_id=video_id, project_id=project_id, question_group_id=group_id,
                        user_id=user_id, include_user_ids=include_user_ids,
                        virtual_responses_by_question=dynamic_virtual_responses, thresholds=thresholds,
                        session=session, user_weights=user_weight_map
                    )
                else:  # reviewer - MINIMAL CHANGE: Pass custom option weights
                    result = ReviewerAutoSubmitService.auto_submit_ground_truth_group_with_custom_weights(
                        video_id=video_id, project_id=project_id, question_group_id=group_id,
                        reviewer_id=user_id, include_user_ids=include_user_ids,
                        virtual_responses_by_question=dynamic_virtual_responses, thresholds=thresholds,
                        session=session, user_weights=user_weight_map, custom_option_weights=option_weights
                    )
                
                # total_submitted += result["submitted_count"]
                # total_skipped += result["skipped_count"]
                # total_threshold_failures += result["threshold_failures"]
                # Count successful question groups, not individual questions
                if result["submitted_count"] > 0:
                    total_submitted += 1  # Count as 1 successful question group
                if result["skipped_count"] > 0:
                    total_skipped += 1
                total_threshold_failures += result["threshold_failures"]
                
                if result.get("verification_failed", False):
                    total_verification_failures += 1
                    verification_failure_details.append({
                        "group": group_display_title,
                        "video": video_uid,
                        "error": result.get("verification_error", "Unknown verification error")
                    })
                
                if "details" in result and "threshold_failures" in result["details"]:
                    for failure in result["details"]["threshold_failures"]:
                        threshold_failure_details.append({
                            "group": group_display_title,
                            "video": video_uid,
                            "question": failure["question"],
                            "percentage": failure["percentage"],
                            "threshold": failure["threshold"]
                        })
                
            except Exception as e:
                total_verification_failures += 1
                verification_failure_details.append({
                    "group": group_display_title,
                    "video": video_uid,
                    "error": str(e)
                })
            
            operation_count += 1
            progress = operation_count / total_operations
            progress_bar.progress(progress)
            status_container.text(f"Processing: {operation_count}/{total_operations}")
    
    # Results display (unchanged from original)
    st.markdown("### 📊 Auto-Submit Results")
    
    result_col1, result_col2, result_col3, result_col4 = st.columns(4)
    
    with result_col1:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #d4edda, #c3e6cb); border: 2px solid #28a745; border-radius: 12px; padding: 16px; text-align: center;">
            <div style="color: #155724; font-size: 1.8rem; font-weight: 700;">{total_submitted}</div>
            <div style="color: #155724; font-weight: 600;">✅ Submitted</div>
        </div>
        """, unsafe_allow_html=True)
    
    with result_col2:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #e2e3e5, #d1ecf1); border: 2px solid #6c757d; border-radius: 12px; padding: 16px; text-align: center;">
            <div style="color: #495057; font-size: 1.8rem; font-weight: 700;">{total_skipped}</div>
            <div style="color: #495057; font-weight: 600;">⏭️ Skipped</div>
        </div>
        """, unsafe_allow_html=True)
    
    with result_col3:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border: 2px solid #ffc107; border-radius: 12px; padding: 16px; text-align: center;">
            <div style="color: #856404; font-size: 1.8rem; font-weight: 700;">{total_threshold_failures}</div>
            <div style="color: #856404; font-weight: 600;">🎯 Threshold</div>
        </div>
        """, unsafe_allow_html=True)
    
    with result_col4:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #f8d7da, #f1b0b7); border: 2px solid #dc3545; border-radius: 12px; padding: 16px; text-align: center;">
            <div style="color: #721c24; font-size: 1.8rem; font-weight: 700;">{total_verification_failures}</div>
            <div style="color: #721c24; font-weight: 600;">❌ Errors</div>
        </div>
        """, unsafe_allow_html=True)
    
    if total_submitted > 0:
        st.success(f"🎉 Successfully auto-submitted {total_submitted} question groups!")
    
    # Failure details (unchanged from original)
    if total_threshold_failures > 0 and threshold_failure_details:
        with st.expander(f"🎯 Threshold Failure Details ({total_threshold_failures} failures)", expanded=False):
            for failure in threshold_failure_details[:10]:
                st.markdown(f"""
                <div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 8px 12px; margin: 4px 0; border-radius: 4px;">
                    <strong>{failure['group']} - {failure['video']}</strong><br>
                    Question: {failure['question']}<br>
                    Achieved: {failure['percentage']:.1f}% | Required: {failure['threshold']:.1f}%
                </div>
                """, unsafe_allow_html=True)
            
            if len(threshold_failure_details) > 10:
                st.caption(f"... and {len(threshold_failure_details) - 10} more threshold failures")
    
    if total_verification_failures > 0 and verification_failure_details:
        with st.expander(f"❌ Error Details ({total_verification_failures} errors)", expanded=False):
            for failure in verification_failure_details[:10]:
                st.markdown(f"""
                <div style="background: #f8d7da; border-left: 4px solid #dc3545; padding: 8px 12px; margin: 4px 0; border-radius: 4px;">
                    <strong>{failure['group']} - {failure['video']}</strong><br>
                    Error: {failure['error']}
                </div>
                """, unsafe_allow_html=True)
            
            if len(verification_failure_details) > 10:
                st.caption(f"... and {len(verification_failure_details) - 10} more errors")

def run_preload_preview(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session):
    """MINIMAL CHANGE: Use custom option weights and description selections for preview"""
    
    virtual_responses_key = f"virtual_responses_{role}_{project_id}"
    thresholds_key = f"thresholds_{role}_{project_id}"
    selected_annotators_key = f"auto_submit_annotators_{role}_{project_id}"
    user_weights_key = f"user_weights_{role}_{project_id}"
    option_weights_key = f"option_weights_{role}_{project_id}"
    
    virtual_responses_by_question = st.session_state.get(virtual_responses_key, {})
    thresholds = st.session_state.get(thresholds_key, {})
    selected_annotators = st.session_state.get(selected_annotators_key, [])
    user_weights = st.session_state.get(user_weights_key, {})
    option_weights = st.session_state.get(option_weights_key, {})
    
    # Get user IDs (unchanged)
    include_user_ids = []
    user_weight_map = {}
    
    if role == "reviewer":
        try:
            available_annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
            include_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            for annotator_name in selected_annotators:
                try:
                    if annotator_name in available_annotators:
                        user_id_for_weight = available_annotators[annotator_name].get('id')
                        if user_id_for_weight:
                            weight = user_weights.get(annotator_name, 1.0)
                            user_weight_map[user_id_for_weight] = weight
                except:
                    continue
        except:
            include_user_ids = []
    else:
        include_user_ids = [user_id]
    
    # BREAK OUT OF COLUMN LAYOUT
    st.markdown("---")
    
    with st.container():
        st.markdown("### 🔍 Auto-Submit Preview")
        st.caption("Preview showing what would happen with your current configuration")
        
        groups_would_submit = 0
        groups_would_skip = 0
        
        if len(videos) > 10:
            progress_bar = st.progress(0)
            status_text = st.empty()
        
        video_results = {}
        
        for video_idx, video in enumerate(videos):
            video_id = video["id"]
            video_uid = video["uid"]
            video_results[video_uid] = {
                "video_id": video_id,
                "groups": {}
            }
            
            if len(videos) > 10:
                progress = (video_idx + 1) / len(videos)
                progress_bar.progress(progress)
                status_text.text(f"Processing video {video_idx + 1}/{len(videos)}: {video_uid}")
            
            for group in selected_groups:
                group_id = group["ID"]
                group_display_title = group["Display Title"]
                
                try:
                    questions_in_group = get_questions_by_group_cached(group_id=group_id, session=session)
                    total_questions_in_group = len(questions_in_group)

                    # MINIMAL CHANGE: Add dynamic virtual responses for this video
                    dynamic_virtual_responses = virtual_responses_by_question.copy()
                    video_specific_responses = build_virtual_responses_for_video(
                        video_id=video_id, project_id=project_id, role=role, session=session
                    )
                    dynamic_virtual_responses.update(video_specific_responses)
                    
                    if role == "reviewer":
                        result = ReviewerAutoSubmitService.calculate_auto_submit_ground_truth_with_custom_weights(
                            video_id=video_id, project_id=project_id, question_group_id=group_id,
                            include_user_ids=include_user_ids, virtual_responses_by_question=dynamic_virtual_responses,
                            thresholds=thresholds, session=session, user_weights=user_weight_map,
                            custom_option_weights=option_weights
                        )
                    else:
                        result = AutoSubmitService.calculate_auto_submit_answers(
                            video_id=video_id, project_id=project_id, question_group_id=group_id,
                            include_user_ids=include_user_ids, virtual_responses_by_question=dynamic_virtual_responses,
                            thresholds=thresholds, session=session
                        )
                    
                    # Rest of the logic unchanged
                    answers_count = len(result["answers"])
                    threshold_failures = len(result["threshold_failures"])
                    skipped_count = len(result.get("skipped", []))
                    verification_failures = 0

                    # Check verification function if it exists
                    if result["answers"]:
                        try:
                            group_details = QuestionGroupService.get_group_details_with_verification(
                                group_id=group_id, session=session
                            )
                            verification_function = group_details.get("verification_function")
                            
                            if verification_function:
                                import verify
                                verify_func = getattr(verify, verification_function, None)
                                if verify_func:
                                    verify_func(result["answers"])
                        except Exception as e:
                            verification_failures = 1
                    
                    group_would_submit = (answers_count == total_questions_in_group) and (threshold_failures == 0) and (verification_failures == 0) and (skipped_count == 0)
                    
                    if group_would_submit:
                        groups_would_submit += 1
                        video_results[video_uid]["groups"][group_display_title] = {
                            "would_submit": True,
                            "answers": result["answers"],
                            "vote_details": result["vote_details"],
                            "error": None,
                            "details": f"✅ All {total_questions_in_group} questions would be submitted"
                        }
                    else:
                        groups_would_skip += 1

                        if skipped_count > 0:
                            failure_reason = f"already completed"
                        elif verification_failures > 0:
                            failure_reason = f"verification failed"
                        else:
                            failure_parts = []
                            missing_answers = total_questions_in_group - answers_count - threshold_failures
                            if missing_answers > 0:
                                failure_parts.append(f"missing answers")
                            if threshold_failures > 0:
                                failure_parts.append(f"threshold failures")
                            
                            failure_reason = " + ".join(failure_parts) if failure_parts else "unknown failure"
                        
                        video_results[video_uid]["groups"][group_display_title] = {
                            "would_submit": False,
                            "answers": result["answers"] if result["answers"] else {},
                            "vote_details": result["vote_details"] if result["vote_details"] else {},
                            "error": None,
                            "details": f"❌ Group failed: {failure_reason}"
                        }
                    
                except Exception as e:
                    groups_would_skip += 1
                    try:
                        questions_in_group = get_questions_by_group_cached(group_id=group_id, session=session)
                        total_questions_in_group = len(questions_in_group)
                    except:
                        total_questions_in_group = 0
                    
                    video_results[video_uid]["groups"][group_display_title] = {
                        "would_submit": False,
                        "answers": {},
                        "vote_details": {},
                        "error": str(e),
                        "details": f"❌ Error occurred (0/{total_questions_in_group} questions)"
                    }
        
        if len(videos) > 10:
            progress_bar.empty()
            status_text.empty()
        
        # Rest of the display logic unchanged from original
        if video_results:
            total_group_operations = len(videos) * len(selected_groups)
            success_rate = (groups_would_submit / total_group_operations * 100) if total_group_operations > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("✅ Groups Would Submit", groups_would_submit)
            with col2:
                st.metric("⏭️ Groups Would Skip", groups_would_skip)
            with col3:
                st.metric("📊 Success Rate", f"{success_rate:.1f}%")
            
            custom_info(f"📊 **Calculation:** {len(videos)} videos × {len(selected_groups)} question groups = {total_group_operations} group operations. Success rate = {groups_would_submit}/{total_group_operations} = {success_rate:.1f}%")
            
            st.markdown("#### 📋 Detailed Preview Results (First 5 Videos)")
            
            displayed_videos = list(video_results.keys())[:5]
            
            for video_uid in displayed_videos:
                video_data = video_results[video_uid]
                
                groups_would_submit_video = sum(1 for group_data in video_data["groups"].values() if group_data["would_submit"])
                groups_would_skip_video = sum(1 for group_data in video_data["groups"].values() if not group_data["would_submit"])
                video_total_groups = len(video_data["groups"])
                
                video_status_color = COLORS['success'] if groups_would_submit_video > 0 else COLORS['warning']
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, {video_status_color}20, {video_status_color}10); border: 2px solid {video_status_color}; border-radius: 12px; padding: 16px; margin: 16px 0;">
                    <div style="color: {video_status_color}; font-weight: 700; font-size: 1.2rem; margin-bottom: 8px;">
                        📹 Video: {video_uid}
                    </div>
                    <div style="color: #495057; font-size: 0.95rem;">
                        📊 Total Groups: {video_total_groups} | Would Submit: {groups_would_submit_video} | Would Skip: {groups_would_skip_video}
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                group_cols = st.columns(min(3, len(video_data["groups"])))
                
                for i, (group_title, group_data) in enumerate(video_data["groups"].items()):
                    with group_cols[i % len(group_cols)]:
                        if group_data["error"]:
                            st.error(f"❌ **{group_title}**: {group_data['error']}")
                        else:
                            if group_data['would_submit']:
                                status_color = COLORS['success']
                                status_text = "✅ Will Submit"
                                status_icon = "📋"
                            else:
                                details = group_data["details"].lower()
                                if "verification failures" in details:
                                    status_color = COLORS['danger']
                                    status_icon = "🛡️"
                                    status_text = "🛡️ Verification Failed"
                                elif "threshold failures" in details:
                                    status_color = COLORS['warning']
                                    status_icon = "🎯"
                                    status_text = "🎯 Threshold Failed"
                                elif "missing answers" in details:
                                    status_color = COLORS['info']
                                    status_icon = "📝"
                                    status_text = "📝 Missing Answers"
                                else:
                                    status_color = COLORS['secondary']
                                    status_icon = "⏭️"
                                    status_text = "⏭️ Will Skip"
                            
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, {status_color}15, {status_color}08); border-left: 4px solid {status_color}; border-radius: 8px; padding: 12px; margin: 8px 0;">
                                <div style="color: {status_color}; font-weight: 600; margin-bottom: 4px;">
                                    {status_icon} {group_title}
                                </div>
                                <div style="color: #495057; font-size: 0.9rem;">
                                    {status_text}
                                </div>
                                <div style="color: #6c757d; font-size: 0.8rem; margin-top: 4px;">
                                    {group_data["details"]}
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            if group_data["would_submit"] and group_data["answers"] and displayed_videos.index(video_uid) < 3:
                                with st.expander(f"👁️ Preview answers for {group_title}", expanded=False):
                                    for question, answer in group_data["answers"].items():
                                        st.write(f"**{question[:60]}{'...' if len(question) > 60 else ''}**: {answer}")
                                        
                                        if question in group_data["vote_details"]:
                                            vote_details = group_data["vote_details"][question]
                                            if vote_details:
                                                total_weight = sum(vote_details.values())
                                                st.caption("**Vote breakdown:**")
                                                for option, weight in vote_details.items():
                                                    percentage = (weight / total_weight * 100) if total_weight > 0 else 0
                                                    st.caption(f"  • {option}: {weight:.2f} weight ({percentage:.1f}%)")
                
                if displayed_videos.index(video_uid) < len(displayed_videos) - 1:
                    st.markdown("---")
            
            if len(videos) > 5:
                custom_info(f"📊 Detailed view shows first 5 videos. All {len(videos)} videos were processed for the summary statistics above.")
        else:
            st.warning("No preview results available")

def run_preload_options_only(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session):
    """Preload options without auto-submitting - REVERTED to original for annotators, fixed for reviewers"""
    
    virtual_responses_key = f"virtual_responses_{role}_{project_id}"
    thresholds_key = f"thresholds_{role}_{project_id}"
    selected_annotators_key = f"auto_submit_annotators_{role}_{project_id}"
    user_weights_key = f"user_weights_{role}_{project_id}"
    
    virtual_responses_by_question = st.session_state.get(virtual_responses_key, {})
    thresholds = st.session_state.get(thresholds_key, {})
    selected_annotators = st.session_state.get(selected_annotators_key, [])
    user_weights = st.session_state.get(user_weights_key, {})
    
    # Get user IDs and weights
    include_user_ids = []
    user_weight_map = {}
    
    if role == "reviewer":
        try:
            available_annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
            annotator_user_ids = AuthService.get_annotator_user_ids_from_display_names(
                display_names=selected_annotators, project_id=project_id, session=session
            )
            include_user_ids = annotator_user_ids
            
            # Map user weights
            for annotator_name in selected_annotators:
                try:
                    if annotator_name in available_annotators:
                        user_id_for_weight = available_annotators[annotator_name].get('id')
                        if user_id_for_weight:
                            weight = user_weights.get(annotator_name, 1.0)
                            user_weight_map[user_id_for_weight] = weight
                except:
                    continue
        except:
            include_user_ids = []
    else:
        include_user_ids = [user_id]
    
    if not include_user_ids or not selected_groups:
        st.warning("Please configure annotators and question groups first.")
        return
    
    # Calculate winning answers for each video and group
    preloaded_answers_dict = {}
    total_preloaded = 0
    
    for group in selected_groups:
        group_id = group["ID"]
        
        for video in videos:
            video_id = video["id"]
            
            try:
                if role == "reviewer":
                    # FIXED: Use dynamic virtual responses for reviewers
                    dynamic_virtual_responses = virtual_responses_by_question.copy()
                    
                    # Get custom settings for reviewers
                    option_weights_key = f"option_weights_{role}_{project_id}"
                    description_selections_key = f"description_selections_{role}_{project_id}"
                    option_weights = st.session_state.get(option_weights_key, {})
                    description_selections = st.session_state.get(description_selections_key, {})
                    
                    # Add dynamic virtual responses for description questions
                    for question_id, selected_annotator in description_selections.items():
                        if selected_annotator != "Auto (use user weights)":
                            try:
                                # Get the specific annotator's answer for this video
                                annotator_info = available_annotators.get(selected_annotator, {})
                                annotator_user_id = annotator_info.get('id')
                                
                                if annotator_user_id:
                                    # Get this annotator's answer for this specific video and question
                                    answers_df = AnnotatorService.get_question_answers(
                                        question_id=question_id, project_id=project_id, session=session
                                    )
                                    
                                    if not answers_df.empty:
                                        user_video_answers = answers_df[
                                            (answers_df["User ID"] == annotator_user_id) & 
                                            (answers_df["Video ID"] == video_id)
                                        ]
                                        
                                        if not user_video_answers.empty:
                                            answer_text = user_video_answers.iloc[0]["Answer Value"]
                                            
                                            # Create virtual response for this specific video
                                            dynamic_virtual_responses[question_id] = [{
                                                "answer": answer_text,
                                                "user_weight": 1.0
                                            }]
                            except Exception as e:
                                print(f"Error getting answer for question {question_id} from {selected_annotator}: {e}")
                                continue
                    
                    # Calculate answers with custom option weights
                    result_answers = calculate_preload_with_weights(
                        video_id=video_id, project_id=project_id, question_group_id=group_id,
                        include_user_ids=include_user_ids, virtual_responses_by_question=dynamic_virtual_responses,
                        session=session, user_weights=user_weight_map, option_weights=option_weights
                    )
                else:
                    # REVERTED: Original logic for annotators (no dynamic virtual responses)
                    result_answers = calculate_preload_answers_no_threshold(
                        video_id=video_id, project_id=project_id, question_group_id=group_id,
                        include_user_ids=include_user_ids, virtual_responses_by_question=virtual_responses_by_question,
                        session=session, user_weights=user_weight_map, role=role
                    )
                
                # Store winning answers for passing to forms
                for question_text, answer_value in result_answers.items():
                    key = (video_id, group_id, question_text)
                    preloaded_answers_dict[key] = answer_value
                    total_preloaded += 1
                
            except Exception as e:
                print(f"Error processing video {video['id']}, group {group_id}: {e}")
                continue
    
    # Store in session state for the display functions to access
    st.session_state[f"current_preloaded_answers_{role}_{project_id}"] = preloaded_answers_dict
    
    # SUCCESS MESSAGE
    if total_preloaded > 0:
        st.success(f"✅ Preloaded {total_preloaded} default answers for forms!")
        custom_info("💡 The calculated answers will now appear as defaults in the question forms below.")
        
        # FORCE RERUN OF ENTIRE PAGE TO PROPAGATE TO ALL FRAGMENTS
        import time
        time.sleep(0.1)  # Small delay to ensure state is saved
        st.rerun()
    else:
        st.warning("⚠️ No answers could be preloaded. Check your configuration.")


###############################################################################
# Auto-Submit Calculation Functions
###############################################################################


def calculate_preload_answers_no_threshold(video_id: int, project_id: int, question_group_id: int, 
                                         include_user_ids: List[int], virtual_responses_by_question: Dict,
                                         session: Session, user_weights: Dict[int, float] = None, role: str = "annotator") -> Dict[str, str]:
    """Calculate preload answers WITHOUT threshold requirements - REVERTED to original for annotators"""
    
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return {}
        
        preload_answers = {}

        # Get cached data if this is for a reviewer with selected annotators
        cache_data = None
        if role == "reviewer" and include_user_ids:
            session_id = get_session_cache_key()
            cache_data = get_cached_video_reviewer_data(
                video_id=video_id, project_id=project_id, 
                annotator_user_ids=include_user_ids, session_id=session_id
            )
        
        for question in questions:
            question_id = question["id"]
            question_text = question["display_text"]
            question_type = question["type"]
            
            if question_type == "single":
                # REVERTED: Use original AutoSubmitService logic for single choice
                vote_counts = {}

                # OPTIMIZED: Use cached data if available
                if cache_data and question_id in cache_data.get("annotator_answers", {}):
                    # Use cached answers
                    for answer_record in cache_data["annotator_answers"][question_id]:
                        answer_value = answer_record["Answer Value"]
                        user_id = answer_record["User ID"]
                        user_weight = user_weights.get(user_id, 1.0) if user_weights else 1.0
                        
                        if answer_value not in vote_counts:
                            vote_counts[answer_value] = 0.0
                        vote_counts[answer_value] += user_weight
                else:
                    # Fallback to original method
                
                    # Add annotator votes
                    if include_user_ids:
                        try:
                            answers_df = AnnotatorService.get_question_answers(
                                question_id=question_id, project_id=project_id, session=session
                            )
                            
                            if not answers_df.empty:
                                video_answers = answers_df[
                                    (answers_df["Video ID"] == video_id) & 
                                    (answers_df["User ID"].isin(include_user_ids))
                                ]
                                
                                for _, answer_row in video_answers.iterrows():
                                    answer_value = answer_row["Answer Value"]
                                    user_id = answer_row["User ID"]
                                    user_weight = user_weights.get(user_id, 1.0) if user_weights else 1.0
                                    
                                    if answer_value not in vote_counts:
                                        vote_counts[answer_value] = 0.0
                                    vote_counts[answer_value] += user_weight
                        except Exception:
                            pass
                
                # Add virtual responses (default answers configured)
                virtual_responses = virtual_responses_by_question.get(question_id, [])
                for vr in virtual_responses:
                    answer_value = vr["answer"]
                    weight = vr["user_weight"]
                    
                    if answer_value not in vote_counts:
                        vote_counts[answer_value] = 0.0
                    vote_counts[answer_value] += weight
                
                # Pick highest weighted option (NO THRESHOLD CHECK!)
                if vote_counts:
                    winning_answer = max(vote_counts.keys(), key=lambda x: vote_counts[x])
                    preload_answers[question_text] = winning_answer
                
            elif question_type == "description":
                # REVERTED: Original logic for description questions
                virtual_responses = virtual_responses_by_question.get(question_id, [])
                
                if virtual_responses:
                    # Use the configured answer from dropdown (ignore annotator weights for description)
                    preload_answers[question_text] = virtual_responses[0]["answer"]
                else:
                    # Fall back to weight-based selection only if no dropdown selection made
                    # OPTIMIZED: Use cached data for description questions
                    if cache_data and question_id in cache_data.get("text_answers", {}):
                        # Use cached text answers
                        answer_scores = {}
                        for answer_info in cache_data["text_answers"][question_id]:
                            answer_value = answer_info['answer_value']
                            # Find user weight from answer info
                            user_name = answer_info['name']
                            user_weight = 1.0
                            if user_weights:
                                # Find user ID from name
                                for uid in include_user_ids:
                                    if cache_data["user_info"].get(uid, {}).get("name") == user_name:
                                        user_weight = user_weights.get(uid, 1.0)
                                        break
                            
                            if answer_value not in answer_scores:
                                answer_scores[answer_value] = 0.0
                            answer_scores[answer_value] += user_weight
                    else:
                        # Fallback to original method
                    
                        answer_scores = {}
                        
                        # Add annotator answers
                        if include_user_ids:
                            try:
                                answers_df = AnnotatorService.get_question_answers(
                                    question_id=question_id, project_id=project_id, session=session
                                )
                                
                                if not answers_df.empty:
                                    video_answers = answers_df[
                                        (answers_df["Video ID"] == video_id) & 
                                        (answers_df["User ID"].isin(include_user_ids))
                                    ]
                                    
                                    for _, answer_row in video_answers.iterrows():
                                        answer_value = answer_row["Answer Value"]
                                        user_id = answer_row["User ID"]
                                        user_weight = user_weights.get(user_id, 1.0) if user_weights else 1.0
                                        
                                        if answer_value not in answer_scores:
                                            answer_scores[answer_value] = 0.0
                                        answer_scores[answer_value] += user_weight
                            except Exception:
                                pass
                    
                    # Pick highest weighted answer (NO THRESHOLD CHECK!)
                    if answer_scores:
                        winning_answer = max(answer_scores.keys(), key=lambda x: answer_scores[x])
                        preload_answers[question_text] = winning_answer
        
        return preload_answers
        
    except Exception:
        return {}



def calculate_preload_with_weights(
    video_id: int, project_id: int, question_group_id: int, 
    include_user_ids: List[int], virtual_responses_by_question: Dict,
    session: Session, user_weights: Dict[int, float] = None, 
    option_weights: Dict[int, Dict[str, float]] = None
) -> Dict[str, str]:
    """Calculate preload answers for reviewers with custom option weights"""
    
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return {}
        
        preload_answers = {}

        cache_data = None
        if include_user_ids:
            session_id = get_session_cache_key()
            cache_data = get_cached_video_reviewer_data(
                video_id=video_id, project_id=project_id, 
                annotator_user_ids=include_user_ids, session_id=session_id
            )
        
        for question in questions:
            question_id = question["id"]
            question_text = question["display_text"]
            question_type = question["type"]
            
            if question_type == "single":
                # Get custom option weights for this question
                question_custom_weights = option_weights.get(question_id, {}) if option_weights else {}

                # OPTIMIZED: Build vote counts from cache
                vote_counts = {}
                
                if cache_data and question_id in cache_data.get("annotator_answers", {}):
                    # Use cached answers
                    for answer_record in cache_data["annotator_answers"][question_id]:
                        answer_value = answer_record["Answer Value"]
                        user_id = answer_record["User ID"]
                        
                        # Get user weight
                        user_weight = user_weights.get(user_id, 1.0) if user_weights else 1.0
                        
                        # Get option weight
                        option_weight = 1.0
                        if question_custom_weights and answer_value in question_custom_weights:
                            option_weight = float(question_custom_weights[answer_value])
                        elif question["option_weights"]:
                            try:
                                option_index = question["options"].index(answer_value)
                                option_weight = float(question["option_weights"][option_index])
                            except (ValueError, IndexError):
                                option_weight = 1.0
                        
                        # Combined weight
                        combined_weight = user_weight * option_weight
                        vote_counts[answer_value] = vote_counts.get(answer_value, 0.0) + combined_weight
                else:
                    # Fallback to service call if no cache
                
                    # Get all votes for this question (annotator + virtual responses)
                    vote_counts = ReviewerAutoSubmitService.get_weighted_votes_for_question_with_custom_weights(
                        video_id=video_id, project_id=project_id, question_id=question_id,
                        include_user_ids=include_user_ids, 
                        virtual_responses=virtual_responses_by_question.get(question_id, []),
                        session=session, user_weights=user_weights,
                        custom_option_weights=question_custom_weights if question_custom_weights else None,
                        cache_data=cache_data
                    )
                    
                # Pick highest weighted option (NO THRESHOLD CHECK!)
                if vote_counts:
                    winning_answer = max(vote_counts.keys(), key=lambda x: vote_counts[x])
                    preload_answers[question_text] = winning_answer
                
            elif question_type == "description":
                # For description questions, prioritize virtual responses (from selected annotator)
                virtual_responses = virtual_responses_by_question.get(question_id, [])
                
                if virtual_responses:
                    # Use the configured answer (from selected annotator for this video)
                    preload_answers[question_text] = virtual_responses[0]["answer"]
                else:
                    # Fall back to weight-based selection
                    # OPTIMIZED: Use cached text answers
                    if cache_data and question_id in cache_data.get("text_answers", {}):
                        answer_scores = {}
                        for answer_info in cache_data["text_answers"][question_id]:
                            answer_value = answer_info['answer_value']
                            # Find user weight
                            user_name = answer_info['name']
                            user_weight = 1.0
                            if user_weights:
                                for uid in include_user_ids:
                                    if cache_data["user_info"].get(uid, {}).get("name") == user_name:
                                        user_weight = user_weights.get(uid, 1.0)
                                        break
                            
                            if answer_value not in answer_scores:
                                answer_scores[answer_value] = 0.0
                            answer_scores[answer_value] += user_weight
                    else:
                        # Fallback to original method
                        answer_scores = {}
                        
                        # Add annotator answers
                        if include_user_ids:
                            try:
                                answers_df = AnnotatorService.get_question_answers(
                                    question_id=question_id, project_id=project_id, session=session
                                )
                                
                                if not answers_df.empty:
                                    video_answers = answers_df[
                                        (answers_df["Video ID"] == video_id) & 
                                        (answers_df["User ID"].isin(include_user_ids))
                                    ]
                                    
                                    for _, answer_row in video_answers.iterrows():
                                        answer_value = answer_row["Answer Value"]
                                        user_id = answer_row["User ID"]
                                        user_weight = user_weights.get(user_id, 1.0) if user_weights else 1.0
                                        
                                        if answer_value not in answer_scores:
                                            answer_scores[answer_value] = 0.0
                                        answer_scores[answer_value] += user_weight
                            except Exception:
                                pass
                    
                    # Pick highest weighted answer (NO THRESHOLD CHECK!)
                    if answer_scores:
                        winning_answer = max(answer_scores.keys(), key=lambda x: answer_scores[x])
                        preload_answers[question_text] = winning_answer
        
        return preload_answers
        
    except Exception as e:
        print(f"Error in calculate_preload_with_weights: {e}")
        return {}



def build_virtual_responses_for_video(video_id: int, project_id: int, role: str, session: Session) -> Dict[int, List[Dict]]:
    """
    Build virtual responses for a specific video, using stored option weights and description selections.
    OPTIMIZED: Uses cached data for description questions
    """
    if role != "reviewer":
        return {}  # Use original virtual_responses for annotators
    
    option_weights_key = f"option_weights_{role}_{project_id}"
    description_selections_key = f"description_selections_{role}_{project_id}"
    selected_annotators_key = f"auto_submit_annotators_{role}_{project_id}"
    
    option_weights = st.session_state.get(option_weights_key, {})
    description_selections = st.session_state.get(description_selections_key, {})
    selected_annotators = st.session_state.get(selected_annotators_key, [])
    
    virtual_responses = {}
    
    # Get cached data if available
    cache_data = None
    if selected_annotators and description_selections:
        annotator_user_ids = get_optimized_annotator_user_ids(
            display_names=selected_annotators, project_id=project_id, session=session
        )
        if annotator_user_ids:
            session_id = get_session_cache_key()
            cache_data = get_cached_video_reviewer_data(
                video_id=video_id, project_id=project_id, 
                annotator_user_ids=annotator_user_ids, session_id=session_id
            )
    
    # Handle description questions with specific annotator selections
    for question_id, selected_annotator in description_selections.items():
        if selected_annotator == "Auto (use user weights)":
            continue  # Use weighted voting
        
        try:
            # OPTIMIZED: Use cached data instead of querying
            if cache_data and cache_data.get("text_answers", {}).get(question_id):
                # Find the answer from the selected annotator in cache
                for answer_info in cache_data["text_answers"][question_id]:
                    if answer_info['name'] == selected_annotator or f"{answer_info['name']} ({answer_info['initials']})" == selected_annotator:
                        virtual_responses[question_id] = [{
                            "answer": answer_info['answer_value'],
                            "user_weight": 1.0
                        }]
                        break
            else:
                # Fallback to original method if cache miss
                available_annotators = get_session_cached_project_annotators(project_id=project_id, session=session)
                annotator_info = available_annotators.get(selected_annotator, {})
                annotator_user_id = annotator_info.get('id')
                
                if annotator_user_id:
                    answers_df = AnnotatorService.get_question_answers(
                        question_id=question_id, project_id=project_id, session=session
                    )
                    
                    if not answers_df.empty:
                        user_video_answers = answers_df[
                            (answers_df["User ID"] == annotator_user_id) & 
                            (answers_df["Video ID"] == video_id)
                        ]
                        
                        if not user_video_answers.empty:
                            answer_text = user_video_answers.iloc[0]["Answer Value"]
                            virtual_responses[question_id] = [{
                                "answer": answer_text,
                                "user_weight": 1.0
                            }]
        except Exception as e:
            print(f"Error getting answer for question {question_id}: {e}")
    
    return virtual_responses



def run_project_wide_auto_submit_on_entry(project_id: int, user_id: int, session: Session):
    """Run auto-submit for all auto-submit groups across entire project when user first enters - OPTIMIZED"""
    
    try:
        # Get all data in fewer queries
        project = ProjectService.get_project_dict_by_id(project_id=project_id, session=session)
        question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
        videos = get_project_videos(project_id=project_id, session=session)
        
        # Find auto-submit groups first
        auto_submit_groups = []
        for group in question_groups:
            try:
                group_details = QuestionGroupService.get_group_details_with_verification(
                    group_id=group["ID"], session=session
                )
                if group_details.get("is_auto_submit", False):
                    auto_submit_groups.append(group)
            except:
                continue
        
        if not auto_submit_groups:
            return  # No auto-submit groups, exit early
        
        # Batch check existing answers for all groups at once to reduce queries
        existing_answers_cache = {}
        for group in auto_submit_groups:
            try:
                user_answers = AnnotatorService.get_user_answers_for_question_group(
                    video_id=None, project_id=project_id, user_id=user_id, 
                    question_group_id=group["ID"], session=session
                )
                existing_answers_cache[group["ID"]] = user_answers
            except:
                existing_answers_cache[group["ID"]] = {}
        
        # Process in smaller batches to avoid overwhelming the connection pool
        batch_size = 5  # Process 5 videos at a time
        
        for i in range(0, len(videos), batch_size):
            video_batch = videos[i:i + batch_size]
            
            for video in video_batch:
                for group in auto_submit_groups:
                    try:
                        # Check cache first to avoid repeated queries
                        cached_answers = existing_answers_cache.get(group["ID"], {})
                        
                        # Check if this specific video has answers
                        has_answers = False
                        try:
                            video_answers = AnnotatorService.get_user_answers_for_question_group(
                                video_id=video["id"], project_id=project_id, user_id=user_id, 
                                question_group_id=group["ID"], session=session
                            )
                            if video_answers and any(answer.strip() for answer in video_answers.values() if answer):
                                has_answers = True
                        except Exception as e:
                            print(f"Error getting user answers: {e}")
                            pass
                        
                        if has_answers:
                            continue
                        
                        # Get questions once per group
                        questions = get_questions_by_group_cached(group_id=group["ID"], session=session)
                        if not questions:
                            continue
                        
                        # Create virtual responses
                        virtual_responses_by_question = {}
                        for question in questions:
                            question_id = question["id"]
                            
                            if question["type"] == "single":
                                if question.get("default_option"):
                                    default_answer = question["default_option"]
                                elif question.get("options") and len(question["options"]) > 0:
                                    default_answer = question["options"][0]
                                else:
                                    continue  # Skip invalid questions
                            else:
                                default_answer = question.get("default_option", "Auto-generated response")
                            
                            virtual_responses_by_question[question_id] = [{
                                "name": "System Default",
                                "answer": default_answer,
                                "user_weight": 1.0
                            }]
                        
                        if not virtual_responses_by_question:
                            continue
                        
                        thresholds = {q["id"]: 100.0 for q in questions}
                        
                        # Auto-submit with error handling
                        try:
                            AutoSubmitService.auto_submit_question_group(
                                video_id=video["id"], project_id=project_id, question_group_id=group["ID"],
                                user_id=user_id, include_user_ids=[user_id],
                                virtual_responses_by_question=virtual_responses_by_question, thresholds=thresholds,
                                session=session
                            )
                        except Exception as submit_error:
                            # Log error but continue with other videos/groups
                            print(f"Auto-submit failed for video {video['id']}, group {group['ID']}: {submit_error}")
                            continue
                        
                    except Exception as group_error:
                        print(f"Error processing group {group.get('ID', 'unknown')}: {group_error}")
                        continue
            
            # Small delay between batches to prevent overwhelming the connection pool
            import time
            time.sleep(0.1)
                        
    except Exception as e:
        pass  # Silently fail to not disrupt user experience


###############################################################################
# Annotator Selection Functions
###############################################################################

def display_smart_annotator_selection_for_auto_submit(annotators: Dict[str, Dict], project_id: int):
    """Annotator selection for auto-submit - only completed annotators"""
    if not annotators:
        custom_info("No annotators have submitted answers for this project yet.")
        return []
    
    # Check completion status for each annotator
    # Use cached completion progress instead of individual calls
    try:
        with get_db_session() as session:
            completion_progress = get_cached_user_completion_progress(project_id=project_id, session=session)
            
            completed_annotators = {}
            for annotator_display, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id and user_id in completion_progress:
                    progress = completion_progress[user_id]
                    if progress >= 100:
                        completed_annotators[annotator_display] = annotator_info
    except:
        completed_annotators = {}
    
    auto_submit_key = f"auto_submit_annotators_{project_id}"
    if auto_submit_key not in st.session_state:
        st.session_state[auto_submit_key] = list(completed_annotators.keys())
    
    with st.container():
        st.markdown("#### 👥 Select Completed Annotators for Auto-Submit")
        st.caption("Only annotators who completed the entire project can be used for auto-submit")
        
        if not completed_annotators:
            custom_info("No completed annotators available for auto-submit.")
            return []
        
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            if st.button("✅ Select All Completed", key=f"select_all_auto_submit_{project_id}", use_container_width=True):
                st.session_state[auto_submit_key] = list(completed_annotators.keys())
                st.rerun()
        
        with btn_col2:
            if st.button("❌ Clear All", key=f"clear_all_auto_submit_{project_id}", use_container_width=True):
                st.session_state[auto_submit_key] = []
                st.rerun()
        
        # Selection checkboxes
        updated_selection = []
        display_annotator_checkboxes(completed_annotators, project_id, "auto_submit", updated_selection, disabled=False)
        
        if set(updated_selection) != set(st.session_state[auto_submit_key]):
            st.session_state[auto_submit_key] = updated_selection
            st.rerun()
        
        # Status display
        selected_count = len(st.session_state[auto_submit_key])
        total_count = len(completed_annotators)
        
        status_color = COLORS['success'] if selected_count > 0 else COLORS['secondary']
        status_text = f"📊 {selected_count} selected of {total_count} completed annotators"
        
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {status_color}15, {status_color}08); border: 1px solid {status_color}40; border-radius: 8px; padding: 8px 16px; margin: 12px 0; text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,0.1);">
            <div style="color: {status_color}; font-weight: 600; font-size: 0.9rem;">{status_text}</div>
        </div>
        """, unsafe_allow_html=True)
    
    return st.session_state[auto_submit_key]



def display_annotator_checkboxes(annotators: Dict[str, Dict], project_id: int, section: str, updated_selection: List[str], disabled: bool = False):
    """Helper function to display annotator checkboxes with correct role display and model-friendly tooltips"""
    num_annotators = len(annotators)
    if num_annotators <= 3:
        num_cols = num_annotators
    elif num_annotators <= 8:
        num_cols = 4
    else:
        num_cols = 5
    
    annotator_items = list(annotators.items())
    
    for row_start in range(0, num_annotators, num_cols):
        cols = st.columns(num_cols)
        row_annotators = annotator_items[row_start:row_start + num_cols]
        
        for i, (annotator_display, annotator_info) in enumerate(row_annotators):
            with cols[i]:
                if " (" in annotator_display and annotator_display.endswith(")"):
                    full_name = annotator_display.split(" (")[0]
                    initials = annotator_display.split(" (")[1][:-1]
                else:
                    full_name = annotator_display
                    initials = annotator_display[:2].upper()
                
                email = annotator_info.get('email', '')
                user_id = annotator_info.get('id', '')
                # Use project-specific role with correct priority
                project_role = annotator_info.get('Role', annotator_info.get('role', 'annotator'))
                system_role = annotator_info.get('system_role', project_role)
                
                # Enhanced display name for different user types
                display_name = annotator_display
                status_icon = "⏳" if disabled else "✅"
                
                if system_role == "model":
                    display_name = f"🤖 {display_name}"
                elif project_role == "admin":
                    display_name = f"👑 {display_name}"
                elif project_role == "reviewer":
                    display_name = f"🔍 {display_name}"
                else:
                    display_name = f"{status_icon} {display_name}"
                
                # Create tooltip - don't show email for model users
                tooltip_parts = []
                if system_role == "model":
                    tooltip_parts.append("Type: AI Model")
                    tooltip_parts.append(f"ID: {user_id}")
                    tooltip_parts.append(f"Project Role: {project_role}")
                    tooltip_parts.append(f"System Role: {system_role}")
                else:
                    tooltip_parts.append(f"Email: {email}")
                    tooltip_parts.append(f"ID: {user_id}")
                    tooltip_parts.append(f"Project Role: {project_role}")
                    if system_role != project_role:
                        tooltip_parts.append(f"System Role: {system_role}")
                
                if disabled:
                    tooltip_parts.append("Status: Incomplete - cannot select")
                
                tooltip = "\n".join(tooltip_parts)
                
                checkbox_key = f"annotator_cb_{project_id}_{section}_{row_start + i}"
                is_selected = annotator_display in st.session_state.selected_annotators and not disabled
                
                checkbox_value = st.checkbox(
                    display_name,
                    value=is_selected,
                    key=checkbox_key,
                    help=tooltip,
                    disabled=disabled
                )
                
                if checkbox_value and not disabled:
                    updated_selection.append(annotator_display)


