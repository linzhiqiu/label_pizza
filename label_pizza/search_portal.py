import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime
from sqlalchemy.orm import Session
from contextlib import contextmanager

from label_pizza.ui_components import (
    get_card_style, COLORS, custom_info
)
from label_pizza.database_utils import (
    get_db_session, handle_database_errors,
    check_project_has_full_ground_truth, get_user_assignment_dates,
    get_project_groups_with_projects, calculate_user_overall_progress,
    get_schema_question_groups
)
from label_pizza.display_fragments import (
    display_single_choice_question, display_description_question, display_question_status,
    submit_answer_reviews, load_existing_answer_reviews, get_questions_with_custom_display_if_enabled,
    get_project_metadata_cached
)
# Import services
from label_pizza.services import (
    VideoService, ProjectService, SchemaService, QuestionService, 
    AuthService, GroundTruthService, ProjectGroupService
)

# Import custom components
from label_pizza.custom_video_player import custom_video_player

###############################################################################
# SEARCH PORTAL
###############################################################################

@handle_database_errors
def search_portal():
    """Advanced Search Portal for Admins - Clean, Organized, Functional"""
    st.title("🔍 Advanced Search Portal")
    # Handle URL parameters for direct video links
    query_params = st.query_params
    if "video_uid" in query_params:
        video_uid_from_url = query_params["video_uid"]
        custom_info(f"🔗 Shared link loaded - Searching for video: {video_uid_from_url}")
        # Auto-populate the search
        if "auto_search_video_uid" not in st.session_state:
            st.session_state.auto_search_video_uid = video_uid_from_url

    st.markdown("**Comprehensive search and editing capabilities across all projects**")
    
    # Clean tab design with clear separation
    main_tabs = st.tabs([
        "🎬 Video Answer Search", 
        "📊 Video Criteria Search",
    ])
    
    with main_tabs[0]:
        video_answer_search_portal()
    
    with main_tabs[1]:
        video_criteria_search_portal()

###############################################################################
# VIDEO ANSWER SEARCH PORTAL
###############################################################################

@st.fragment
def video_answer_search_portal():
    """Search and edit all answers for a specific video with improved UI"""
    
    st.markdown("## 🎬 Video Answer Search & Editor")
    st.markdown("*Find and edit all answers for any video across all project groups*")
    
    with get_db_session() as session:
        # Improved Step 1: Video Selection Section
        video_info = display_improved_video_selection_section(session)
        
        if not video_info:
            return
        
        st.markdown("---")
        
        # Improved Step 2: Project Group Filter Section  
        project_group_filter = display_improved_project_group_filter_section(session)
        
        st.markdown("---")
        
        # Improved Step 3: Main Video + Answers Layout
        display_improved_video_answers_editor(video_info, project_group_filter, session)

def display_improved_video_selection_section(session: Session) -> Optional[Dict[str, Any]]:
    """Improved video selection interface with better styling"""
    
    st.markdown("### 📹 Step 1: Select Video")
    
    # Get videos with better organization
    videos_df = VideoService.get_all_videos(session=session)
    if videos_df.empty:
        st.warning("🚫 No videos available in the system")
        return None
    
    # Restore search term from session state or URL parameter
    stored_search = st.session_state.get("search_portal_search_term", "")
    auto_video_uid = st.session_state.get("auto_search_video_uid", "")
    initial_search = auto_video_uid if auto_video_uid else stored_search
    
    # Improved filter controls
    col1, col2, col3 = st.columns([3, 2, 2])
    
    with col1:
        search_term = st.text_input(
            "🔍 Search videos by UID", 
            value=initial_search,
            placeholder="Type video UID to filter...",
            key="admin_video_search",
            help="Search through video UIDs to find the one you want"
        )
        
        # Store search term to preserve across reruns
        if search_term:
            st.session_state["search_portal_search_term"] = search_term
    
    with col2:
        include_archived = st.selectbox(
            "📦 Archive Filter",
            ["Active videos only", "Include archived videos"],
            key="include_archived_videos_select",
            help="Choose whether to show archived videos"
        ) == "Include archived videos"
    
    with col3:
        total_videos = len(videos_df)
        active_videos = len(videos_df[~videos_df["Archived"]])
        
        if include_archived:
            info_display = st.selectbox(
                "📊 Video Count",
                [f"Total: {total_videos} ({active_videos} active + {total_videos - active_videos} archived)"],
                key="video_count_display",
                disabled=True,
                help="Currently showing all videos including archived ones"
            )
        else:
            info_display = st.selectbox(
                "📊 Video Count", 
                [f"Active: {active_videos}" + (f" ({total_videos - active_videos} archived hidden)" if total_videos > active_videos else " (no archived)")],
                key="video_count_display",
                disabled=True,
                help="Currently showing only active videos"
            )
    
    # Apply filters
    filtered_videos = videos_df
    if not include_archived:
        filtered_videos = videos_df[~videos_df["Archived"]]
    
    if search_term:
        filtered_videos = filtered_videos[
            filtered_videos["Video UID"].str.contains(search_term, case=False, na=False)
        ]
    
    if filtered_videos.empty:
        st.warning("🔍 No videos match your search criteria")
        # Clear auto-search if video not found
        if auto_video_uid:
            custom_info(f"🔍 Video '{auto_video_uid}' not found in this project")
            if "auto_search_video_uid" in st.session_state:
                del st.session_state.auto_search_video_uid
        return None
    
    # Improved video selection with simplified format
    st.markdown("**📋 Select Video:**")
    
    # Build video options with simplified format
    video_options = {}
    for _, row in filtered_videos.iterrows():
        video_uid = row["Video UID"]
        # Simple format: just the video UID, with archive status if needed
        if row["Archived"]:
            display_name = f"{video_uid} (archived)"
        else:
            display_name = video_uid
        video_options[display_name] = video_uid
    
    # Auto-select logic - prioritize stored selection over URL parameter
    options_list = [""] + list(video_options.keys())
    default_index = 0
    
    # First check if we have a stored selection from previous interaction
    stored_selection = st.session_state.get("search_portal_selected_video")
    if stored_selection and stored_selection["display"] in options_list:
        try:
            default_index = options_list.index(stored_selection["display"])
        except ValueError:
            pass
    
    # Only use auto_video_uid if no stored selection
    if default_index == 0 and auto_video_uid:
        # Look for exact match in video_options values
        matching_display_name = None
        for display_name, video_uid in video_options.items():
            if video_uid == auto_video_uid:
                matching_display_name = display_name
                break
        
        if matching_display_name:
            try:
                default_index = options_list.index(matching_display_name)
                # Clear auto-search state after using it
                if "auto_search_video_uid" in st.session_state:
                    del st.session_state.auto_search_video_uid
            except ValueError:
                pass
        else:
            custom_info(f"🔍 Video '{auto_video_uid}' not found in this project")
            if "auto_search_video_uid" in st.session_state:
                del st.session_state.auto_search_video_uid
    
    # Better selectbox presentation
    if len(video_options) > 20:
        st.info(f"📊 Showing {len(video_options)} videos (use search to narrow results)")
    
    selected_video_display = st.selectbox(
        "Choose from available videos:",
        options_list,
        index=default_index,
        key="selected_video_admin",
        help=f"Select from {len(video_options)} available videos",
        label_visibility="collapsed"
    )
    
    if not selected_video_display:
        st.info("👆 Please select a video from the dropdown above to continue")
        return None
    
    # Get video info and store selection
    selected_video_uid = video_options[selected_video_display]
    
    # Store the selected video in session state to preserve across reruns
    st.session_state["search_portal_selected_video"] = {
        "uid": selected_video_uid,
        "display": selected_video_display
    }
    
    video_info = VideoService.get_video_info_by_uid(video_uid=selected_video_uid, session=session)
    
    if not video_info:
        st.error("❌ Error loading video information")
        return None
    
    # Simple selected video display
    st.markdown(f"""
    <div style="{get_card_style('#28a745')}text-align: center;">
        <div style="color: #155724; font-weight: 600; font-size: 1rem;">
            ✅ Selected: <strong>{video_info['uid']}</strong>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    return video_info

def display_improved_project_group_filter_section(session: Session) -> List[int]:
    """Improved project group filtering interface"""
    
    st.markdown("### 🗂️ Step 2: Configure Project Group Filters")
    
    try:
        project_groups = ProjectGroupService.list_project_groups(session=session)
        
        # Check for unassigned projects
        all_projects = ProjectService.get_all_projects_including_archived(session=session)
        unassigned_project_count = 0
        archived_project_count = 0

        if not all_projects.empty:
            archived_project_count = len(all_projects[all_projects.get("Archived", False) == True])

            assigned_project_ids = set()
            
            for group in project_groups:
                try:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group["id"], session=session)
                    assigned_project_ids.update(int(p["id"]) for p in group_info["projects"])
                except:
                    continue
            
            unassigned_projects = all_projects[~all_projects["ID"].isin(assigned_project_ids)]
            unassigned_project_count = len(unassigned_projects)
        
        if not project_groups and unassigned_project_count == 0 and archived_project_count == 0:
            st.warning("🚫 No project groups or projects found")
            return []
        
        # Initialize selections
        if "admin_selected_groups" not in st.session_state:
            st.session_state.admin_selected_groups = [g["id"] for g in project_groups]
        if "admin_include_unassigned" not in st.session_state:
            st.session_state.admin_include_unassigned = unassigned_project_count > 0
        if "admin_include_archived" not in st.session_state:
            st.session_state.admin_include_archived = False
        
        # Quick actions for project groups only
        action_col1, action_col2 = st.columns([1, 1])

        with action_col1:
            if st.button("✅ Select All Groups", key="admin_select_all_groups", use_container_width=True):
                st.session_state.admin_selected_groups = [g["id"] for g in project_groups]
                st.rerun(scope="fragment")
        
        with action_col2:
            if st.button("❌ Clear All Groups", key="admin_clear_all_groups", use_container_width=True):
                st.session_state.admin_selected_groups = []
                st.rerun(scope="fragment")
        
        # Separate columns for checkboxes
        action_col3, action_col4 = st.columns([1, 1])

        with action_col3:
            # Unassigned projects checkbox (if any exist)
            include_unassigned = st.checkbox(
                f"Include {unassigned_project_count} unassigned projects",
                value=st.session_state.admin_include_unassigned,
                key="admin_unassigned_checkbox",
                disabled=unassigned_project_count == 0,
                help="Include projects that are not assigned to any project group"
            )
            if include_unassigned != st.session_state.admin_include_unassigned:
                st.session_state.admin_include_unassigned = include_unassigned
                st.rerun(scope="fragment")
        
        with action_col4:
            include_archived = st.checkbox(
                f"Include {archived_project_count} archived projects",
                value=st.session_state.admin_include_archived,
                key="admin_include_archived_projects",
                disabled=archived_project_count == 0,
                help="Include projects that have been archived"
            )
            if include_archived != st.session_state.admin_include_archived:
                st.session_state.admin_include_archived = include_archived
                st.rerun(scope="fragment")
        
        selected_video = st.session_state.get("search_portal_selected_video")
        if selected_video:
            video_uid = selected_video["uid"]
            
            # Count projects that contain this video
            active_grouped_projects = 0
            archived_grouped_projects = 0
            active_unassigned_projects = 0
            archived_unassigned_projects = 0
            
            # Count projects in selected groups that contain this video
            for group_id in st.session_state.admin_selected_groups:
                try:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group_id, session=session)
                    for project in group_info["projects"]:
                        project_id = int(project["id"])
                        
                        # Check if this project contains the video
                        project_videos = VideoService.get_project_videos(project_id=project_id, session=session)
                        if any(v["uid"] == video_uid for v in project_videos):
                            if project.get("archived", False):
                                archived_grouped_projects += 1
                            else:
                                active_grouped_projects += 1
                except:
                    continue
            
            # Count unassigned projects that contain this video
            if st.session_state.admin_include_unassigned:
                for _, project_row in unassigned_projects.iterrows():
                    project_id = project_row["ID"]
                    
                    # Check if this project contains the video
                    project_videos = VideoService.get_project_videos(project_id=project_id, session=session)
                    if any(v["uid"] == video_uid for v in project_videos):
                        if project_row.get("Archived", False):
                            archived_unassigned_projects += 1
                        else:
                            active_unassigned_projects += 1
            
            # Calculate totals based on checkbox setting
            if st.session_state.admin_include_archived:
                total_projects = active_grouped_projects + archived_grouped_projects + active_unassigned_projects + archived_unassigned_projects
                total_grouped = active_grouped_projects + archived_grouped_projects
                total_unassigned = active_unassigned_projects + archived_unassigned_projects
                total_archived = archived_grouped_projects + archived_unassigned_projects
            else:
                total_projects = active_grouped_projects + active_unassigned_projects
                total_grouped = active_grouped_projects
                total_unassigned = active_unassigned_projects
                total_archived = 0
            
            # Display metrics
            if total_projects > 0:
                if st.session_state.admin_include_archived and total_archived > 0:
                    # Show 4 columns including archived count
                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                    
                    with metric_col1:
                        st.metric("📁 Total Projects", total_projects)
                    with metric_col2:
                        st.metric("📋 From Groups", total_grouped)
                    with metric_col3:
                        st.metric("📄 Unassigned", total_unassigned)
                    with metric_col4:
                        st.metric("🗄️ Archived", total_archived)
                else:
                    # Show 3 columns without archived count
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    
                    with metric_col1:
                        st.metric("📁 Total Projects", total_projects)
                    with metric_col2:
                        st.metric("📋 From Groups", total_grouped)
                    with metric_col3:
                        st.metric("📄 Unassigned", total_unassigned)
                
                st.success(f"✅ Found **{total_projects} projects** containing video '{video_uid}'")
            else:
                st.warning(f"⚠️ No projects found containing video '{video_uid}' with current filter settings")
        else:
            # No video selected yet - show general counts
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            
            with metric_col1:
                selected_groups_count = len(st.session_state.admin_selected_groups)
                total_groups_count = len(project_groups)
                st.metric("🗂️ Groups Selected", f"{selected_groups_count}/{total_groups_count}")
            
            with metric_col2:
                unassigned_status = "✅ Included" if st.session_state.admin_include_unassigned else "❌ Excluded"
                st.metric("📄 Unassigned", unassigned_status)
            
            with metric_col3:
                archived_status = "✅ Included" if st.session_state.admin_include_archived else "❌ Excluded"
                st.metric("📦 Archived", archived_status)
            
            custom_info("💡 Select a video above to see how many projects contain it")

        # Rest of the function remains the same...
        # Project groups selection
        if project_groups:
            st.markdown("**📋 Select Project Groups:**")
            
            num_cols = min(2, len(project_groups))
            cols = st.columns(num_cols)
            
            updated_selections = []
            
            for i, group in enumerate(project_groups):
                with cols[i % num_cols]:
                    # Get project count
                    try:
                        group_info = ProjectGroupService.get_project_group_by_id(group_id=int(group["id"]), session=session)
                        project_count = len(group_info["projects"])
                    except:
                        project_count = 0
                    
                    is_selected = group["id"] in st.session_state.admin_selected_groups
                    
                    checkbox_value = st.checkbox(
                        f"**{group['name']}**",
                        value=is_selected,
                        key=f"admin_group_cb_{group['id']}",
                        help=f"Description: {group['description'] or 'No description'}"
                    )
                    
                    # Project group info card
                    card_color = "#EAE1F9" if is_selected else "#f8f9fa"
                    border_color = "#B180FF" if is_selected else "#e9ecef"
                    
                    st.markdown(f"""
                    <div style="background: {card_color}; border: 1px solid {border_color}; border-radius: 8px; padding: 12px; margin: 8px 0;">
                        <div style="color: #5C00BF; font-weight: 600; font-size: 0.9rem; margin-bottom: 6px;">
                            📁 {project_count} project{'s' if project_count != 1 else ''}
                        </div>
                        <div style="color: #424242; font-size: 0.85rem; line-height: 1.4;">
                            {group['description'] if group['description'] else '<em>No description provided</em>'}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if checkbox_value:
                        updated_selections.append(int(group["id"]))
            
            if set(updated_selections) != set(st.session_state.admin_selected_groups):
                st.session_state.admin_selected_groups = updated_selections
                st.rerun(scope="fragment")
        
        return st.session_state.admin_selected_groups
        
    except Exception as e:
        st.error(f"❌ Error loading project groups: {str(e)}")
        return []

def display_improved_video_answers_editor(video_info: Dict[str, Any], selected_group_ids: List[int], session: Session):
    """Improved video + answers editor layout"""
    
    st.markdown("### 🎯 Step 3: Video Player & Ground Truth Editor")
    
    if not selected_group_ids:
        st.warning("⚠️ Please select at least one project group in Step 2 to see results")
        return
    
    # Get all ground truth for this video
    gt_data = get_video_ground_truth_across_groups(video_info["id"], selected_group_ids, session)
    
    # Improved video player section
    display_improved_video_player_section(video_info)
    
    st.markdown("---")
    
    # Improved project groups display
    display_improved_project_groups_section(gt_data, video_info, session)

def display_improved_video_player_section(video_info: Dict[str, Any]):
    """Improved video player section"""
    
    st.markdown("#### 📹 Video Player")
    
    # Improved video info card
    created_at = video_info["created_at"].strftime('%Y-%m-%d %H:%M') if video_info["created_at"] else 'Unknown'
    
    st.markdown(f"""
    <div style="{get_card_style('#B180FF')}">
        <div style="color: #1565c0; font-weight: 700; font-size: 1.1rem; margin-bottom: 8px;">
            📹 {video_info["uid"]}
        </div>
        <div style="color: #1976d2; font-size: 0.85rem; margin-bottom: 6px;">
            <strong>URL:</strong> <code style="background: rgba(255,255,255,0.8); padding: 2px 6px; border-radius: 4px; font-size: 0.8rem;">{video_info["url"]}</code>
        </div>
        <div style="color: #1976d2; font-size: 0.85rem;">
            <strong>Created:</strong> {created_at}
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Video player
    custom_video_player(video_info["url"], autoplay=False, loop=True, show_share_button=True)

def display_improved_project_groups_section(gt_data: Dict, video_info: Dict[str, Any], session: Session):
    """Improved project groups display with better organization"""
    
    if not gt_data:
        st.info("📭 No projects found with questions in the selected project groups")
        return
    
    # Enhanced summary stats
    total_projects = sum(len(group_data["projects"]) for group_data in gt_data.values())
    total_groups = len(gt_data)
    
    # Count total question groups across all projects
    total_qg = 0
    for group_data in gt_data.values():
        for project_data in group_data["projects"].values():
            total_qg += len(project_data.get("question_groups", {}))
    
    summary_col1, summary_col2, summary_col3 = st.columns(3)
    
    with summary_col1:
        st.metric("📁 Project Groups", total_groups)
    
    with summary_col2:
        st.metric("📂 Projects", total_projects)
    
    with summary_col3:
        st.metric("❓ Question Groups", total_qg)
    
    # Display each project group with improved styling
    for group_id, group_data in gt_data.items():
        group_name = group_data["group_name"]
        projects = group_data["projects"]
        project_count = len(projects)
        
        # Improved project group header
        st.markdown(f"""
        <div style="{get_card_style('#B180FF')}text-align: center; margin: 20px 0;">
            <div style="color: #5C00BF; font-weight: 700; font-size: 1.2rem; margin-bottom: 4px;">
                📁 {group_name}
            </div>
            <div style="color: #5C00BF; font-size: 0.9rem;">
                {project_count} project{'s' if project_count != 1 else ''} with questions
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Display projects with improved styling
        display_improved_projects_as_expanders(group_data, video_info, session)

def display_improved_projects_as_expanders(group_data: Dict, video_info: Dict[str, Any], session: Session):
    """Display projects with improved expander styling"""
    
    projects = group_data["projects"]
    
    if not projects:
        st.info("📭 No projects with questions in this group")
        return
    
    current_user = st.session_state.user
    user_id = current_user["id"]
    
    # Display each project with enhanced info
    for project_id, project_data in projects.items():
        project_name = project_data["project_name"]
        project_description = project_data.get("project_description", "")
        
        # Count question groups and get completion status
        question_groups = project_data.get("question_groups", {})
        qg_count = len(question_groups)
        
        # Calculate completion status WITH QUESTION GROUP NAMES
        completed_qg = 0
        completion_details = []
        
        for qg_id, qg_data in question_groups.items():
            qg_title = qg_data.get("title", f"Question Group {qg_id}")
            has_gt = any(q_data.get("ground_truth") is not None for q_data in qg_data.get("questions", {}).values())
            
            if has_gt:
                completed_qg += 1
                completion_details.append(f"✅ {qg_title}")
            else:
                completion_details.append(f"⏳ {qg_title}")
        
        # Enhanced project label with detailed status like other portals
        completion_icon = "✅" if completed_qg == qg_count and qg_count > 0 else "⏳" if completed_qg > 0 else "📝"
        
        if qg_count > 0:
            # Show individual question group status like other portals
            detailed_progress = " | ".join(completion_details)
            project_label = f"{completion_icon} {project_name} • {detailed_progress}"
        else:
            project_label = f"{completion_icon} {project_name} • No question groups"
        
        with st.expander(project_label, expanded=(completed_qg == 0)):  # Expand incomplete projects by default
            if project_description:
                st.markdown(f"*{project_description}*")
            
            # Show project stats
            if qg_count > 1:
                progress_percent = (completed_qg / qg_count * 100) if qg_count > 0 else 0
                st.progress(progress_percent / 100)
                st.caption(f"📊 Overall Progress: {completed_qg}/{qg_count} question groups completed ({progress_percent:.0f}%)")
            
            display_project_question_groups_with_tabs(
                project_data, project_id, video_info, user_id, session
            )

def display_project_question_groups_with_tabs(project_data: Dict, project_id: int, video_info: Dict[str, Any], user_id: int, session: Session):
    """Display question groups using tabs exactly like other portals"""
    
    question_groups = project_data.get("question_groups", {})
    
    if not question_groups:
        st.info("📭 No question groups found in this project")
        return
    
    # Convert to list format like other portals
    qg_list = []
    for qg_id, qg_data in question_groups.items():
        qg_list.append({
            "ID": qg_id,
            "Title": qg_data["title"],
            "Description": qg_data.get("description", "")
        })
    
    # Use tabs for question groups EXACTLY like other portals
    if len(qg_list) > 1:
        qg_tab_names = [group['Title'] for group in qg_list]
        qg_tabs = st.tabs(qg_tab_names)
        
        for qg_tab, group in zip(qg_tabs, qg_list):
            with qg_tab:
                display_single_question_group_for_search(
                    video_info, project_id, user_id, group["ID"], 
                    question_groups[group["ID"]], session
                )
    else:
        # Single question group
        group = qg_list[0]
        st.markdown(f"### ❓ {group['Title']}")
        if group.get('Description'):
            st.caption(group['Description'])
        
        display_single_question_group_for_search(
            video_info, project_id, user_id, group["ID"], 
            question_groups[group["ID"]], session
        )


def determine_ground_truth_status(video_id: int, project_id: int, question_group_id: int, user_id: int, session: Session) -> Dict[str, str]:
    """Determine the correct role and button text - simplified with error logging only"""
    
    try:
        # Check if ground truth exists for this question group
        gt_df = GroundTruthService.get_ground_truth_dict_for_question_group(
            video_id=video_id, project_id=project_id, question_group_id=question_group_id, session=session
        )
        questions = QuestionService.get_questions_by_group_id(group_id=question_group_id, session=session)
        
        # Check if gt_df is empty or no questions (dict is empty if len == 0)
        if len(gt_df) == 0 or not questions:
            return {
                "role": "reviewer",
                "button_text": "Submit Ground Truth",
                "message": "Create Ground Truth for this question group"
            }
        
        # Check if any questions in this group have ground truth
        question_texts = [q["text"] for q in questions]
        has_gt_for_group = any(q_text in gt_df for q_text in question_texts)
        
        if not has_gt_for_group:
            return {
                "role": "reviewer", 
                "button_text": "Submit Ground Truth",
                "message": "Create Ground Truth for this question group"
            }
        
        # Check if any questions were modified by admin
        question_ids = [q["id"] for q in questions]
        has_admin_override = any(
            GroundTruthService.check_question_modified_by_admin(
                video_id=int(video_id), project_id=int(project_id), question_id=int(qid), session=session
            )
            for qid in question_ids
        )
        
        if has_admin_override:
            return {
                "role": "meta_reviewer",
                "button_text": "🎯 Override Ground Truth",
                "message": "Override Ground Truth for this question group"
            }
        
        # Check if current user is the original reviewer by getting full GT data
        gt_full_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        if not gt_full_df.empty:
            group_gt = gt_full_df[gt_full_df["Question ID"].isin(question_ids)]
            if not group_gt.empty:
                original_reviewer_ids = set(group_gt["Reviewer ID"].unique())
                if user_id in original_reviewer_ids:
                    return {
                        "role": "reviewer_resubmit",
                        "button_text": "✅ Re-submit Ground Truth",
                        "message": "Re-submit Ground Truth for this question group"
                    }
        
        # Ground truth exists but user is not original reviewer
        return {
            "role": "meta_reviewer",
            "button_text": "🎯 Override Ground Truth",
            "message": "Override Ground Truth for this question group"
        }
        
    except Exception as e:
        print(f"❌ ERROR in determine_ground_truth_status: {e}")
        import traceback
        print(f"❌ ERROR traceback: {traceback.format_exc()}")
        
        return {
            "role": "reviewer",
            "button_text": "Submit Ground Truth",
            "message": "Create Ground Truth for this question group"
        }


@st.fragment
def display_single_question_group_for_search(video_info: Dict, project_id: int, user_id: int, qg_id: int, qg_data: Dict, session: Session):
    """Display single question group as fragment to prevent full page rerun"""
    
    # Determine the correct role and button text
    try:
        gt_status = determine_ground_truth_status(video_info["id"], project_id, qg_id, user_id, session)
    except Exception as e:
        print(f"❌ ERROR determining GT status: {e}")
        # Create fallback form
        with st.form(f"error_gt_status_{video_info['id']}_{qg_id}_{project_id}"):
            st.error(f"Could not determine ground truth status: {str(e)}")
            st.form_submit_button("Unable to Load", disabled=True)
        return
    
    # Show mode messages exactly like other portals
    if gt_status["role"] == "meta_reviewer":
        st.warning("🎯 **Meta-Reviewer Mode** - Override ground truth answers as needed.")
    else:  # reviewer or reviewer_resubmit
        st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
    
    # Get questions from service
    try:
        # service_questions = QuestionService.get_questions_by_group_id(group_id=qg_id, session=session)
        service_questions = get_questions_with_custom_display_if_enabled(
            group_id=qg_id, 
            project_id=project_id, 
            video_id=video_info["id"], 
            session=session
        )

        if not service_questions:
            st.info("No questions found in this group")
            # Create empty form with submit button to prevent error
            with st.form(f"empty_form_{video_info['id']}_{qg_id}_{project_id}"):
                st.info("No questions available")
                st.form_submit_button("No Actions Available", disabled=True)
            return
        
    except Exception as e:
        print(f"❌ ERROR getting questions from service: {e}")
        import traceback
        print(f"❌ ERROR traceback: {traceback.format_exc()}")
        # Create error form
        with st.form(f"questions_error_{video_info['id']}_{qg_id}_{project_id}"):
            st.error(f"Error loading questions: {str(e)}")
            st.form_submit_button("Unable to Load Questions", disabled=True)
        return
    
    # Get existing answers to populate form (IMPROVEMENT 1: Better default loading)
    existing_answers = {}
    try:
        existing_answers = GroundTruthService.get_ground_truth_dict_for_question_group(
            video_id=video_info["id"], project_id=project_id, question_group_id=qg_id, session=session
        )
    except Exception as e:
        print(f"❌ ERROR loading existing answers: {e}")
        existing_answers = {}
    
    # Get selected annotators
    selected_annotators = st.session_state.get("selected_annotators", [])
    
    # Initialize answer review states
    answer_reviews = {}
    if gt_status["role"] in ["reviewer", "meta_reviewer"]:
        for question in service_questions:
            if question["type"] == "description":
                question_text = question["text"]
                try:
                    existing_review_data = load_existing_answer_reviews(
                        video_id=video_info["id"], project_id=project_id, 
                        question_id=question["id"], session=session
                    )
                    answer_reviews[question_text] = existing_review_data
                except Exception as review_error:
                    print(f"❌ ERROR loading review data for question {question['id']}: {review_error}")
                    answer_reviews[question_text] = {}
    
    # Create form with unique key INCLUDING project_id to prevent duplicates
    form_key = f"video_search_form_{video_info['id']}_{qg_id}_{project_id}_{gt_status['role']}_{user_id}"
    
    # Ensure form is always created
    try:
        with st.form(form_key):
            answers = {}
            
            # Content height (same as other portals)
            content_height = 500
            
            # Wrap question display in try/catch
            try:
                with st.container(height=content_height, border=False):
                    for i, question in enumerate(service_questions):
                        question_id = question["id"]
                        question_text = question["text"]
                        existing_value = existing_answers.get(question_text, "")
                        
                        if i > 0:
                            st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                        
                        # Check admin modification status
                        is_modified_by_admin = False
                        admin_info = None
                        if gt_status["role"] == "meta_reviewer":
                            try:
                                is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                    video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                                )
                                if is_modified_by_admin:
                                    admin_info = GroundTruthService.get_admin_modification_details(
                                        video_id=video_info["id"], project_id=project_id, question_id=question_id, session=session
                                    )
                            except Exception as admin_check_error:
                                print(f"❌ ERROR checking admin modification for question {question_id}: {admin_check_error}")
                                is_modified_by_admin = False
                                admin_info = None
                        
                        # Use existing question display functions with keyword arguments
                        try:
                            if question["type"] == "single":
                                answers[question_text] = display_single_choice_question(
                                    question=question,
                                    video_id=video_info["id"],
                                    project_id=project_id,
                                    group_id=qg_id,
                                    role=gt_status["role"],
                                    existing_value=existing_value,
                                    is_modified_by_admin=is_modified_by_admin,
                                    admin_info=admin_info,
                                    form_disabled=False,
                                    session=session,
                                    gt_value="",
                                    mode="",
                                    selected_annotators=selected_annotators,
                                    key_prefix="video_search_",
                                    preloaded_answers=None
                                )
                            else:
                                answers[question_text] = display_description_question(
                                    question=question,
                                    video_id=video_info["id"],
                                    project_id=project_id,
                                    group_id=qg_id,
                                    role=gt_status["role"],
                                    existing_value=existing_value,
                                    is_modified_by_admin=is_modified_by_admin,
                                    admin_info=admin_info,
                                    form_disabled=False,
                                    session=session,
                                    gt_value="",
                                    mode="",
                                    answer_reviews=answer_reviews,
                                    selected_annotators=selected_annotators,
                                    key_prefix="video_search_",
                                    preloaded_answers=None
                                )
                        except Exception as e:
                            print(f"❌ ERROR displaying question {question_id}: {e}")
                            st.error(f"Error displaying question {question_id}: {str(e)}")
                            # Provide fallback answer
                            answers[question_text] = existing_value
            except Exception as e:
                print(f"❌ ERROR displaying questions: {e}")
                # Still provide empty answers
                answers = {}
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # ALWAYS include submit button
            submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
            
            if submitted:
                try:
                    if gt_status["role"] == "meta_reviewer":
                        GroundTruthService.override_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            question_group_id=qg_id, admin_id=user_id, 
                            answers=answers, session=session
                        )
                        st.success("✅ Ground truth overridden successfully!")
                    else:  # reviewer or reviewer_resubmit
                        GroundTruthService.submit_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            reviewer_id=user_id, question_group_id=qg_id, 
                            answers=answers, session=session
                        )
                        success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth submitted successfully!"
                        st.success(success_msg)
                    
                    # Submit answer reviews if any
                    if answer_reviews:
                        submit_answer_reviews(answer_reviews, video_info["id"], project_id, user_id, session)
                    
                    st.rerun(scope="fragment")
                    
                except Exception as e:
                    print(f"❌ ERROR saving ground truth: {e}")
                    import traceback
                    print(f"❌ ERROR traceback: {traceback.format_exc()}")
                    st.error(f"❌ Error saving ground truth: {str(e)}")
    
    except Exception as e:
        print(f"❌ ERROR creating form: {e}")
        import traceback
        print(f"❌ ERROR traceback: {traceback.format_exc()}")
        # Create emergency fallback form
        with st.form(f"emergency_form_{video_info['id']}_{qg_id}_{project_id}_{user_id}"):
            st.error(f"Could not create proper form: {str(e)}")
            st.form_submit_button("Form Creation Failed", disabled=True)


def get_video_ground_truth_across_groups(video_id: int, selected_group_ids: List[int], session: Session) -> Dict:
    """Get only ground truth for a video across selected project groups"""
    
    results = {}

    # Handle unassigned projects if checkbox is selected
    include_unassigned = st.session_state.get("admin_include_unassigned", False)
    include_archived = st.session_state.get("admin_include_archived", False)
    if include_unassigned:
        # Get all projects and find unassigned ones
        all_projects = ProjectService.get_all_projects_including_archived(session=session)

        if not include_archived and not all_projects.empty:
            all_projects = all_projects[all_projects.get("Archived", False) != True]
        
        if not all_projects.empty:
            assigned_project_ids = set()
            for group_id in selected_group_ids:
                try:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=int(group_id), session=session)
                    assigned_project_ids.update(int(p["id"]) for p in group_info["projects"])
                except:
                    continue
            
            unassigned_projects = all_projects[~all_projects["ID"].isin(assigned_project_ids)]
            
            if not unassigned_projects.empty:
                # Process unassigned projects
                unassigned_results = {"group_name": "Unassigned Projects", "projects": {}}
                
                for _, project_row in unassigned_projects.iterrows():
                    project_id = project_row["ID"]
                    project_name = project_row["Name"]
                    
                    # Check if video is in this project
                    project_videos = VideoService.get_project_videos(project_id=project_id, session=session)
                    video_in_project = any(v["id"] == video_id for v in project_videos)
                    
                    if video_in_project:
                        project_results = get_project_ground_truth_for_video(video_id, project_id, session)
                        
                        if project_results["has_data"]:
                            unassigned_results["projects"][project_id] = {
                                "project_name": project_name,
                                "project_description": project_row.get("Description", ""),
                                **project_results
                            }
                
                if unassigned_results["projects"]:
                    results[-1] = unassigned_results  # Use -1 as special key for unassigned
    
    for group_id in selected_group_ids:
        try:
            # Get group info
            group_info = ProjectGroupService.get_project_group_by_id(group_id=group_id, session=session)
            group_name = group_info["group"]["name"]
            projects = group_info["projects"]
            
            group_results = {"group_name": group_name, "projects": {}}
            
            for project in projects:
                if not include_archived and project.get("archived", False): 
                    continue  # Skip archived projects
                
                # Check if video is in this project
                project_videos = VideoService.get_project_videos(project_id=int(project["id"]), session=session)
                video_in_project = any(v["id"] == video_id for v in project_videos)
                
                if not video_in_project:
                    continue
                
                # Get project ground truth only
                project_results = get_project_ground_truth_for_video(video_id, int(project["id"]), session)
                
                if project_results["has_data"]:
                    group_results["projects"][int(project["id"])] = {
                        "project_name": project["name"],
                        "project_description": project["description"],
                        **project_results
                    }
            
            if group_results["projects"]:
                results[group_id] = group_results
                
        except Exception as e:
            st.error(f"Error processing group {group_id}: {str(e)}")
            continue
    
    return results

def get_project_ground_truth_for_video(video_id: int, project_id: int, session: Session) -> Dict:
    """Get only ground truth for a video in a specific project"""
    
    try:
        # Get project info
        project = get_project_metadata_cached(project_id=project_id, session=session)
        
        # Get schema question groups
        question_groups = SchemaService.get_schema_question_groups_list(
            schema_id=project["schema_id"], session=session
        )
        
        project_results = {
            "has_data": False,
            "question_groups": {}
        }
        
        for group in question_groups:
            group_id = group["ID"]
            group_title = group["Title"]
            
            # Get questions in this group
            # questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
            questions = get_questions_with_custom_display_if_enabled(
                group_id=group_id, 
                project_id=project_id, 
                video_id=video_id, 
                session=session
            )
            
            group_data = {
                "title": group_title,
                "description": group["Description"],
                "questions": {}
            }

            if not questions:
                continue
            
            for question in questions:
                question_id = question["id"]
                question_text = question["text"]
                question_type = question["type"]
                
                question_data = {
                    "text": question_text,
                    "type": question_type,
                    "ground_truth": None
                }
                
                # Get ground truth only
                try:
                    gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
                    
                    if not gt_df.empty:
                        gt_row = gt_df[gt_df["Question ID"] == question_id]
                        
                        if not gt_row.empty:
                            gt_data = gt_row.iloc[0]
                            reviewer_info = AuthService.get_user_info_by_id(
                                user_id=int(gt_data["Reviewer ID"]), session=session
                            )
                            
                            question_data["ground_truth"] = {
                                "answer_value": gt_data["Answer Value"],
                                "original_value": gt_data["Original Value"],
                                "reviewer_name": reviewer_info["user_id_str"],
                                "modified_by_admin": gt_data["Modified By Admin"] is not None,
                                "created_at": gt_data["Created At"],
                                "modified_at": gt_data["Modified At"]
                            }
                except Exception as e:
                    print(f"Error getting ground truth: {e}")
                    pass
                
                # Always include question even if no GT yet (for editing)
                group_data["questions"][question_id] = question_data
            
            project_results["question_groups"][group_id] = group_data
            project_results["has_data"] = True
        
        return project_results
        
    except Exception as e:
        st.error(f"Error getting project ground truth: {str(e)}")
        return {"has_data": False, "question_groups": {}}


###############################################################################
# VIDEO CRITERIA SEARCH PORTAL
###############################################################################

@st.fragment
def video_criteria_search_portal():
    """Search videos by criteria with clean results display"""
    
    st.markdown("## 📊 Video Criteria Search")
    st.markdown("*Find videos based on ground truth criteria or completion status*")
    
    with get_db_session() as session:
        search_type_tabs = st.tabs(["🎯 Ground Truth Criteria", "📈 Completion Status"])
        
        with search_type_tabs[0]:
            ground_truth_criteria_search(session)
        
        with search_type_tabs[1]:
            completion_status_search(session)

def ground_truth_criteria_search(session: Session):
    """Search videos by ground truth criteria"""
    
    st.markdown("### 🎯 Search by Ground Truth Answers")
    
    # Project selection
    all_projects_df = ProjectService.get_all_projects_including_archived(session=session)
    if all_projects_df.empty:
        st.warning("🚫 No projects available")
        return
    
    # ADD ARCHIVE FILTERING
    archived_count = len(all_projects_df[all_projects_df.get("Archived", False) == True]) if not all_projects_df.empty else 0
    
    project_filter_col1, project_filter_col2 = st.columns([2, 1])
    
    with project_filter_col1:
        st.markdown("**Step 1: Select Projects to Search**")
        
    with project_filter_col2:
        if archived_count > 0:
            include_archived = st.checkbox(
                f"Include {archived_count} archived projects",
                value=False,  # Default to hide archived
                key="criteria_include_archived",
                help="Include archived projects in search"
            )
        else:
            include_archived = False
    
    # FILTER PROJECTS BASED ON ARCHIVE SETTING
    if include_archived:
        projects_df = all_projects_df
    else:
        projects_df = all_projects_df[all_projects_df.get("Archived", False) != True]
    
    selected_projects = st.multiselect(
        "Projects",
        projects_df["ID"].tolist(),
        format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
        key="criteria_search_projects",
        help="Choose which projects to include in the search"
    )
    
    if not selected_projects:
        st.info("👆 Please select one or more projects to continue")
        return
    
    # Criteria builder (keep existing code)
    st.markdown("**Step 2: Build Search Criteria**")
    
    if "search_criteria_admin" not in st.session_state:
        st.session_state.search_criteria_admin = []
    
    # Add criteria interface (keep existing code)
    with st.expander("➕ Add New Criteria", expanded=True):
        add_col1, add_col2, add_col3, add_col4 = st.columns([2, 2, 2, 1])
        
        with add_col1:
            project_for_criteria = st.selectbox(
                "Project",
                selected_projects,
                format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
                key="criteria_project_select"
            )
        
        with add_col2:
            if project_for_criteria:
                questions = ProjectService.get_project_questions(project_id=project_for_criteria, session=session)
                single_questions = [q for q in questions if q["type"] == "single"]
                
                if single_questions:
                    selected_question = st.selectbox(
                        "Question",
                        [None] + single_questions,
                        format_func=lambda x: x["text"] if x else "Select question...",
                        key="criteria_question_select"
                    )
                else:
                    selected_question = None
                    st.warning("No single-choice questions available")
            else:
                selected_question = None
        
        with add_col3:
            if selected_question:
                question_data = QuestionService.get_question_by_id(question_id=selected_question["id"], session=session)
                if question_data["options"]:
                    selected_answer = st.selectbox(
                        "Required Answer",
                        question_data["options"],
                        key="criteria_answer_select"
                    )
                else:
                    selected_answer = None
                    st.warning("No options available")
            else:
                selected_answer = None
        
        with add_col4:
            if project_for_criteria and selected_question and selected_answer:
                if st.button("➕ Add", key="add_criteria_btn", use_container_width=True):
                    project_name = projects_df[projects_df["ID"]==project_for_criteria]["Name"].iloc[0]
                    
                    criterion = {
                        "project_id": project_for_criteria,
                        "project_name": project_name,
                        "question_id": selected_question["id"],
                        "question_text": selected_question["text"],
                        "required_answer": selected_answer
                    }
                    
                    if criterion not in st.session_state.search_criteria_admin:
                        st.session_state.search_criteria_admin.append(criterion)
                        st.rerun(scope="fragment")
    
    # Display current criteria (keep existing code)
    if st.session_state.search_criteria_admin:
        st.markdown("**Current Search Criteria:**")
        
        for i, criterion in enumerate(st.session_state.search_criteria_admin):
            crit_col1, crit_col2 = st.columns([6, 1])
            
            with crit_col1:
                st.markdown(f"""
                <div style="background: #e3f2fd; border: 1px solid #2196f3; border-radius: 8px; padding: 12px; margin: 4px 0;">
                    <div style="color: #1976d2; font-weight: 600;">{criterion['project_name']}</div>
                    <div style="color: #424242; margin-top: 4px;">
                        <strong>Question:</strong> {criterion['question_text']}<br>
                        <strong>Required Answer:</strong> {criterion['required_answer']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            with crit_col2:
                if st.button("🗑️", key=f"remove_crit_{i}", help="Remove criteria"):
                    st.session_state.search_criteria_admin.pop(i)
                    st.rerun(scope="fragment")
        
        # Search execution
        st.markdown("**Step 3: Execute Search**")
        
        exec_col1, exec_col2, exec_col3 = st.columns([2, 2, 2])
        
        with exec_col1:
            match_logic = st.radio(
                "Match Logic",
                ["Match ALL criteria", "Match ANY criteria"],
                key="criteria_match_logic",
                help="ALL = video must match every criteria, ANY = video matches at least one"
            )
        
        with exec_col2:
            if st.button("🔍 Search Videos", key="execute_criteria_search", type="primary", use_container_width=True):
                match_all = (match_logic == "Match ALL criteria")
                results = execute_ground_truth_search(st.session_state.search_criteria_admin, match_all, session)
                st.session_state.criteria_search_results = results
                st.rerun(scope="fragment")
        
        with exec_col3:
            if st.button("🧹 Clear Criteria", key="clear_criteria_admin", use_container_width=True):
                st.session_state.search_criteria_admin = []
                if "criteria_search_results" in st.session_state:
                    del st.session_state.criteria_search_results
                st.rerun(scope="fragment")
        
        # Display results with new interface
        if "criteria_search_results" in st.session_state:
            display_criteria_search_results_interface(st.session_state.criteria_search_results, session)

def display_criteria_search_results_interface(results: List[Dict], session: Session):
    """Display criteria search results with video editing interface similar to video search"""
    
    if not results:
        st.warning("🔍 No videos match your search criteria")
        return
    
    st.markdown("---")
    st.markdown(f"### 🎬 Search Results ({len(results)} videos found)")
    
    # Layout settings
    st.markdown("#### 🎛️ Layout Settings")
    layout_col1, layout_col2 = st.columns(2)
    
    with layout_col1:
        videos_per_page = st.slider(
            "Videos per page", 
            5, 30, 15, 
            key="criteria_search_per_page",
            help="Number of videos to display per page"
        )
    
    with layout_col2:
        autoplay = st.checkbox(
            "🚀 Auto-play videos",
            value=True,
            key="criteria_search_autoplay",
            help="Automatically start playing videos when they load"
        )
    
    # FIXED PAGINATION SECTION - Always show selectbox
    total_pages = max(1, (len(results) - 1) // videos_per_page + 1)
    
    # Always show pagination selectbox for consistent UI
    st.markdown("**📄 Navigation**")
    page_selection_key = f"criteria_search_page_{videos_per_page}_{len(results)}"
    
    page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
    with page_col2:
        current_page = st.selectbox(
            "Select Page",
            options=list(range(1, total_pages + 1)),
            key=page_selection_key,
            help=f"Navigate through {total_pages} page{'s' if total_pages > 1 else ''}",
            index=0,  # Always start at page 1 when key changes
            disabled=(total_pages == 1)  # Disable but still show when only one page
        ) - 1
    
    # Show page info
    if total_pages > 1:
        st.caption(f"📊 Page {current_page + 1} of {total_pages} • {len(results)} total videos")
    else:
        st.caption(f"📊 Showing all {len(results)} videos on one page")
    
    # Calculate page bounds
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(results))
    page_results = results[start_idx:end_idx]
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(results)}**")
    
    # Display videos
    user = st.session_state.user
    user_id = user["id"]
    
    for result in page_results:
        display_criteria_search_video_result(result, user_id, autoplay, session)
        st.markdown("---")

def display_criteria_search_video_result(result: Dict, user_id: int, autoplay: bool, session: Session):
    """Display a single video result with editing interface"""
    
    video_info = result["video_info"]
    matches = result.get("matches", [])
    criteria = result.get("criteria", [])
    
    # Calculate match info
    if criteria:
        match_count = sum(matches) if matches else 0
        total_criteria = len(criteria)
        match_percentage = match_count / total_criteria if total_criteria > 0 else 0
    else:
        match_percentage = 1.0
        match_count = 0
        total_criteria = 0
    
    # Card styling based on match percentage
    if match_percentage == 1.0:
        card_color = "#4caf50"
    elif match_percentage >= 0.5:
        card_color = "#ff9800"
    else:
        card_color = "#2196f3"
    
    # Video header with match info
    st.markdown(f"""
    <div style="{get_card_style(card_color)}text-align: center;">
        <div style="color: {card_color}; font-weight: 700; font-size: 1.2rem; margin-bottom: 8px;">
            📹 {video_info["uid"]}
        </div>
        {f'<div style="color: #424242; font-size: 0.9rem; margin-bottom: 6px;"><strong>{match_count}/{total_criteria}</strong> criteria matched</div>' if criteria else ''}
    </div>
    """, unsafe_allow_html=True)
    
    # Two columns layout - video and questions
    video_col, question_col = st.columns([1, 1])
    
    with video_col:
        # Video player
        loop = st.session_state.get("criteria_search_loop", True)
        video_height = custom_video_player(video_info["url"], autoplay=autoplay, loop=loop)
        
        # Match details if criteria exist
        if criteria:
            st.markdown("#### 📋 Criteria Match Details")
            for i, (criterion, match) in enumerate(zip(criteria, matches)):
                match_color = "#4caf50" if match else "#f44336"
                match_icon = "✅" if match else "❌"
                
                st.markdown(f"""
                <div style="background: {match_color}15; border-left: 4px solid {match_color}; padding: 8px; margin: 4px 0; border-radius: 4px;">
                    <div style="color: {match_color}; font-weight: 600; font-size: 0.9rem;">
                        {match_icon} {criterion['project_name']}
                    </div>
                    <div style="color: #424242; margin-top: 4px; font-size: 0.85rem;">
                        <strong>Q:</strong> {criterion['question_text']}<br>
                        <strong>Required:</strong> {criterion['required_answer']}
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    with question_col:
        # Show questions for editing - group by project
        projects_with_criteria = {}
        for criterion in criteria:
            project_id = criterion["project_id"]
            if project_id not in projects_with_criteria:
                projects_with_criteria[project_id] = {
                    "project_name": criterion["project_name"],
                    "questions": []
                }
            projects_with_criteria[project_id]["questions"].append(criterion)
        
        if projects_with_criteria:
            # Use tabs if multiple projects
            if len(projects_with_criteria) > 1:
                project_names = [data["project_name"] for data in projects_with_criteria.values()]
                project_tabs = st.tabs([f"📂 {name}" for name in project_names])
                
                for tab, (project_id, project_data) in zip(project_tabs, projects_with_criteria.items()):
                    with tab:
                        display_criteria_project_questions(
                            video_info, project_id, user_id, project_data["questions"], 
                            video_height, session
                        )
            else:
                # Single project
                project_id, project_data = next(iter(projects_with_criteria.items()))
                st.markdown(f"**📂 Project:** {project_data['project_name']}")
                display_criteria_project_questions(
                    video_info, project_id, user_id, project_data["questions"], 
                    video_height, session
                )

def display_criteria_project_questions(video_info: Dict, project_id: int, user_id: int, criteria_questions: List[Dict], video_height: int, session: Session):
    """Display and edit questions for a specific project in criteria search"""
    
    try:
        project = get_project_metadata_cached(project_id=project_id, session=session)
        
        # Get the question groups that contain our criteria questions
        question_ids = [q["question_id"] for q in criteria_questions]
        
        # Group questions by their question groups
        question_groups_with_criteria = {}
        
        for criterion in criteria_questions:
            question_id = criterion["question_id"]
            
            # Find which question group this question belongs to
            try:
                question_groups = SchemaService.get_schema_question_groups_list(
                    schema_id=project["schema_id"], session=session
                )
                
                for group in question_groups:
                    # group_questions = QuestionService.get_questions_by_group_id(
                    #     group_id=group["ID"], session=session
                    # )
                    group_questions = get_questions_with_custom_display_if_enabled(
                        group_id=group["ID"], 
                        project_id=project_id, 
                        video_id=video_info["id"], 
                        session=session
                    )
                    
                    if any(q["id"] == question_id for q in group_questions):
                        group_id = group["ID"]
                        group_title = group["Title"]
                        
                        if group_id not in question_groups_with_criteria:
                            question_groups_with_criteria[group_id] = {
                                "title": group_title,
                                "questions": group_questions,
                                "criteria_question_ids": []
                            }
                        
                        question_groups_with_criteria[group_id]["criteria_question_ids"].append(question_id)
                        break
            except Exception as e:
                st.error(f"Error finding question group for question {question_id}: {str(e)}")
                continue
        
        if not question_groups_with_criteria:
            st.warning("No question groups found for the criteria questions")
            return
        
        # Display question groups with tabs if multiple
        if len(question_groups_with_criteria) > 1:
            group_names = [data["title"] for data in question_groups_with_criteria.values()]
            group_tabs = st.tabs(group_names)
            
            for tab, (group_id, group_data) in zip(group_tabs, question_groups_with_criteria.items()):
                with tab:
                    display_criteria_question_group_editor(
                        video_info, project_id, user_id, group_id, group_data, 
                        video_height, session
                    )
        else:
            # Single question group
            group_id, group_data = next(iter(question_groups_with_criteria.items()))
            st.markdown(f"### ❓ {group_data['title']}")
            display_criteria_question_group_editor(
                video_info, project_id, user_id, group_id, group_data, 
                video_height, session
            )
            
    except Exception as e:
        st.error(f"Error loading project questions: {str(e)}")

def display_criteria_question_group_editor(video_info: Dict, project_id: int, user_id: int, group_id: int, group_data: Dict, video_height: int, session: Session):
    """Display question group editor specifically for criteria search results"""
    
    try:
        questions = group_data["questions"]
        criteria_question_ids = group_data["criteria_question_ids"]
        
        if not questions:
            st.info("No questions in this group.")
            return
        
        # GET SELECTED ANNOTATORS (same as video search)
        selected_annotators = st.session_state.get("selected_annotators", [])
        
        # Determine ground truth status for this question group
        gt_status = determine_ground_truth_status(
            video_info["id"], project_id, group_id, user_id, session
        )
        
        # Show mode message
        if gt_status["role"] == "meta_reviewer":
            st.warning("🎯 **Meta-Reviewer Mode** - Override ground truth answers as needed.")
        else:
            st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
        
        # Get existing answers
        existing_answers = {}
        try:
            existing_answers = GroundTruthService.get_ground_truth_dict_for_question_group(
                video_id=video_info["id"], project_id=project_id, 
                question_group_id=group_id, session=session
            )
        except Exception as e:
            print(f"Error getting ground truth: {e}")
            pass
        
        # Initialize answer review states (same as video search)
        answer_reviews = {}
        if gt_status["role"] in ["reviewer", "meta_reviewer"]:
            for question in questions:
                if question["type"] == "description":
                    question_text = question["text"]
                    existing_review_data = load_existing_answer_reviews(
                        video_id=video_info["id"], project_id=project_id, 
                        question_id=question["id"], session=session
                    )
                    answer_reviews[question_text] = existing_review_data
        
        # Create form
        form_key = f"criteria_form_{video_info['id']}_{group_id}_{project_id}_{gt_status['role']}"
        
        with st.form(form_key):
            answers = {}
            
            # Adjust content height based on video height
            content_height = max(350, video_height - 150)
            
            with st.container(height=content_height, border=False):
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["text"]
                    existing_value = existing_answers.get(question_text, "")
                    
                    # Highlight criteria questions
                    is_criteria_question = question_id in criteria_question_ids
                    
                    if i > 0:
                        st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                    
                    # Check admin modification status
                    is_modified_by_admin = False
                    admin_info = None
                    if gt_status["role"] == "meta_reviewer":
                        try:
                            is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                video_id=video_info["id"], project_id=project_id, 
                                question_id=question_id, session=session
                            )
                            if is_modified_by_admin:
                                admin_info = GroundTruthService.get_admin_modification_details(
                                    video_id=video_info["id"], project_id=project_id, 
                                    question_id=question_id, session=session
                                )
                        except Exception as e:
                            print(f"Error checking question modified by admin: {e}")
                            pass
                    
                    # Add criteria highlighting
                    if is_criteria_question:
                        st.markdown(f"""
                        <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border-left: 4px solid #ffc107; padding: 8px 12px; margin-bottom: 8px; border-radius: 4px;">
                            <strong>🎯 Search Criteria Question</strong>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    # Display questions with keyword arguments
                    if question["type"] == "single":
                        answers[question_text] = display_single_choice_question(
                            question=question,
                            video_id=video_info["id"],
                            project_id=project_id,
                            group_id=group_id,
                            role=gt_status["role"],
                            existing_value=existing_value,
                            is_modified_by_admin=is_modified_by_admin,
                            admin_info=admin_info,
                            form_disabled=False,
                            session=session,
                            gt_value="",
                            mode="",
                            selected_annotators=selected_annotators,
                            key_prefix="criteria_",
                            preloaded_answers=None
                        )
                    else:
                        answers[question_text] = display_description_question(
                            question=question,
                            video_id=video_info["id"],
                            project_id=project_id,
                            group_id=group_id,
                            role=gt_status["role"],
                            existing_value=existing_value,
                            is_modified_by_admin=is_modified_by_admin,
                            admin_info=admin_info,
                            form_disabled=False,
                            session=session,
                            gt_value="",
                            mode="",
                            answer_reviews=answer_reviews,
                            selected_annotators=selected_annotators,
                            key_prefix="criteria_",
                            preloaded_answers=None
                        )
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # Submit button
            submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
            
            if submitted:
                try:
                    if gt_status["role"] == "meta_reviewer":
                        GroundTruthService.override_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            question_group_id=group_id, admin_id=user_id, 
                            answers=answers, session=session
                        )
                        
                        # Submit answer reviews if any
                        if answer_reviews:
                            submit_answer_reviews(answer_reviews, video_info["id"], project_id, user_id, session)
                        
                        st.success("✅ Ground truth overridden successfully!")
                    else:  # reviewer or reviewer_resubmit
                        GroundTruthService.submit_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            reviewer_id=user_id, question_group_id=group_id, 
                            answers=answers, session=session
                        )
                        
                        # Submit answer reviews if any
                        if answer_reviews:
                            submit_answer_reviews(answer_reviews, video_info["id"], project_id, user_id, session)
                        
                        success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth submitted successfully!"
                        st.success(success_msg)
                    
                    st.rerun(scope="fragment")
                    
                except Exception as e:
                    st.error(f"❌ Error saving ground truth: {str(e)}")
                    
    except Exception as e:
        st.error(f"❌ Error loading question group: {str(e)}")


def completion_status_search(session: Session):
    """Search videos by completion status with improved UI layout matching criteria search"""
    
    st.markdown("### 📈 Search by Completion Status")
    
    # Step 1: Project Selection Section (matching ground truth criteria layout)
    all_projects_df = ProjectService.get_all_projects_including_archived(session=session)
    if all_projects_df.empty:
        st.warning("🚫 No projects available")
        return
    
    # Archive filtering with improved layout
    archived_count = len(all_projects_df[all_projects_df.get("Archived", False) == True]) if not all_projects_df.empty else 0
    
    project_filter_col1, project_filter_col2 = st.columns([2, 1])
    
    with project_filter_col1:
        st.markdown("**Step 1: Select Projects to Search**")
        
    with project_filter_col2:
        if archived_count > 0:
            include_archived = st.checkbox(
                f"Include {archived_count} archived projects",
                value=False,  # Default to hide archived
                key="status_include_archived",
                help="Include archived projects in search"
            )
        else:
            include_archived = False
    
    # Filter projects based on archive setting
    if include_archived:
        projects_df = all_projects_df
    else:
        projects_df = all_projects_df[all_projects_df.get("Archived", False) != True]
    
    selected_projects_status = st.multiselect(
        "Projects",
        projects_df["ID"].tolist(),
        format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
        key="status_search_projects",
        help="Choose which projects to include in the search"
    )
    
    if not selected_projects_status:
        st.info("👆 Please select one or more projects to continue")
        return
    
    st.markdown("---")
    
    # Step 2: Completion Filter Section
    completion_filter = st.selectbox(
        "📊 Completion Status Filter",
        ["All videos", "Complete ground truth", "Missing ground truth", "Partial ground truth"],
        key="status_completion_filter",
        help="Filter videos based on their ground truth completion status"
    )
    
    # Row 2: Project Summary
    total_projects = len(selected_projects_status)
    if include_archived and archived_count > 0:
        active_projects = len([p for p in selected_projects_status if not projects_df[projects_df["ID"]==p]["Archived"].iloc[0]])
        archived_projects = total_projects - active_projects
        
        summary_col1, summary_col2, summary_col3 = st.columns(3)
        with summary_col1:
            st.metric("📁 Total Projects", total_projects)
        with summary_col2:
            st.metric("✅ Active", active_projects)
        with summary_col3:
            st.metric("📦 Archived", archived_projects)
    else:
        st.metric("📁 Selected Projects", total_projects)
    
    # Row 3: Search Execution
    st.markdown("**Execute Search:**")
    search_col1, search_col2, search_col3 = st.columns([2, 2, 2])
    
    with search_col2:  # Center the button
        if st.button("🔍 Search Videos", key="execute_status_search", type="primary", use_container_width=True):
            results = execute_project_based_search(selected_projects_status, completion_filter, session)
            st.session_state.status_search_results = results
            st.rerun(scope="fragment")
    
    # Step 3: Clear previous results option
    if "status_search_results" in st.session_state:
        st.markdown("---")
        clear_col1, clear_col2, clear_col3 = st.columns([1, 1, 1])
        
        with clear_col2:
            if st.button("🧹 Clear Results", key="clear_status_results", use_container_width=True):
                if "status_search_results" in st.session_state:
                    del st.session_state.status_search_results
                st.rerun(scope="fragment")
    
    # Display results with editing interface
    if "status_search_results" in st.session_state:
        display_completion_status_results_interface(st.session_state.status_search_results, session)

def display_completion_status_results_interface(results: List[Dict], session: Session):
    """Display completion status results with video editing interface"""
    
    if not results:
        st.warning("🔍 No videos match your criteria")
        return
    
    st.markdown("---")
    st.markdown(f"### 📊 Completion Status Results ({len(results)} videos)")
    
    # Layout settings
    st.markdown("#### 🎛️ Layout Settings")
    layout_col1, layout_col2 = st.columns(2)
    
    with layout_col1:
        videos_per_page = st.slider(
            "Videos per page", 
            5, 30, 15, 
            key="completion_search_per_page",
            help="Number of videos to display per page"
        )
    
    with layout_col2:
        autoplay = st.checkbox(
            "🚀 Auto-play videos",
            value=True,
            key="completion_search_autoplay",
            help="Automatically start playing videos when they load"
        )
    
    # Status summary - FIXED: Dynamic columns based on actual status count
    status_counts = {}
    for result in results:
        status = result["completion_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Display summary with dynamic column count
    status_styles = {
        "complete": ("✅", "Complete", "#4caf50"),
        "partial": ("⚠️", "Partial", "#ff9800"),
        "missing": ("❌", "Missing", "#f44336"),
        "no_questions": ("❓", "No Questions", "#9e9e9e")
    }
    
    # Create columns based on actual status count (no empty columns)
    valid_statuses = [(status, count) for status, count in status_counts.items() if status in status_styles]
    
    if valid_statuses:
        num_cols = len(valid_statuses)
        summary_cols = st.columns(num_cols)
        
        for i, (status, count) in enumerate(valid_statuses):
            emoji, label, color = status_styles[status]
            with summary_cols[i]:
                st.metric(f"{emoji} {label}", count)
    
    # FIXED PAGINATION SECTION - Always show selectbox
    total_pages = max(1, (len(results) - 1) // videos_per_page + 1)
    
    # Always show pagination selectbox for consistent UI
    st.markdown("**📄 Navigation**")
    page_selection_key = f"completion_search_page_{videos_per_page}_{len(results)}"
    
    page_col1, page_col2, page_col3 = st.columns([1, 2, 1])
    with page_col2:
        current_page = st.selectbox(
            "Select Page",
            options=list(range(1, total_pages + 1)),
            key=page_selection_key,
            help=f"Navigate through {total_pages} page{'s' if total_pages > 1 else ''}",
            index=0,  # Always start at page 1 when key changes
            disabled=(total_pages == 1)  # Disable but still show when only one page
        ) - 1
    
    # Show page info
    if total_pages > 1:
        st.caption(f"📊 Page {current_page + 1} of {total_pages} • {len(results)} total videos")
    else:
        st.caption(f"📊 Showing all {len(results)} videos on one page")
    
    # Calculate page bounds
    start_idx = current_page * videos_per_page
    end_idx = min(start_idx + videos_per_page, len(results))
    page_results = results[start_idx:end_idx]
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(results)}**")
    
    # Display videos with editing interface
    user = st.session_state.user
    user_id = user["id"]
    
    for result in page_results:
        display_completion_status_video_result(result, user_id, autoplay, session)
        st.markdown("---")
        
def display_completion_status_video_result(result: Dict, user_id: int, autoplay: bool, session: Session):
    """Display a single video result for completion status search with editing interface"""
    
    video_id = result["video_id"]
    video_uid = result["video_uid"]
    video_url = result["video_url"]
    project_id = result["project_id"]
    project_name = result["project_name"]
    completion_status = result["completion_status"]
    completed_questions = result["completed_questions"]
    total_questions = result["total_questions"]
    
    # Get video info
    video_info = {
        "id": video_id,
        "uid": video_uid,
        "url": video_url
    }
    
    # Status styling
    status_styles = {
        "complete": ("#4caf50", "✅ Complete"),
        "partial": ("#ff9800", "⚠️ Partial"),
        "missing": ("#f44336", "❌ Missing"),
        "no_questions": ("#9e9e9e", "❓ No Questions")
    }
    
    status_color, status_text = status_styles.get(completion_status, ("#9e9e9e", "❓ Unknown"))
    completion_pct = (completed_questions / total_questions * 100) if total_questions > 0 else 0
    
    # Video header with completion info
    st.markdown(f"""
    <div style="{get_card_style(status_color)}text-align: center;">
        <div style="color: {status_color}; font-weight: 700; font-size: 1.2rem; margin-bottom: 8px;">
            📹 {video_uid} • 📂 {project_name}
        </div>
        <div style="color: #424242; font-size: 0.9rem; margin-bottom: 6px;">
            <strong>{status_text}</strong> • {completed_questions}/{total_questions} questions ({completion_pct:.1f}%)
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Two columns layout - video and ALL question groups
    video_col, question_col = st.columns([1, 1])
    
    with video_col:
        # Video player
        loop = st.session_state.get("completion_search_loop", True)
        video_height = custom_video_player(video_url, autoplay=autoplay, loop=loop)
    
    with question_col:
        # Show ALL question groups for this project (like reviewer/meta-reviewer portal)
        try:
            project = get_project_metadata_cached(project_id=project_id, session=session)
            question_groups = get_schema_question_groups(schema_id=project["schema_id"], session=session)
            
            if not question_groups:
                st.info("No question groups found for this project.")
                return
            
            # Use tabs for question groups (same as other portals)
            if len(question_groups) > 1:
                qg_tab_names = [group['Title'] for group in question_groups]
                qg_tabs = st.tabs(qg_tab_names)
                
                for qg_tab, group in zip(qg_tabs, question_groups):
                    with qg_tab:
                        display_completion_question_group_editor(
                            video_info, project_id, user_id, group["ID"], group, 
                            video_height, session
                        )
            else:
                # Single question group
                group = question_groups[0]
                st.markdown(f"### ❓ {group['Title']}")
                if group.get('Description'):
                    st.caption(group['Description'])
                
                display_completion_question_group_editor(
                    video_info, project_id, user_id, group["ID"], group, 
                    video_height, session
                )
                
        except Exception as e:
            st.error(f"Error loading project questions: {str(e)}")


def display_completion_question_group_editor(video_info: Dict, project_id: int, user_id: int, group_id: int, group_data: Dict, video_height: int, session: Session):
    """Display question group editor for completion status search (same as criteria search)"""
    
    try:
        # questions = QuestionService.get_questions_by_group_id(group_id=group_id, session=session)
        questions = get_questions_with_custom_display_if_enabled(
            group_id=group_id, 
            project_id=project_id, 
            video_id=video_info["id"], 
            session=session
        )
        
        if not questions:
            st.info("No questions in this group.")
            return
        
        # Get selected annotators (same as other search)
        selected_annotators = st.session_state.get("selected_annotators", [])
        
        # Determine ground truth status
        gt_status = determine_ground_truth_status(
            video_info["id"], project_id, group_id, user_id, session
        )
        
        # Show mode message
        if gt_status["role"] == "meta_reviewer":
            st.warning("🎯 **Meta-Reviewer Mode** - Override ground truth answers as needed.")
        else:
            st.info("🔍 **Review Mode** - Help create the ground truth dataset!")
        
        # Get existing answers
        existing_answers = {}
        try:
            existing_answers = GroundTruthService.get_ground_truth_dict_for_question_group(
                video_id=video_info["id"], project_id=project_id, 
                question_group_id=group_id, session=session
            )
        except Exception as e:
            print(f"Error getting ground truth: {e}")
            pass
        
        # Initialize answer review states
        answer_reviews = {}
        if gt_status["role"] in ["reviewer", "meta_reviewer"]:
            for question in questions:
                if question["type"] == "description":
                    question_text = question["text"]
                    existing_review_data = load_existing_answer_reviews(
                        video_id=video_info["id"], project_id=project_id, 
                        question_id=question["id"], session=session
                    )
                    answer_reviews[question_text] = existing_review_data
        
        # Create form (same pattern as other search functions)
        form_key = f"completion_form_{video_info['id']}_{group_id}_{project_id}_{gt_status['role']}"
        
        with st.form(form_key):
            answers = {}
            
            content_height = max(350, video_height - 150)
            
            with st.container(height=content_height, border=False):
                for i, question in enumerate(questions):
                    question_id = question["id"]
                    question_text = question["text"]
                    existing_value = existing_answers.get(question_text, "")
                    
                    if i > 0:
                        st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                    
                    # Check admin modification status
                    is_modified_by_admin = False
                    admin_info = None
                    if gt_status["role"] == "meta_reviewer":
                        try:
                            is_modified_by_admin = GroundTruthService.check_question_modified_by_admin(
                                video_id=video_info["id"], project_id=project_id, 
                                question_id=question_id, session=session
                            )
                            if is_modified_by_admin:
                                admin_info = GroundTruthService.get_admin_modification_details(
                                    video_id=video_info["id"], project_id=project_id, 
                                    question_id=question_id, session=session
                                )
                        except Exception as e:
                            print(f"Error checking question modified by admin: {e}")
                            pass
                    
                    # Display question with keyword arguments using completion prefix
                    if question["type"] == "single":
                        answers[question_text] = display_single_choice_question(
                            question=question,
                            video_id=video_info["id"],
                            project_id=project_id,
                            group_id=group_id,
                            role=gt_status["role"],
                            existing_value=existing_value,
                            is_modified_by_admin=is_modified_by_admin,
                            admin_info=admin_info,
                            form_disabled=False,
                            session=session,
                            gt_value="",
                            mode="",
                            selected_annotators=selected_annotators,
                            key_prefix="completion_",
                            preloaded_answers=None
                        )
                    else:
                        answers[question_text] = display_description_question(
                            question=question,
                            video_id=video_info["id"],
                            project_id=project_id,
                            group_id=group_id,
                            role=gt_status["role"],
                            existing_value=existing_value,
                            is_modified_by_admin=is_modified_by_admin,
                            admin_info=admin_info,
                            form_disabled=False,
                            session=session,
                            gt_value="",
                            mode="",
                            answer_reviews=answer_reviews,
                            selected_annotators=selected_annotators,
                            key_prefix="completion_",
                            preloaded_answers=None
                        )
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
            # Submit button
            submitted = st.form_submit_button(gt_status["button_text"], use_container_width=True, type="primary")
            
            if submitted:
                try:
                    if gt_status["role"] == "meta_reviewer":
                        GroundTruthService.override_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            question_group_id=group_id, admin_id=user_id, 
                            answers=answers, session=session
                        )
                        
                        if answer_reviews:
                            submit_answer_reviews(answer_reviews, video_info["id"], project_id, user_id, session)
                        
                        st.success("✅ Ground truth overridden successfully!")
                    else:
                        GroundTruthService.submit_ground_truth_to_question_group(
                            video_id=video_info["id"], project_id=project_id, 
                            reviewer_id=user_id, question_group_id=group_id, 
                            answers=answers, session=session
                        )
                        
                        if answer_reviews:
                            submit_answer_reviews(answer_reviews, video_info["id"], project_id, user_id, session)
                        
                        success_msg = "✅ Ground truth re-submitted successfully!" if gt_status["role"] == "reviewer_resubmit" else "✅ Ground truth submitted successfully!"
                        st.success(success_msg)
                    
                    st.rerun(scope="fragment")
                    
                except Exception as e:
                    st.error(f"❌ Error saving ground truth: {str(e)}")
                    
    except Exception as e:
        st.error(f"❌ Error loading question group: {str(e)}")

###############################################################################
# HELPER FUNCTIONS (REUSED AND OPTIMIZED)
###############################################################################

def execute_ground_truth_search(criteria: List[Dict], match_all: bool, session: Session) -> List[Dict]:
    """Execute ground truth search with given criteria and progress tracking"""
    
    if not criteria:
        return []
    
    # Show progress tracking interface
    st.markdown("### 🔍 Searching Videos...")
    st.caption(f"Searching for videos matching {len(criteria)} criteria")
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    try:
        # Use optimized search function
        matching_videos = GroundTruthService.search_videos_by_criteria_optimized(
            criteria=criteria, 
            match_all=match_all, 
            session=session,
            progress_callback=lambda current, total, message: (
                progress_bar.progress(min(current / total, 1.0)),
                status_container.text(f"Step {current}/{total}: {message}")
            )
        )
        
        # Clear progress interface
        progress_bar.empty()
        status_container.empty()
        
        return matching_videos
        
    except Exception as e:
        progress_bar.empty()
        status_container.empty()
        st.error(f"Search failed: {str(e)}")
        return []

def execute_project_based_search(project_ids: List[int], completion_filter: str, session: Session) -> List[Dict]:
    """Execute project-based search with progress tracking"""
    
    # Show progress tracking interface
    st.markdown("### 🔍 Searching Videos by Completion Status...")
    st.caption(f"Searching {len(project_ids)} projects for {completion_filter.lower()}")
    
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    try:
        # Use optimized search function
        results = GroundTruthService.search_projects_by_completion_optimized(
            project_ids=project_ids,
            completion_filter=completion_filter,
            session=session,
            progress_callback=lambda current, total, message: (
                progress_bar.progress(min(current / total, 1.0)),
                status_container.text(f"Step {current}/{total}: {message}")
            )
        )
        
        # Clear progress interface
        progress_bar.empty()
        status_container.empty()
        
        return results
        
    except Exception as e:
        progress_bar.empty()
        status_container.empty()
        st.error(f"Search failed: {str(e)}")
        return []
