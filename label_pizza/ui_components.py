import streamlit as st
import streamlit.components.v1 as components
from label_pizza.services import AuthService

###############################################################################
# UI COMPONENTS (colors, card styles, etc.)
###############################################################################

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
# DISPLAY FUNCTIONS
###############################################################################

def display_user_simple(user_name: str, user_email: str, is_ground_truth: bool = False, user_role: str = None):
    """Simple user display using custom styling"""
    display_name, initials = AuthService.get_user_display_name_with_initials(user_name)
    
    icon = "🏆" if is_ground_truth else "👤"
    
    # Add role text if provided - only role text is purple
    role_text = f"<br><div style='margin-top: 8px; font-size: 0.85rem; font-weight: 600; color: #5C00BF;'>Role: {user_role.title()}</div>" if user_role else ""
    
    st.markdown(f"""
    <div style="background: #EAE1F9; border-radius: 12px; padding: 12px 16px; margin: 8px 0; text-align: center;">
        <div style="color: #333333; font-weight: 600; font-size: 0.95rem;">
            {icon} <strong>{user_name}</strong> ({initials}) - {user_email}
            {role_text}
        </div>
    </div>
    """, unsafe_allow_html=True)

def display_pagination_controls(current_page: int, total_pages: int, page_key: str, role: str, project_id: int, position: str = "bottom", video_list_info_str: str = ""):
    """Display pagination controls (can be used for both top and bottom) - keeps original styling"""
    if total_pages <= 1:
        return
        
    def get_pagination_options(current, total):
        if total <= 7:
            return list(range(total))
        
        options = [0]
        start = max(1, current - 1)
        end = min(total - 1, current + 2)
        
        if start > 1:
            options.append("...")
        
        for i in range(start, end):
            if i not in options:
                options.append(i)
        
        if end < total - 1:
            options.append("...")
        
        if total - 1 not in options:
            options.append(total - 1)
        
        return options

    
    # Keep original column layout [1, 5, 1]
    nav_col1, nav_col2, nav_col3 = st.columns([1, 5, 1])
    
    with nav_col1:
        if st.button("◀ Prev", disabled=(current_page == 0), 
                    key=f"{role}_prev_{position}_{project_id}", use_container_width=True):
            st.session_state[page_key] = max(0, current_page - 1)
            st.rerun()
    
    with nav_col2:
        # Keep original nested centering [1, 2, 1]
        _, center_col, _ = st.columns([2, 3, 0.5])  # Push more right
        
        with center_col:
            pagination_options = get_pagination_options(current_page, total_pages)
            
            display_options = []
            actual_pages = []
            for opt in pagination_options:
                if opt == "...":
                    display_options.append("...")
                    actual_pages.append(None)
                else:
                    display_options.append(f"{opt + 1}")
                    actual_pages.append(opt)
            
            try:
                current_display_index = actual_pages.index(current_page)
            except ValueError:
                current_display_index = 0
            
            segmented_key = f"{role}_page_segmented_{position}_{project_id}"
            selected_display = st.segmented_control(
                "📄 Navigate Pages", 
                display_options,
                default=display_options[current_display_index] if current_display_index < len(display_options) else display_options[0],
                key=segmented_key,
                label_visibility="collapsed"
            )
        
        if selected_display and selected_display != "...":
            try:
                selected_index = display_options.index(selected_display)
                new_page = actual_pages[selected_index]
                if new_page is not None and new_page != current_page:
                    st.session_state[page_key] = new_page
                    st.rerun()
            except (ValueError, IndexError):
                pass
    
    with nav_col3:
        if st.button("Next ▶", disabled=(current_page == total_pages - 1), 
                    key=f"{role}_next_{position}_{project_id}", use_container_width=True):
            st.session_state[page_key] = min(total_pages - 1, current_page + 1)
            st.rerun()
    
    # Show page info with appropriate spacing
    # Show page info with appropriate spacing
    page_info_style = "margin-bottom: 1rem;" if position == "top" else "margin-top: 1rem;"
    markdown_str = f"<div style='text-align: center; color: #6c757d; {page_info_style}'>Page {current_page + 1} of {total_pages} • {video_list_info_str}</div>"
    st.markdown(markdown_str, unsafe_allow_html=True)

    # Add working "Back to top" component for bottom pagination only
    if position == "bottom":
        components.html("""
        <div style="text-align: center; margin-top: 8px;">
            <a href="#" onclick="
                try {
                    var element = parent.document.getElementById('video-list-section');
                    if (element) {
                        element.scrollIntoView({behavior: 'smooth'});
                    }
                } catch (e) {
                    console.log('Scroll error:', e);
                }
                return false;
            " style="color: #9553FE; text-decoration: none; font-size: 0.9rem;">
                ↑ Back to top
            </a>
        </div>
        """, height=25)


@st.dialog("🎉 Congratulations!")
def show_annotator_completion():
    """Simple completion popup for annotators"""
    st.markdown("### 🎉 **CONGRATULATIONS!** 🎉")
    st.success("You've completed all questions in this project!")
    custom_info("Great work! You can now move on to other projects or review your answers.")
    
    st.snow()
    st.balloons()
    
    if st.button("Close", use_container_width=True):
        st.rerun()

@st.dialog("🎉 Outstanding Work!")
def show_reviewer_completion():
    """Simple completion popup for reviewers"""
    st.markdown("### 🎉 **OUTSTANDING WORK!** 🎉")
    st.success("This project's ground truth dataset is now complete!")
    custom_info("Please notify the admin that you have completed this project. Excellent job!")
    
    st.snow()
    st.balloons()
    
    if st.button("Close", use_container_width=True):
        st.rerun()
