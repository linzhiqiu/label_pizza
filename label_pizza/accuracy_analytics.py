import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Any, Tuple
from datetime import datetime
from sqlalchemy.orm import Session

from label_pizza.services import (
    AuthService, GroundTruthService, ProjectService
)
from label_pizza.ui_components import (
    custom_info, get_card_style, COLORS
)
from label_pizza.database_utils import (
    calculate_user_overall_progress,
    check_project_has_full_ground_truth,
    get_annotator_accuracy_cached,
    get_reviewer_accuracy_cached,
    clear_accuracy_cache_for_project
)

###############################################################################
# ACCURACY DISPLAY FUNCTIONS
###############################################################################

def get_accuracy_color(accuracy: float) -> str:
    """Get color based on accuracy percentage"""
    if accuracy >= 90: return COLORS['success']
    elif accuracy >= 80: return COLORS['warning']
    elif accuracy >= 70: return "#fd7e14"  # Orange
    else: return COLORS['danger']

def format_accuracy_badge(accuracy: Optional[float], total_questions: int = 0) -> str:
    """Format accuracy as a colored badge"""
    if accuracy is None:
        return "📊 No data"
    
    color = get_accuracy_color(accuracy)
    questions_text = f" ({total_questions} answers)" if total_questions > 0 else ""
    
    return f'<span style="background: {color}; color: white; padding: 4px 8px; border-radius: 12px; font-weight: bold; font-size: 0.85rem;">📊 {accuracy:.1f}%{questions_text}</span>'


@st.dialog("📊 Your Personal Accuracy Report", width="large")
def show_personal_annotator_accuracy(user_id: int, project_id: int, session: Session):
    """Show detailed personal accuracy report for a single annotator"""
    try:
        accuracy_data = get_annotator_accuracy_cached(project_id=project_id, session=session)
        if user_id not in accuracy_data:
            st.warning("No accuracy data available for your account.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
        questions = ProjectService.get_project_questions(project_id=int(project_id), session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        user_name = user_info["user_id_str"]
        accuracy = overall_accuracy.get(user_id)
        
        if accuracy is None:
            st.warning("No accuracy data available.")
            return
        
        st.markdown(f"### 🎯 {user_name}'s Performance Report")
        
        color = get_accuracy_color(accuracy)
        total_correct = sum(stats["correct"] for stats in accuracy_data[user_id].values())
        total_answers = sum(stats["total"] for stats in accuracy_data[user_id].values())
        
        st.markdown(f"""
        <div style="{get_card_style(color)}text-align: center;">
            <h2 style="color: {color}; margin: 0;">🏆 Overall Accuracy: {accuracy:.1f}%</h2>
            <p style="margin: 10px 0 0 0; font-size: 1.1rem;">
                You answered {total_correct} out of {total_answers} questions correctly
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Performance level feedback
        if accuracy >= 90:
            st.success("🌟 **Excellent Performance!** You're doing great!")
        elif accuracy >= 80:
            st.info("👍 **Good Performance!** Keep up the good work!")
        elif accuracy >= 70:
            st.warning("📈 **Room for Improvement** - You're getting there!")
        else:
            st.error("📚 **Keep Learning** - Practice makes perfect!")
        
        st.markdown("---")
        st.markdown("### 📋 Question-by-Question Breakdown")
        
        question_details = []
        for question_id, question_text in question_lookup.items():
            user_accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
            stats = accuracy_data[user_id].get(question_id, {"total": 0, "correct": 0})
            
            if stats["total"] > 0:
                status = ("✅ Correct" if user_accuracy == 100 else 
                         "❌ Incorrect" if user_accuracy == 0 else 
                         f"⭐ {user_accuracy:.1f}%")
                
                question_details.append({
                    "Question": question_text[:80] + ("..." if len(question_text) > 80 else ""),
                    "Your Performance": status,
                    "Score": f"{stats['correct']}/{stats['total']}"
                })
        
        if question_details:
            df = pd.DataFrame(question_details)
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download your accuracy report",
                data=csv,
                file_name=f"my_accuracy_report_project_{project_id}.csv",
                mime="text/csv"
            )
        else:
            custom_info("No detailed question data available.")
    
    except Exception as e:
        st.error(f"Error loading your accuracy data: {str(e)}")

@st.dialog("📊 Your Reviewer Accuracy Report", width="large")  
def show_personal_reviewer_accuracy(user_id: int, project_id: int, session: Session):
    """Show detailed personal accuracy report for a single reviewer"""
    try:
        accuracy_data = get_reviewer_accuracy_cached(project_id=project_id, session=session)
        if user_id not in accuracy_data:
            st.warning("No reviewer accuracy data available for your account.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        user_info = AuthService.get_user_info_by_id(user_id=int(user_id), session=session)
        questions = ProjectService.get_project_questions(project_id=int(project_id), session=session)
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        user_name = user_info["user_id_str"]
        accuracy = overall_accuracy.get(user_id)
        
        if accuracy is None:
            st.warning("No accuracy data available.")
            return
        
        st.markdown(f"### 🎯 {user_name}'s Reviewer Performance Report")
        st.caption("Accuracy is measured by how many of your ground truth answers were NOT overridden by admin")
        
        color = get_accuracy_color(accuracy)
        total_correct = sum(stats["correct"] for stats in accuracy_data[user_id].values())
        total_reviews = sum(stats["total"] for stats in accuracy_data[user_id].values())
        
        st.markdown(f"""
        <div style="{get_card_style(color)}text-align: center;">
            <h2 style="color: {color}; margin: 0;">🏆 Reviewer Accuracy: {accuracy:.1f}%</h2>
            <p style="margin: 10px 0 0 0; font-size: 1.1rem;">
                {total_correct} out of {total_reviews} reviews were not overridden
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Performance level feedback
        if accuracy >= 90:
            st.success("🌟 **Excellent Reviewing!** Your ground truth selections are very reliable!")
        elif accuracy >= 80:
            st.info("👍 **Good Reviewing!** Most of your decisions align with final ground truth!")
        elif accuracy >= 70:
            st.warning("📈 **Room for Improvement** - Some of your reviews were adjusted by admin.")
        else:
            st.error("📚 **Review Carefully** - Consider the guidelines more carefully.")
        
        st.markdown("---")
        st.markdown("### 📋 Question-by-Question Review Performance")
        
        question_details = []
        for question_id, question_text in question_lookup.items():
            user_accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
            stats = accuracy_data[user_id].get(question_id, {"total": 0, "correct": 0})
            
            if stats["total"] > 0:
                status = ("✅ Not Overridden" if user_accuracy == 100 else 
                         "❌ Overridden by Admin" if user_accuracy == 0 else 
                         f"⭐ {user_accuracy:.1f}%")
                
                question_details.append({
                    "Question": question_text[:80] + ("..." if len(question_text) > 80 else ""),
                    "Admin Override Status": status,
                    "Reviews": f"{stats['correct']}/{stats['total']} kept"
                })
        
        if question_details:
            df = pd.DataFrame(question_details)
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download your reviewer report",
                data=csv,
                file_name=f"my_reviewer_report_project_{project_id}.csv",
                mime="text/csv"
            )
        else:
            custom_info("No detailed review data available.")
    
    except Exception as e:
        st.error(f"Error loading your reviewer accuracy data: {str(e)}")

def display_user_accuracy_simple(user_id: int, project_id: int, role: str, session: Session) -> bool:
    """Display simple accuracy for a single user if project is in training mode"""
    if not check_project_has_full_ground_truth(project_id=project_id, session=session):
        return False
    
    try:
        if role == "annotator":
            overall_progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
            if overall_progress < 100:
                return False
            
            accuracy_data = get_annotator_accuracy_cached(project_id=project_id, session=session)
            if user_id not in accuracy_data:
                return False
            
            overall_accuracy_dict = calculate_overall_accuracy(accuracy_data)
            accuracy = overall_accuracy_dict.get(user_id)
            
            if accuracy is not None:
                total_answered = sum(stats["total"] for stats in accuracy_data[user_id].values())
                accuracy_badge = format_accuracy_badge(accuracy, total_answered)
                
                acc_col1, acc_col2 = st.columns([2, 1])
                with acc_col1:
                    st.markdown(f"**Your Training Accuracy:** {accuracy_badge}", unsafe_allow_html=True)
                with acc_col2:
                    if st.button("📋 View Details", key=f"personal_acc_{user_id}_{project_id}", 
                                help="View detailed accuracy breakdown"):
                        show_personal_annotator_accuracy(user_id=user_id, project_id=project_id, session=session)
                return True
        
        elif role == "reviewer":
            accuracy_data = get_reviewer_accuracy_cached(project_id=project_id, session=session)
            if user_id not in accuracy_data:
                return False
            
            overall_accuracy_dict = calculate_overall_accuracy(accuracy_data)
            accuracy = overall_accuracy_dict.get(user_id)
            
            if accuracy is not None:
                total_reviewed = sum(stats["total"] for stats in accuracy_data[user_id].values())
                accuracy_badge = format_accuracy_badge(accuracy, total_reviewed)
                
                acc_col1, acc_col2 = st.columns([2, 1])
                with acc_col1:
                    st.markdown(f"**Your Reviewer Accuracy:** {accuracy_badge}", unsafe_allow_html=True)
                with acc_col2:
                    if st.button("📋 View Details", key=f"personal_rev_acc_{user_id}_{project_id}", 
                                help="View detailed reviewer accuracy breakdown"):
                        show_personal_reviewer_accuracy(user_id=user_id, project_id=project_id, session=session)
                return True
        
        elif role == "meta_reviewer":
            return False
    
    except Exception:
        print(f"Error displaying user accuracy: {e}")
        pass
    
    return False


###############################################################################
# Detailed Accuracy Analytics
###############################################################################

@st.dialog("📊 Detailed Accuracy Analytics", width="large")
def show_annotator_accuracy_detailed(project_id: int, session: Session):
    """Show detailed annotator accuracy analytics in a modal dialog"""
    try:
        accuracy_data = get_annotator_accuracy_cached(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No annotator accuracy data available for this project.")
            st.info("💡 Make sure annotators have completed their work and there is ground truth data available.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        users_df = AuthService.get_all_users(session=session)
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        
        user_lookup = {row["ID"]: row["User ID"] for _, row in users_df.iterrows()}
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        overview_tab, detailed_tab, comparison_tab = st.tabs(["📈 Overview", "🔍 Per Question", "⚖️ Compare"])
        
        with overview_tab:
            st.markdown("### 📊 Overall Accuracy Rankings")
            
            overview_data = []
            for user_id, accuracy in overall_accuracy.items():
                if accuracy is not None:
                    user_name = user_lookup.get(user_id, f"User {user_id}")
                    total_answered = sum(stats["total"] for stats in accuracy_data[user_id].values())
                    overview_data.append({
                        "Annotator": user_name,
                        "Overall Accuracy": accuracy,
                        "Total Answers": total_answered,
                        "User ID": user_id
                    })
            
            if overview_data:
                overview_data.sort(key=lambda x: x["Overall Accuracy"], reverse=True)
                
                for i, data in enumerate(overview_data):
                    rank = i + 1
                    accuracy = data["Overall Accuracy"]
                    color = get_accuracy_color(accuracy)
                    
                    rank_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                    
                    st.markdown(f"""
                    <div style="{get_card_style(color)}display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>{rank_emoji} {data['Annotator']}</strong>
                            <br><small>{data['Total Answers']} total answers</small>
                        </div>
                        <div style="background: {color}; color: white; padding: 8px 16px; border-radius: 20px; font-weight: bold; font-size: 1.1rem;">
                            {accuracy:.1f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 📈 Summary Statistics")
                accuracies = [data["Overall Accuracy"] for data in overview_data]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Accuracy", f"{sum(accuracies)/len(accuracies):.1f}%")
                    st.metric("Highest Accuracy", f"{max(accuracies):.1f}%")
                with col2:
                    st.metric("Lowest Accuracy", f"{min(accuracies):.1f}%")
                    st.metric("Total Annotators", len(overview_data))
            else:
                custom_info("No accuracy data available to display.")
        
        with detailed_tab:
            st.markdown("### 🔍 Per-Question Accuracy Analysis")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                selected_users = st.multiselect(
                    "Select annotators to analyze:",
                    list(user_options.keys()),
                    default=list(user_options.keys())[:5] if len(user_options) > 5 else list(user_options.keys())
                )
                
                if selected_users:
                    selected_user_ids = [user_options[name] for name in selected_users]
                    
                    detailed_data = []
                    for question_id, question_text in question_lookup.items():
                        row = {"Question": question_text[:60] + ("..." if len(question_text) > 60 else "")}
                        
                        for user_name in selected_users:
                            user_id = user_options[user_name]
                            accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
                            row[f"{user_name}"] = f"{accuracy:.1f}%" if accuracy is not None else "No data"
                        
                        detailed_data.append(row)
                    
                    if detailed_data:
                        df = pd.DataFrame(detailed_data)
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download detailed accuracy data",
                            data=csv,
                            file_name=f"annotator_accuracy_project_{project_id}.csv",
                            mime="text/csv"
                        )
            else:
                custom_info("No annotators found with accuracy data.")
        
        with comparison_tab:
            st.markdown("### ⚖️ Compare Selected Annotators")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                compare_users = st.multiselect(
                    "Select annotators to compare (max 5):",
                    list(user_options.keys()),
                    max_selections=5
                )
                
                if len(compare_users) >= 2:
                    compare_user_ids = [user_options[name] for name in compare_users]
                    
                    selected_accuracies = [overall_accuracy.get(uid) for uid in compare_user_ids if overall_accuracy.get(uid) is not None]
                    
                    if selected_accuracies:
                        avg_accuracy = sum(selected_accuracies) / len(selected_accuracies)
                        st.metric("Average Accuracy of Selected Annotators", f"{avg_accuracy:.1f}%")
                        
                        st.markdown("#### Individual Performance:")
                        for user_name in compare_users:
                            user_id = user_options[user_name]
                            accuracy = overall_accuracy.get(user_id)
                            if accuracy is not None:
                                diff_from_avg = accuracy - avg_accuracy
                                color = get_accuracy_color(accuracy)
                                
                                diff_icon = "📈" if diff_from_avg > 0 else "📉" if diff_from_avg < 0 else "➡️"
                                diff_text = f"({diff_from_avg:+.1f}% vs group avg)"
                                
                                st.markdown(f"""
                                <div style="background: linear-gradient(135deg, {color}15, {color}05); border-left: 4px solid {color}; padding: 10px 15px; margin: 5px 0; border-radius: 5px;">
                                    <strong>{user_name}</strong>: 
                                    <span style="color: {color}; font-weight: bold;">{accuracy:.1f}%</span>
                                    <small style="color: #666; margin-left: 10px;">{diff_icon} {diff_text}</small>
                                </div>
                                """, unsafe_allow_html=True)
                else:
                    custom_info("Select at least 2 annotators to compare.")
            else:
                custom_info("No annotators available for comparison.")
    
    except Exception as e:
        st.error(f"Error loading accuracy data: {str(e)}")

@st.dialog("📊 Reviewer Accuracy Analytics", width="large")
def show_reviewer_accuracy_detailed(project_id: int, session: Session):
    """Show detailed reviewer accuracy analytics in a modal dialog"""
    try:
        accuracy_data = get_reviewer_accuracy_cached(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No reviewer accuracy data available for this project.")
            st.info("💡 Make sure reviewers have submitted ground truth and there are admin overrides to measure against.")
            return
        
        overall_accuracy = calculate_overall_accuracy(accuracy_data)
        per_question_accuracy = calculate_per_question_accuracy(accuracy_data)
        
        users_df = AuthService.get_all_users(session=session)
        questions = ProjectService.get_project_questions(project_id=project_id, session=session)
        
        user_lookup = {row["ID"]: row["User ID"] for _, row in users_df.iterrows()}
        question_lookup = {q["id"]: q["text"] for q in questions}
        
        overview_tab, detailed_tab = st.tabs(["📈 Overview", "🔍 Per Question"])
        
        with overview_tab:
            st.markdown("### 📊 Reviewer Accuracy (Questions Not Overridden by Admin)")
            st.caption("Accuracy is measured by how many of a reviewer's ground truth answers were NOT overridden by admin")
            
            overview_data = []
            for user_id, accuracy in overall_accuracy.items():
                if accuracy is not None:
                    user_name = user_lookup.get(user_id, f"User {user_id}")
                    total_reviewed = sum(stats["total"] for stats in accuracy_data[user_id].values())
                    correct_count = sum(stats["correct"] for stats in accuracy_data[user_id].values())
                    overview_data.append({
                        "Reviewer": user_name,
                        "Accuracy": accuracy,
                        "Correct Answers": correct_count,
                        "Total Reviews": total_reviewed,
                        "User ID": user_id
                    })
            
            if overview_data:
                overview_data.sort(key=lambda x: x["Accuracy"], reverse=True)
                
                for i, data in enumerate(overview_data):
                    rank = i + 1
                    accuracy = data["Accuracy"]
                    color = get_accuracy_color(accuracy)
                    
                    rank_emoji = "🥇" if rank == 1 else "🥈" if rank == 2 else "🥉" if rank == 3 else f"#{rank}"
                    
                    st.markdown(f"""
                    <div style="{get_card_style(color)}display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>{rank_emoji} {data['Reviewer']}</strong>
                            <br><small>{data['Correct Answers']}/{data['Total Reviews']} not overridden</small>
                        </div>
                        <div style="background: {color}; color: white; padding: 8px 16px; border-radius: 20px; font-weight: bold; font-size: 1.1rem;">
                            {accuracy:.1f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("---")
                st.markdown("### 📈 Summary Statistics")
                accuracies = [data["Accuracy"] for data in overview_data]
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Accuracy", f"{sum(accuracies)/len(accuracies):.1f}%")
                    st.metric("Highest Accuracy", f"{max(accuracies):.1f}%")
                with col2:
                    st.metric("Lowest Accuracy", f"{min(accuracies):.1f}%")
                    st.metric("Total Reviewers", len(overview_data))
            else:
                custom_info("No reviewer accuracy data available to display.")
        
        with detailed_tab:
            st.markdown("### 🔍 Per-Question Reviewer Analysis")
            
            user_options = {user_lookup.get(uid, f"User {uid}"): uid for uid in accuracy_data.keys()}
            if user_options:
                selected_users = st.multiselect(
                    "Select reviewers to analyze:",
                    list(user_options.keys()),
                    default=list(user_options.keys())
                )
                
                if selected_users:
                    detailed_data = []
                    for question_id, question_text in question_lookup.items():
                        row = {"Question": question_text[:60] + ("..." if len(question_text) > 60 else "")}
                        
                        for user_name in selected_users:
                            user_id = user_options[user_name]
                            accuracy = per_question_accuracy.get(user_id, {}).get(question_id)
                            row[f"{user_name}"] = f"{accuracy:.1f}%" if accuracy is not None else "No data"
                        
                        detailed_data.append(row)
                    
                    if detailed_data:
                        df = pd.DataFrame(detailed_data)
                        st.dataframe(df, use_container_width=True)
                        
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download detailed reviewer accuracy data",
                            data=csv,
                            file_name=f"reviewer_accuracy_project_{project_id}.csv",
                            mime="text/csv"
                        )
            else:
                custom_info("No reviewers found with accuracy data.")
    
    except Exception as e:
        st.error(f"Error loading reviewer accuracy data: {str(e)}")

def display_accuracy_button_for_project(project_id: int, role: str, session: Session):
    """Display an elegant accuracy analytics button for training mode projects"""
#     if not check_project_has_full_ground_truth(project_id=project_id, session=session):
#         return False
    
#     try:
#         if role in ["reviewer", "meta_reviewer"]:
#             accuracy_data = get_annotator_accuracy_cached(project_id=project_id, session=session)
#             if accuracy_data:
#                 annotator_count = len(accuracy_data)
#                 if st.button(f"📊 Annotator Analytics ({annotator_count})", 
#                            key=f"accuracy_btn_{project_id}_{role}",
#                            help=f"View detailed accuracy analytics for {annotator_count} annotators",
#                            use_container_width=True):
#                     show_annotator_accuracy_detailed(project_id=project_id, session=session)
#                 return True
            
#             reviewer_accuracy_data = get_reviewer_accuracy_cached(project_id=project_id, session=session)
#             if reviewer_accuracy_data:
#                 reviewer_count = len(reviewer_accuracy_data)
#                 if st.button(f"📊 Reviewer Analytics ({reviewer_count})", 
#                            key=f"reviewer_accuracy_btn_{project_id}_{role}",
#                            help=f"View detailed accuracy analytics for {reviewer_count} reviewers",
#                            use_container_width=True):
#                     show_reviewer_accuracy_detailed(project_id=project_id, session=session)
#                 return True
#     except Exception as e:
#         print(f"Error displaying user accuracy: {e}")
#         pass
    
#     return False

# def display_lazy_accuracy_buttons(project_id: int, role: str, session: Session):
    # """Display analytics buttons without pre-loading any data"""
    if not check_project_has_full_ground_truth(project_id=project_id, session=session):
        custom_info("📝 Analytics are only available for training mode projects with ground truth.")
        return
    
    if role in ["reviewer", "meta_reviewer"]:
        # Show both annotator and reviewer analytics buttons
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Annotator Analytics", 
                        key=f"lazy_annotator_analytics_{project_id}_{role}",
                        help="View detailed accuracy analytics for annotators",
                        use_container_width=True):
                show_annotator_accuracy_detailed(project_id=project_id, session=session)
        
        with col2:
            if st.button("📊 Reviewer Analytics", 
                        key=f"lazy_reviewer_analytics_{project_id}_{role}",
                        help="View detailed accuracy analytics for reviewers", 
                        use_container_width=True):
                show_reviewer_accuracy_detailed(project_id=project_id, session=session)
    
    else:  # annotator role
        if st.button("📊 View Analytics", 
                    key=f"lazy_analytics_{project_id}_{role}",
                    help="View detailed accuracy analytics",
                    use_container_width=True):
            show_annotator_accuracy_detailed(project_id=project_id, session=session)

###############################################################################
# Accuracy Calculation Functions
###############################################################################

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