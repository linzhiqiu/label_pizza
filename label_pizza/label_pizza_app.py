"""
Label Pizza - Modern Streamlit App
==================================
Updated with Meta-Reviewer Portal, copy/paste functionality, approve/reject buttons, improved dashboard,
NEW ACCURACY FEATURES for training mode projects, ENHANCED ANNOTATOR SELECTION, and ADVANCED SORTING/FILTERING.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
from contextlib import contextmanager
import json
import re

# Import modules (adjust paths as needed)
from db import SessionLocal, engine
from models import Base
from services import (
    VideoService, ProjectService, SchemaService, QuestionService, 
    AuthService, QuestionGroupService, AnnotatorService, 
    GroundTruthService, ProjectGroupService, AutoSubmitService, ReviewerAutoSubmitService, get_weighted_votes_for_question_with_custom_weights
)
from custom_video_player import custom_video_player
from search_portal import search_portal
from utils import (
    get_card_style, custom_info, COLORS, handle_database_errors, get_db_session,
    _display_unified_status, _display_clean_sticky_single_choice_question,
    _display_clean_sticky_description_question, _get_enhanced_options_for_reviewer,
    _submit_answer_reviews, _load_existing_answer_reviews, calculate_overall_accuracy, calculate_per_question_accuracy,
    get_schema_question_groups, get_project_videos, get_questions_by_group_cached
)

Base.metadata.create_all(engine)

# Seed admin
def _seed_admin():
    with SessionLocal() as session:
        AuthService.seed_admin(session)
_seed_admin()


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
        accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
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
        accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
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
            
            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
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
            accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
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

@st.dialog("📊 Detailed Accuracy Analytics", width="large")
def show_annotator_accuracy_detailed(project_id: int, session: Session):
    """Show detailed annotator accuracy analytics in a modal dialog"""
    try:
        accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No annotator accuracy data available for this project.")
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
        accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
        if not accuracy_data:
            st.warning("No reviewer accuracy data available for this project.")
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
    if not check_project_has_full_ground_truth(project_id=project_id, session=session):
        return False
    
    try:
        if role in ["reviewer", "meta_reviewer"]:
            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
            if accuracy_data:
                annotator_count = len(accuracy_data)
                if st.button(f"📊 Annotator Analytics ({annotator_count})", 
                           key=f"accuracy_btn_{project_id}_{role}",
                           help=f"View detailed accuracy analytics for {annotator_count} annotators",
                           use_container_width=True):
                    show_annotator_accuracy_detailed(project_id=project_id, session=session)
                return True
            
            reviewer_accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
            if reviewer_accuracy_data:
                reviewer_count = len(reviewer_accuracy_data)
                if st.button(f"📊 Reviewer Analytics ({reviewer_count})", 
                           key=f"reviewer_accuracy_btn_{project_id}_{role}",
                           help=f"View detailed accuracy analytics for {reviewer_count} reviewers",
                           use_container_width=True):
                    show_reviewer_accuracy_detailed(project_id=project_id, session=session)
                return True
    except Exception as e:
        print(f"Error displaying user accuracy: {e}")
        pass
    
    return False

###############################################################################
# AUTHENTICATION & ROUTING
###############################################################################

def login_page():
    st.markdown("<br><br>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div style="text-align: center; margin-bottom: 2rem;">
                <h1 style="color: #9553FE; margin-bottom: 0.5rem;">🍕 Label Pizza</h1>
                <p style="color: #6c757d; font-size: 1.1rem; margin: 0;">Welcome back! Please sign in to your account</p>
            </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            st.markdown("### Sign In")
            
            email = st.text_input("Email Address", placeholder="Enter your email address", help="Use your registered email address")
            password = st.text_input("Password", type="password", placeholder="Enter your password", help="Enter your account password")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            submitted = st.form_submit_button("🚀 Sign In", use_container_width=True, type="primary")
            
            if submitted:
                authenticate_user(email, password)
        
        st.markdown("<br><br>", unsafe_allow_html=True)

def authenticate_user(email: str, password: str):
    if not email or not password:
        st.error("Please enter both email and password")
        return
        
    try:
        with get_db_session() as session:
            user = (AuthService.authenticate(email=email, pwd=password, role="admin", session=session) or
                   AuthService.authenticate(email=email, pwd=password, role="human", session=session))
            
            if user:
                if 'email' not in user or not user['email']:
                    user['email'] = email
                    
                st.session_state.user = user
                st.session_state.user_projects = get_user_projects(user_id=user["id"], session=session)
                st.session_state.available_portals = get_available_portals(user=user, user_projects=st.session_state.user_projects)
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                st.success(f"Welcome {user['name']}!")
                st.rerun()
            else:
                st.error("Invalid email or password")
    except Exception as e:
        st.error(f"Login failed: {str(e)}")

def get_user_projects(user_id: int, session: Session) -> Dict:
    """Get projects assigned to user by role - using ONLY service layer"""
    try:
        user_projects = AuthService.get_user_projects_by_role(user_id=user_id, session=session)
        
        if user_projects.get("admin"):
            all_projects_df = ProjectService.get_all_projects(session=session)
            all_projects_list = [
                {"id": project_row["ID"], "name": project_row["Name"], "description": "", "created_at": None}
                for _, project_row in all_projects_df.iterrows()
            ]
            
            return {
                "annotator": all_projects_list.copy(),
                "reviewer": all_projects_list.copy(),
                "admin": all_projects_list.copy(),
                "meta_reviewer": all_projects_list.copy()
            }
        
        return user_projects
        
    except ValueError as e:
        st.error(f"Error getting user projects: {str(e)}")
        return {"annotator": [], "reviewer": [], "admin": []}

def get_available_portals(user: Dict, user_projects: Dict) -> List[str]:
    """Determine which portals the user can access"""
    if user["role"] == "admin":
        return ["annotator", "reviewer", "meta_reviewer", "search", "admin"]
    
    available_portals = []
    if user_projects.get("annotator"):
        available_portals.append("annotator")
    if user_projects.get("reviewer"):
        available_portals.append("reviewer")
    
    return available_portals



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
            project = ProjectService.get_project_by_id(project_id=project_id, session=session)
            question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
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
            
            auto_group_names = [group["Title"] for group in auto_submit_groups]
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
                [group["Title"] for group in manual_groups],
                key=f"manual_groups_{role}_{project_id}",
                disabled=is_training_mode
            )
            
            selected_groups = [group for group in manual_groups if group["Title"] in selected_group_names]
            
            if selected_groups:
                display_manual_auto_submit_controls(selected_groups, target_videos, project_id, user_id, role, session, is_training_mode)
    
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
            project = ProjectService.get_project_by_id(project_id=project_id, session=session)
            question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
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
            [group["Title"] for group in question_groups],
            key=f"manual_groups_{role}_{project_id}"
        )
        
        selected_groups = [group for group in question_groups if group["Title"] in selected_group_names]
        
        if selected_groups:
            display_manual_auto_submit_controls(selected_groups, target_videos, project_id, user_id, role, session, False)


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

def build_dynamic_virtual_responses_for_video(
    video_id: int, project_id: int, role: str, session: Session,
    virtual_responses_by_question: Dict[int, List[Dict]], 
    description_annotators: Dict[int, str], 
    available_annotators: Dict[str, Dict]
) -> Dict[int, List[Dict]]:
    """
    Build dynamic virtual responses for a specific video, handling description questions correctly.
    
    For reviewers:
    - Single choice questions: Use empty virtual responses (let option weights apply to real answers)
    - Description questions: Get the selected annotator's actual answer for this specific video
    
    For annotators: Return the original virtual responses unchanged
    """
    if role != "reviewer":
        return virtual_responses_by_question
    
    # Start with the configured virtual responses
    dynamic_responses = virtual_responses_by_question.copy()
    
    # Process description questions for reviewers
    for question_id, selected_annotator in description_annotators.items():
        if selected_annotator == "Auto (use user weights)":
            # Use weighted voting - keep any existing virtual responses
            continue
        
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
                        dynamic_responses[question_id] = [{
                            "answer": answer_text,
                            "user_weight": 1.0
                        }]
                    else:
                        # No answer from this annotator for this video - use empty
                        dynamic_responses[question_id] = []
                else:
                    # No answers at all for this question - use empty
                    dynamic_responses[question_id] = []
        except Exception as e:
            print(f"Error getting answer for question {question_id} from {selected_annotator}: {e}")
            dynamic_responses[question_id] = []
    
    return dynamic_responses

def build_virtual_responses_for_video(video_id: int, project_id: int, role: str, session: Session) -> Dict[int, List[Dict]]:
    """
    Build virtual responses for a specific video, using stored option weights and description selections.
    
    MINIMAL CHANGE: Only for reviewers, and only when they have custom settings.
    For annotators: returns empty dict (uses original virtual_responses)
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
    
    # Handle description questions with specific annotator selections
    for question_id, selected_annotator in description_selections.items():
        if selected_annotator == "Auto (use user weights)":
            continue  # Use weighted voting
        
        try:
            # Get available annotators
            available_annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
            annotator_info = available_annotators.get(selected_annotator, {})
            annotator_user_id = annotator_info.get('id')
            
            if annotator_user_id:
                # Get this annotator's answer for this specific video
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




# MINIMAL MODIFICATION: Update the preview and execution functions to use custom settings
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
            available_annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
        group_title = group["Title"]
        
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
                        "group": group_title,
                        "video": video_uid,
                        "error": result.get("verification_error", "Unknown verification error")
                    })
                
                if "details" in result and "threshold_failures" in result["details"]:
                    for failure in result["details"]["threshold_failures"]:
                        threshold_failure_details.append({
                            "group": group_title,
                            "video": video_uid,
                            "question": failure["question"],
                            "percentage": failure["percentage"],
                            "threshold": failure["threshold"]
                        })
                
            except Exception as e:
                total_verification_failures += 1
                verification_failure_details.append({
                    "group": group_title,
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
            available_annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
                group_title = group["Title"]
                
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
                        video_results[video_uid]["groups"][group_title] = {
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
                        
                        video_results[video_uid]["groups"][group_title] = {
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
                    
                    video_results[video_uid]["groups"][group_title] = {
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

def calculate_preload_answers_no_threshold(video_id: int, project_id: int, question_group_id: int, 
                                         include_user_ids: List[int], virtual_responses_by_question: Dict,
                                         session: Session, user_weights: Dict[int, float] = None, role: str = "annotator") -> Dict[str, str]:
    """Calculate preload answers WITHOUT threshold requirements - REVERTED to original for annotators"""
    
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return {}
        
        preload_answers = {}
        
        for question in questions:
            question_id = question["id"]
            question_text = question["display_text"]
            question_type = question["type"]
            
            if question_type == "single":
                # REVERTED: Use original AutoSubmitService logic for single choice
                vote_counts = {}
                
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
            available_annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
                    result_answers = calculate_preload_answers_no_threshold_with_custom_weights(
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


def calculate_preload_answers_no_threshold_with_custom_weights(
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
        
        for question in questions:
            question_id = question["id"]
            question_text = question["display_text"]
            question_type = question["type"]
            
            if question_type == "single":
                # Get custom option weights for this question
                question_custom_weights = option_weights.get(question_id, {}) if option_weights else {}
                
                # Get all votes for this question (annotator + virtual responses)
                vote_counts = get_weighted_votes_for_question_with_custom_weights(
                    video_id=video_id, project_id=project_id, question_id=question_id,
                    include_user_ids=include_user_ids, 
                    virtual_responses=virtual_responses_by_question.get(question_id, []),
                    session=session, user_weights=user_weights,
                    custom_option_weights=question_custom_weights if question_custom_weights else None
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
        print(f"Error in calculate_preload_answers_no_threshold_with_custom_weights: {e}")
        return {}


def display_manual_auto_submit_controls(selected_groups: List[Dict], videos: List[Dict], project_id: int, user_id: int, role: str, session: Session, is_training_mode: bool):
    """Display manual auto-submit controls - MINIMAL FIX for reviewer virtual responses"""
    
    # Get annotators for filtering (reviewer only)
    available_annotators = {}
    if role == "reviewer":
        try:
            available_annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
                        value=current_weight,
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
                group_title = group["Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_title}**")
                
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
                            default_option_weights = question_data.get("option_weights", [])
                            options = question_data.get("options", [])
                            
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
                                            value=current_weight,
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
                                                # Apply same naming convention as get_optimized_all_project_annotators
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
                group_title = group["Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_title}**")
                
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
                group_title = group["Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_title}**")
                
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
                group_title = group["Title"]
                questions = all_questions_by_group.get(group_id, [])
                
                if not questions:
                    continue
                
                st.markdown(f"**📋 {group_title}**")
                
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
        
        # REMOVED: Ugly preloaded status message
        
        # Get selected annotators for reviewer/meta-reviewer roles
        selected_annotators = None
        if role in ["reviewer", "meta_reviewer"]:
            selected_annotators = st.session_state.get("selected_annotators", [])
        
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
                gt_df = GroundTruthService.get_ground_truth(video_id=video["id"], project_id=project_id, session=session)
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
                    existing_review_data = _load_existing_answer_reviews(
                        video_id=video["id"], project_id=project_id, 
                        question_id=question["id"], session=session
                    )
                    answer_reviews[question_text] = existing_review_data
        
        # Create form - ensure it always has questions and a submit button
        form_key = f"form_{video['id']}_{group_id}_{role}"
        with st.form(form_key):
            answers = {}
            
            content_height = max(350, container_height - 150)
            
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
                            st.markdown('<div style="margin: 32px 0;"></div>', unsafe_allow_html=True)
                        
                        # FIXED: Pass group_id directly to avoid lookup issues
                        if question["type"] == "single":
                            answers[question_text] = _display_clean_sticky_single_choice_question(
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
                                preloaded_answers=preloaded_answers
                            )
                        else:
                            answers[question_text] = _display_clean_sticky_description_question(
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
                                preloaded_answers=preloaded_answers
                            )
            except Exception as e:
                st.error(f"Error displaying questions: {str(e)}")
                # Still provide empty answers dict for form submission
                answers = {}
            
            st.markdown('<div style="margin: 8px 0;"></div>', unsafe_allow_html=True)
            
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
                            if overall_progress >= 100:
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
                                _submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
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
                                _submit_answer_reviews(answer_reviews, video["id"], project_id, user_id, session)
                            
                            try:
                                project_progress = ProjectService.progress(project_id=project_id, session=session)
                                if project_progress['completion_percentage'] >= 100:
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
# SHARED UTILITY FUNCTIONS
###############################################################################

def get_optimized_all_project_annotators(project_id: int, session: Session) -> Dict[str, Dict]:
    """Get all annotators who have answered questions in this project with correct project-specific roles"""
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
        
    except ValueError as e:
        st.error(f"Error getting project annotators: {str(e)}")
        return {}


def display_user_simple(user_name: str, user_email: str, is_ground_truth: bool = False):
    """Simple user display using custom styling"""
    display_name, initials = AuthService.get_user_display_name_with_initials(user_name)
    
    icon = "🏆" if is_ground_truth else "👤"
    
    st.markdown(f"""
    <div style="background: #EAE1F9; border-radius: 12px; padding: 12px 16px; margin: 8px 0; text-align: center;">
        <div style="color: #333333; font-weight: 600; font-size: 0.95rem;">
            {icon} <strong>{user_name}</strong> ({initials}) - {user_email}
        </div>
    </div>
    """, unsafe_allow_html=True)

def check_project_has_full_ground_truth(project_id: int, session: Session) -> bool:
    """Check if project has complete ground truth for ALL questions and videos"""
    try:
        return ProjectService.check_project_has_full_ground_truth(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error checking ground truth status: {str(e)}")
        return False

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
                if st.button("◀ Previous", disabled=(current_page == 0), key=f"prev_{page_key}", use_container_width=True):
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
###############################################################################
# NEW FUNCTIONS FOR SORTING AND FILTERING
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
        
        selected_filters = {}
        
        for question_id, available_answers in gt_options.items():
            question_text = question_lookup.get(question_id, f"Question {question_id}")
            
            # Don't truncate - show full question text
            display_question = question_text
            
            filter_key = f"video_filter_q_{question_id}_{project_id}"
            selected_answer = st.selectbox(
                f"**{display_question}**",
                ["Any"] + sorted(available_answers),
                key=filter_key,
                help=f"Filter videos where this question has the selected ground truth answer"
            )
            
            if selected_answer != "Any":
                selected_filters[question_id] = selected_answer
        
        if selected_filters:
            filter_summary = []
            for q_id, answer in selected_filters.items():
                q_text = question_lookup.get(q_id, f"Q{q_id}")
                # Show more of the question text in summary
                display_text = q_text[:80] + "..." if len(q_text) > 80 else q_text
                filter_summary.append(f"{display_text} = {answer}")
            
            st.success(f"🔍 **Active Filters:** {' | '.join(filter_summary)}")
        else:
            custom_info("ℹ️ No filters active - showing all videos")
        
        # Store filters in session state
        st.session_state[f"video_filters_{project_id}"] = selected_filters
    else:
        custom_info("No ground truth data available for filtering yet. Complete ground truth annotation to enable filtering.")
        st.session_state[f"video_filters_{project_id}"] = {}
    
    # st.markdown(f"""
    # <div style="background: linear-gradient(135deg, #fff3cd, #ffeaa7); border-left: 4px solid {COLORS['warning']}; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #2c3e50;">
    #     💡 <strong>Tip:</strong> Filters only work on questions that have ground truth answers. Complete annotation first to see more filter options.
    # </div>
    # """, unsafe_allow_html=True)
    custom_info("💡 Filters only work on questions that have ground truth answers. Complete annotation first to see more filter options.")

def run_project_wide_auto_submit_on_entry(project_id: int, user_id: int, session: Session):
    """Run auto-submit for all auto-submit groups across entire project when user first enters - OPTIMIZED"""
    
    try:
        # Get all data in fewer queries
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
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

def display_smart_annotator_selection(annotators: Dict[str, Dict], project_id: int):
    """Modern, compact annotator selection with completion checks and confidence scores for model users"""
    if not annotators:
        custom_info("No annotators have submitted answers for this project yet.")
        return []
    
    # Check completion status for each annotator
    try:
        with get_db_session() as session:
            completed_annotators = {}
            incomplete_annotators = {}
            
            for annotator_display, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id:
                    try:
                        progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                        if progress >= 100:
                            completed_annotators[annotator_display] = annotator_info
                        else:
                            incomplete_annotators[annotator_display] = annotator_info
                    except:
                        incomplete_annotators[annotator_display] = annotator_info
                else:
                    incomplete_annotators[annotator_display] = annotator_info
    except:
        # Fallback: treat all as completed if we can't check
        completed_annotators = annotators
        incomplete_annotators = {}
    
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

def display_smart_annotator_selection_for_auto_submit(annotators: Dict[str, Dict], project_id: int):
    """Annotator selection for auto-submit - only completed annotators"""
    if not annotators:
        custom_info("No annotators have submitted answers for this project yet.")
        return []
    
    # Check completion status for each annotator
    try:
        with get_db_session() as session:
            completed_annotators = {}
            
            for annotator_display, annotator_info in annotators.items():
                user_id = annotator_info.get('id')
                if user_id:
                    try:
                        progress = calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
                        if progress >= 100:
                            completed_annotators[annotator_display] = annotator_info
                    except:
                        pass  # Skip if can't check progress
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

def display_order_tab(project_id: int, role: str, project: Any, session: Session):
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
    question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
    
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
                group_title = group["Title"]
                
                order_col1, order_col2, order_col3 = st.columns([0.1, 0.8, 0.1])
                
                with order_col1:
                    if st.button("⬆️", key=f"group_up_{project_id}_{group_id}_{i}", 
                                disabled=(i == 0), help="Move up"):
                        st.session_state[order_key][i], st.session_state[order_key][i-1] = \
                            st.session_state[order_key][i-1], st.session_state[order_key][i]
                        st.rerun()
                
                with order_col2:
                    st.write(f"**{i+1}.** {group_title}")
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
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error loading project: {str(e)}")
        return
    
    # 🔥 ADD THE CACHE CLEARING CODE HERE - RIGHT AFTER PROJECT LOADS SUCCESSFULLY
    # Clear cache when entering a new project for fresh data
    if st.session_state.get('last_project_id') != project_id:
        from utils import clear_project_cache  # Import the function
        clear_project_cache(project_id)
        st.session_state.last_project_id = project_id
    
    
    mode = "Training" if check_project_has_full_ground_truth(project_id=project_id, session=session) else "Annotation"
    
    st.markdown(f"## 📁 {project.name}")
    
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
        # st.markdown("---")
        
        if mode == "Training":
            analytics_tab, annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, auto_submit_tab = st.tabs([
                "📊 Analytics", "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "⚡ Auto-Submit"
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
            annotator_tab, sort_tab, filter_tab, order_tab, layout_tab, auto_submit_tab = st.tabs([
                "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout", "⚡ Auto-Submit"
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
                annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
            st.markdown("#### 🎛️ Video Layout Settings")
            
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            _display_video_layout_controls(videos, role)
            custom_info("💡 Tip: Adjust layout to optimize your workflow.")
        
        with auto_submit_tab:
            display_auto_submit_tab(project_id=project_id, user_id=user_id, role=role, videos=videos, session=session)
    
    elif role == "meta_reviewer":
        # st.markdown("---")
        
        # NO AUTO-SUBMIT TAB FOR META-REVIEWER
        if mode == "Training":
            analytics_tab, annotator_tab, sort_tab, filter_tab, order_tab, layout_tab = st.tabs([
                "📊 Analytics", "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout"
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
            annotator_tab, sort_tab, filter_tab, order_tab, layout_tab = st.tabs([
                "👥 Annotators", "🔄 Sort", "🔍 Filter", "📋 Order", "🎛️ Layout"
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
                annotators = get_optimized_all_project_annotators(project_id=project_id, session=session)
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
            st.markdown("#### 🎛️ Video Layout Settings")
            
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            _display_video_layout_controls(videos, role)
            custom_info("💡 Tip: Adjust layout to optimize your workflow.")
    
    else:  # Annotator role
        # st.markdown("---")
        
        layout_tab, sort_tab, auto_submit_tab = st.tabs(["🎛️ Layout Settings", "🔄 Sort", "⚡ Auto-Submit"])
        
        with layout_tab:
            st.markdown("#### 🎛️ Video Layout Settings")
            
            st.markdown(f"""
            <div style="{get_card_style('#B180FF')}text-align: center;">
                <div style="color: #5C00BF; font-weight: 500; font-size: 0.95rem;">
                    🎛️ Customize Your Video Display - Adjust how videos and questions are laid out
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            _display_video_layout_controls(videos, role)
            # st.markdown(f"""
            # <div style="background: #EAE1F9; border-left: 4px solid #B180FF; border-radius: 8px; padding: 12px 16px; margin-top: 16px; font-size: 0.9rem; color: #5C00BF;">
            #     💡 <strong>Tip:</strong> Adjust layout to optimize your annotation workflow.
            # </div>
            # """, unsafe_allow_html=True)
            custom_info("💡 Adjust layout to optimize your annotation workflow.")
        
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
    
    st.markdown(f"**Showing videos {start_idx + 1}-{end_idx} of {len(videos)}**")
    
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
    
    # Pagination controls
    if total_pages > 1:
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
        
        nav_col1, nav_col2, nav_col3 = st.columns([1, 5, 1])
        
        with nav_col1:
            if st.button("◀ Previous Page", disabled=(current_page == 0), key=f"{role}_prev_{project_id}", use_container_width=True):
                st.session_state[page_key] = max(0, current_page - 1)
                st.rerun()
        
        with nav_col2:
            _, center_col, _ = st.columns([1, 2, 1])
            
            with center_col:
                pagination_options = get_pagination_options(current_page, total_pages)
                
                display_options = []
                actual_pages = []
                for opt in pagination_options:
                    if opt == "...":
                        display_options.append("...")
                        actual_pages.append(None)
                    else:
                        display_options.append(f"Page {opt + 1}")
                        actual_pages.append(opt)
                
                try:
                    current_display_index = actual_pages.index(current_page)
                except ValueError:
                    current_display_index = 0
                
                segmented_key = f"{role}_page_segmented_{project_id}"
                selected_display = st.segmented_control(
                    "📄 Navigate Pages", 
                    display_options,
                    default=display_options[current_display_index] if current_display_index < len(display_options) else display_options[0],
                    key=segmented_key
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
            if st.button("Next Page ▶", disabled=(current_page == total_pages - 1), key=f"{role}_next_{project_id}", use_container_width=True):
                st.session_state[page_key] = min(total_pages - 1, current_page + 1)
                st.rerun()
        
        st.markdown(f"<div style='text-align: center; color: #6c757d; margin-top: 1rem;'>Page {current_page + 1} of {total_pages}</div>", unsafe_allow_html=True)

def _display_video_layout_controls(videos: List[Dict], role: str):
    """Display video layout controls"""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🔄 Video Pairs Per Row**")
        st.slider(
            "Choose layout", 
            1, 2, 
            st.session_state.get(f"{role}_pairs_per_row", 1), 
            key=f"{role}_pairs_per_row",
            help="Choose how many video-answer pairs to display side by side"
        )
    
    with col2:
        st.markdown("**📄 Videos Per Page**")
        
        min_videos_per_page = st.session_state.get(f"{role}_pairs_per_row", 1)
        max_videos_per_page = max(min(20, len(videos)), min_videos_per_page + 1)
        default_videos_per_page = min(min(4, len(videos)), max_videos_per_page)
        
        if len(videos) == 1:
            st.write("**1** (only video in project)")
        elif max_videos_per_page > min_videos_per_page:
            st.slider(
                "Pagination setting", 
                min_videos_per_page, 
                max_videos_per_page, 
                st.session_state.get(f"{role}_per_page", default_videos_per_page),
                key=f"{role}_per_page",
                help="Set how many videos to show on each page"
            )
        else:
            st.write(f"**{len(videos)}** (showing all videos)")
    
    st.markdown("**🎬 Video Playback Settings**")
    col3, col4 = st.columns(2)
    
    with col3:
        st.checkbox(
            "🚀 Auto-play videos on load",
            value=st.session_state.get(f"{role}_autoplay", True),
            key=f"{role}_autoplay",
            help="Automatically start playing videos when they load"
        )
    
    with col4:
        st.checkbox(
            "🔄 Loop videos",
            value=st.session_state.get(f"{role}_loop", True),
            key=f"{role}_loop",
            help="Automatically restart videos when they finish"
        )

# Update display_video_answer_pair function to handle transaction errors

@st.fragment
def display_video_answer_pair(video: Dict, project_id: int, user_id: int, role: str, mode: str, session: Session):
    """Display a single video-answer pair in side-by-side layout with tabs"""
    try:
        project = ProjectService.get_project_by_id(project_id=project_id, session=session)
        
        # Add transaction recovery for question groups
        try:
            question_groups = get_schema_question_groups(schema_id=project.schema_id, session=session)
        except Exception as qg_error:
            # Try to recover by creating a new session
            st.error(f"Database error occurred. Refreshing...")
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
            f"✅ {group['Title']}" if completion_status[group["ID"]] else group['Title']
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
            video_height = custom_video_player(video["url"], autoplay=autoplay, loop=loop)
        
        with answer_col:
            tab_names = [group['Title'] for group in question_groups]
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

def check_all_questions_have_ground_truth(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    """Check if all questions in a group have ground truth answers for this specific video"""
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return False
        
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        if gt_df.empty:
            return False
        
        gt_question_ids = set(gt_df["Question ID"].tolist())
        return all(question["id"] in gt_question_ids for question in questions)
    except:
        return False

def check_ground_truth_exists_for_group(video_id: int, project_id: int, question_group_id: int, session: Session) -> bool:
    """Check if ANY ground truth exists for questions in this group"""
    try:
        questions = get_questions_by_group_cached(group_id=question_group_id, session=session)
        if not questions:
            return False
        
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
        if gt_df.empty:
            return False
        
        gt_question_ids = set(gt_df["Question ID"].tolist())
        return any(question["id"] in gt_question_ids for question in questions)
    except:
        return False




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
        existing_answers = GroundTruthService.get_ground_truth_for_question_group(
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

###############################################################################
# ANNOTATOR PORTAL
###############################################################################

@handle_database_errors
def annotator_portal():
    st.title("👥 Annotator Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="annotator", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="annotator", session=session)

###############################################################################
# REVIEWER PORTAL
###############################################################################

@handle_database_errors
def reviewer_portal():
    st.title("🔍 Reviewer Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="reviewer", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="reviewer", session=session)

###############################################################################
# META-REVIEWER PORTAL
###############################################################################

@handle_database_errors
def meta_reviewer_portal():
    """Meta-Reviewer Portal for global admins"""
    st.title("🎯 Meta-Reviewer Portal")
    user = st.session_state.user
    
    current_view = st.session_state.get("current_view", "dashboard")
    
    with get_db_session() as session:
        if current_view == "dashboard":
            display_project_dashboard(user_id=user["id"], role="meta_reviewer", session=session)
        elif current_view == "project":
            display_project_view(user_id=user["id"], role="meta_reviewer", session=session)

###############################################################################
# ADMIN PORTAL - OPTIMIZED WITH FRAGMENTS
###############################################################################

@handle_database_errors
def admin_portal():
    st.title("⚙️ Admin Portal")
    
    tabs = st.tabs([
        "📹 Videos", "📁 Projects", "📋 Schemas", 
        "❓ Questions", "👥 Users", "🔗 Assignments", "📊 Project Groups"
    ])
    
    with tabs[0]:
        admin_videos()
    with tabs[1]:
        admin_projects()
    with tabs[2]:
        admin_schemas()
    with tabs[3]:
        admin_questions()
    with tabs[4]:
        admin_users()
    with tabs[5]:
        admin_assignments()
    with tabs[6]:
        admin_project_groups()

@st.fragment
def admin_videos():
    st.subheader("📹 Video Management")
    
    with get_db_session() as session:
        # Load all videos once to minimize database calls
        try:
            all_videos_df = VideoService.get_all_videos(session=session)
            
            if not all_videos_df.empty:
                # Calculate metrics
                total_videos = len(all_videos_df)
                active_videos = len(all_videos_df[~all_videos_df["Archived"]])
                archived_videos = total_videos - active_videos
                
                # Get videos not assigned to any project
                try:
                    # Get all videos that are assigned to projects
                    projects_df = ProjectService.get_all_projects(session=session)
                    assigned_video_ids = set()
                    
                    if not projects_df.empty:
                        for _, project in projects_df.iterrows():
                            project_videos = VideoService.get_project_videos(project_id=project["ID"], session=session)
                            assigned_video_ids.update(v["id"] for v in project_videos)
                    
                    unassigned_videos = all_videos_df[~all_videos_df["ID"].isin(assigned_video_ids)]
                    unassigned_count = len(unassigned_videos)
                except Exception as e:
                    unassigned_count = "Error"
                    print(f"Error calculating unassigned videos: {e}")
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📹 Total Videos", total_videos)
                with col2:
                    st.metric("✅ Active Videos", active_videos)
                with col3:
                    st.metric("🗄️ Archived Videos", archived_videos)
                with col4:
                    st.metric("📭 Unassigned Videos", unassigned_count)
            else:
                custom_info("No videos in the database yet.")
                total_videos = 0
                all_videos_df = pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading video summary: {str(e)}")
            total_videos = 0
            all_videos_df = pd.DataFrame()
        
        # Option to view full video table
        if total_videos > 0:
            with st.expander("📋 View All Videos (Database Table)", expanded=False):
                try:
                    # Add search functionality for the table
                    search_col1, search_col2, search_col3 = st.columns([2, 1, 1])
                    with search_col1:
                        video_search = st.text_input("🔍 Search videos", placeholder="Video UID, URL...", key="admin_video_search_table")
                    with search_col2:
                        show_archived = st.checkbox("Show archived videos", key="admin_show_archived_table")
                    with search_col3:
                        show_only_unassigned = st.checkbox("Show only unassigned", key="admin_show_unassigned_table", help="Show only videos not assigned to any project")
                    
                    # Filter videos (reuse the loaded data)
                    filtered_videos = all_videos_df.copy()
                    
                    if not show_archived:
                        filtered_videos = filtered_videos[~filtered_videos["Archived"]]
                    
                    if show_only_unassigned:
                        try:
                            # Recalculate unassigned for filtering (reuse logic from above)
                            projects_df = ProjectService.get_all_projects(session=session)
                            assigned_video_ids = set()
                            
                            if not projects_df.empty:
                                for _, project in projects_df.iterrows():
                                    project_videos = VideoService.get_project_videos(project_id=project["ID"], session=session)
                                    assigned_video_ids.update(v["id"] for v in project_videos)
                            
                            filtered_videos = filtered_videos[~filtered_videos["ID"].isin(assigned_video_ids)]
                        except Exception as e:
                            st.error(f"Error filtering unassigned videos: {str(e)}")
                    
                    if video_search:
                        mask = (
                            filtered_videos["Video UID"].str.contains(video_search, case=False, na=False) |
                            filtered_videos["URL"].str.contains(video_search, case=False, na=False)
                        )
                        filtered_videos = filtered_videos[mask]
                    
                    if not filtered_videos.empty:
                        custom_info(f"Showing {len(filtered_videos)} of {len(all_videos_df)} total videos")
                        
                        # Enhanced table display with project assignment info
                        display_videos = filtered_videos.copy()
                        
                        # Add project assignment info
                        if not show_only_unassigned:
                            try:
                                projects_df = ProjectService.get_all_projects(session=session)
                                video_project_map = {}
                                
                                if not projects_df.empty:
                                    for _, project in projects_df.iterrows():
                                        project_videos = VideoService.get_project_videos(project_id=project["ID"], session=session)
                                        for video in project_videos:
                                            video_id = video["id"]
                                            if video_id not in video_project_map:
                                                video_project_map[video_id] = []
                                            video_project_map[video_id].append(project["Name"])
                                
                                # Add project info to display
                                display_videos["Projects"] = display_videos["ID"].apply(
                                    lambda vid: ", ".join(video_project_map.get(vid, ["Unassigned"]))
                                )
                            except Exception as e:
                                display_videos["Projects"] = "Error loading"
                                print(f"Error adding project info: {e}")
                        
                        st.dataframe(display_videos, use_container_width=True)
                    else:
                        st.warning("No videos match your search criteria.")
                except Exception as e:
                    st.error(f"Error displaying video table: {str(e)}")
        
        video_tabs = st.tabs(["➕ Add Video", "✏️ Edit Video"])
        
        with video_tabs[0]:
            url = st.text_input("Video URL", key="admin_video_url")
            metadata_json = st.text_area("Metadata (JSON, optional)", "{}", key="admin_video_metadata")
            
            if st.button("Add Video", key="admin_add_video_btn"):
                if url:
                    try:
                        import json
                        metadata = json.loads(metadata_json) if metadata_json.strip() else {}
                        VideoService.add_video(url=url, session=session, metadata=metadata)
                        st.success("Video added!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        with video_tabs[1]:
            # For editing, allow editing ANY video in database (not just project-assigned ones)
            if not all_videos_df.empty:
                # Search for videos to edit
                edit_search = st.text_input("🔍 Search videos to edit", placeholder="Video UID...", key="admin_edit_video_search")
                
                filtered_edit_videos = all_videos_df.copy()
                if edit_search:
                    filtered_edit_videos = filtered_edit_videos[
                        filtered_edit_videos["Video UID"].str.contains(edit_search, case=False, na=False)
                    ]
                
                if not filtered_edit_videos.empty:
                    video_options = {f"{row['Video UID']} - {row['URL'][:50]}...": row['Video UID'] for _, row in filtered_edit_videos.iterrows()}
                    
                    if len(filtered_edit_videos) > 20:
                        custom_info(f"📊 Found {len(filtered_edit_videos)} videos. Use search to narrow results.")
                    
                    selected_video_display = st.selectbox(
                        "Select Video to Edit",
                        [""] + list(video_options.keys()),
                        key="admin_edit_video_select"
                    )
                    
                    if selected_video_display:
                        selected_video_uid = video_options[selected_video_display]
                        
                        try:
                            # Get current video details
                            current_video = VideoService.get_video_by_uid(video_uid=selected_video_uid, session=session)
                            
                            if current_video:
                                st.markdown(f"**Editing Video:** {selected_video_uid}")
                                
                                col1, col2 = st.columns([2, 1])
                                
                                with col1:
                                    new_url = st.text_input(
                                        "Video URL",
                                        value=current_video.url,
                                        key="admin_edit_video_url",
                                        help="Update the video URL"
                                    )
                                
                                with col2:
                                    st.markdown("**Current Video UID:**")
                                    st.code(selected_video_uid)
                                    st.caption("Video UID cannot be changed")
                                
                                # Metadata editing
                                st.markdown("**Video Metadata:**")
                                current_metadata = current_video.video_metadata or {}
                                
                                metadata_tab1, metadata_tab2 = st.tabs(["🎛️ Form Editor", "📝 JSON Editor"])
                                
                                with metadata_tab1:
                                    st.markdown("**Edit metadata fields:**")
                                    
                                    # Dynamic metadata form
                                    updated_metadata = {}
                                    
                                    # Show existing metadata fields
                                    for key, value in current_metadata.items():
                                        col_key, col_val, col_del = st.columns([1, 2, 0.3])
                                        with col_key:
                                            new_key = st.text_input(f"Key", value=key, key=f"meta_key_{key}")
                                        with col_val:
                                            if isinstance(value, (str, int, float)):
                                                new_value = st.text_input(f"Value", value=str(value), key=f"meta_val_{key}")
                                                try:
                                                    # Try to convert back to original type
                                                    if isinstance(value, int):
                                                        new_value = int(new_value)
                                                    elif isinstance(value, float):
                                                        new_value = float(new_value)
                                                except ValueError:
                                                    pass  # Keep as string
                                            else:
                                                new_value = st.text_area(f"Value (JSON)", value=str(value), key=f"meta_val_{key}")
                                                try:
                                                    import json
                                                    new_value = json.loads(new_value)
                                                except:
                                                    new_value = str(value)  # Fallback to string
                                        with col_del:
                                            if st.button("🗑️", key=f"del_{key}", help="Delete this field"):
                                                continue  # Skip adding to updated_metadata
                                        
                                        if new_key:  # Only add if key is not empty
                                            updated_metadata[new_key] = new_value
                                    
                                    # Add new metadata field
                                    st.markdown("**Add new field:**")
                                    col_new_key, col_new_val = st.columns(2)
                                    with col_new_key:
                                        new_field_key = st.text_input("New field key", key="admin_new_meta_key")
                                    with col_new_val:
                                        new_field_value = st.text_input("New field value", key="admin_new_meta_value")
                                    
                                    if new_field_key and new_field_value:
                                        # Try to parse as number if possible
                                        try:
                                            if '.' in new_field_value:
                                                updated_metadata[new_field_key] = float(new_field_value)
                                            else:
                                                updated_metadata[new_field_key] = int(new_field_value)
                                        except ValueError:
                                            updated_metadata[new_field_key] = new_field_value
                                
                                with metadata_tab2:
                                    import json
                                    metadata_json = st.text_area(
                                        "Metadata JSON",
                                        value=json.dumps(current_metadata, indent=2) if current_metadata else "{}",
                                        height=200,
                                        key="admin_edit_video_metadata_json",
                                        help="Edit metadata as JSON. This will override form editor changes."
                                    )
                                    
                                    try:
                                        json_metadata = json.loads(metadata_json)
                                        st.success("✅ Valid JSON")
                                        updated_metadata = json_metadata
                                    except json.JSONDecodeError as e:
                                        st.error(f"❌ Invalid JSON: {str(e)}")
                                        updated_metadata = current_metadata
                                
                                # Update button
                                update_col, preview_col = st.columns(2)
                                
                                with update_col:
                                    if st.button("💾 Update Video", key="admin_update_video_btn", use_container_width=True):
                                        try:
                                            # Use proper service methods
                                            VideoService.update_video(
                                                video_uid=selected_video_uid, 
                                                new_url=new_url, 
                                                new_metadata=updated_metadata, 
                                                session=session
                                            )
                                            
                                            st.success(f"✅ Video '{selected_video_uid}' updated successfully!")
                                            st.rerun(scope="fragment")
                                        except Exception as e:
                                            st.error(f"❌ Error updating video: {str(e)}")
                                
                                with preview_col:
                                    with st.expander("👁️ Preview Changes"):
                                        st.markdown("**New URL:**")
                                        st.code(new_url)
                                        st.markdown("**New Metadata:**")
                                        st.json(updated_metadata)
                            else:
                                st.error(f"Video with UID '{selected_video_uid}' not found")
                        except Exception as e:
                            st.error(f"Error loading video details: {str(e)}")
                    else:
                        custom_info("👆 Select a video from the dropdown above to edit")
                else:
                    if edit_search:
                        custom_infoing(f"No videos found matching '{edit_search}'")
                    else:
                        custom_info("Use the search box to find videos to edit")
            else:
                custom_info("No videos available to edit")
                
@st.fragment
def admin_questions():
    st.subheader("❓ Question & Group Management")
    
    with get_db_session() as session:
        q_tab1, q_tab2 = st.tabs(["📁 Question Groups", "❓ Individual Questions"])
        
        with q_tab1:
            groups_df = QuestionGroupService.get_all_groups(session=session)
            st.dataframe(groups_df, use_container_width=True)
            
            group_management_tabs = st.tabs(["➕ Create Group", "✏️ Edit Group"])
            
            with group_management_tabs[0]:
                st.markdown("### 🆕 Create New Question Group")
                
                # with basic_col1:
                title = st.text_input("Group Title", key="admin_group_title", placeholder="Enter group title...")
                basic_col1, basic_col2 = st.columns(2)
                with basic_col1:
                    is_reusable = st.checkbox("Reusable across schemas", key="admin_group_reusable", 
                                            help="Allow this group to be used in multiple schemas")
                with basic_col2:
                    is_auto_submit = st.checkbox("Auto Submit", key="admin_group_auto_submit", 
                                            help="Automatically submit answers for this group")
                description = st.text_area("Description", key="admin_group_description", 
                                         placeholder="Describe the purpose of this question group...")
                
                # Verification function selection
                st.markdown("**🔧 Verification Function (Optional):**")
                try:
                    available_functions = QuestionGroupService.get_available_verification_functions()
                    if available_functions:
                        verification_function = st.selectbox(
                            "Select verification function",
                            ["None"] + available_functions,
                            key="admin_group_verification",
                            help="Optional function to validate answers"
                        )
                        
                        if verification_function != "None":
                            try:
                                func_info = QuestionGroupService.get_verification_function_info(verification_function)
                                custom_info(f"Function: `{func_info['name']}{func_info['signature']}`")
                                if func_info['docstring']:
                                    st.markdown(f"**Documentation:** {func_info['docstring']}")
                            except Exception as e:
                                st.error(f"Error loading function info: {str(e)}")
                        
                        verification_function = verification_function if verification_function != "None" else None
                    else:
                        custom_info("No verification functions found in verify.py")
                        verification_function = None
                except Exception as e:
                    st.error(f"Error loading verification functions: {str(e)}")
                    verification_function = None
                
                st.markdown("**📋 Select Questions:**")
                questions_df = QuestionService.get_all_questions(session=session)
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    selected_questions = st.multiselect(
                        "Questions",
                        available_questions["ID"].tolist(),
                        format_func=lambda x: available_questions[available_questions["ID"]==x]["Text"].iloc[0],
                        key="admin_group_questions",
                        help="Select questions to include in this group"
                    )
                else:
                    selected_questions = []
                    custom_infoing("No questions available.")
                
                if st.button("🚀 Create Question Group", key="admin_create_group_btn", type="primary", use_container_width=True):
                    if title and selected_questions:
                        try:
                            QuestionGroupService.create_group(
                                title=title, description=description, is_reusable=is_reusable, 
                                question_ids=selected_questions, verification_function=verification_function, 
                                is_auto_submit=is_auto_submit, session=session
                            )
                            st.success("✅ Question group created successfully!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            
            with group_management_tabs[1]:
                st.markdown("### ✏️ Edit Existing Question Group")
                
                if not groups_df.empty:
                    available_groups = groups_df[~groups_df["Archived"]]
                    if not available_groups.empty:
                        group_options = {f"{row['Name']} (ID: {row['ID']})": row['ID'] for _, row in available_groups.iterrows()}
                        
                        # Check for selection change and clear state if needed
                        previous_selection = st.session_state.get("admin_edit_group_previous_selection")
                        
                        selected_group_name = st.selectbox(
                            "Select Group to Edit",
                            list(group_options.keys()),
                            key="admin_edit_group_select",
                            help="Choose a question group to modify"
                        )
                        
                        # If selection changed, clear related session state and rerun
                        if selected_group_name and selected_group_name != previous_selection:
                            st.session_state["admin_edit_group_previous_selection"] = selected_group_name
                            selected_group_id = group_options[selected_group_name]
                            
                            # Clear any existing order state for this group
                            order_key = f"edit_group_order_{selected_group_id}"
                            if order_key in st.session_state:
                                del st.session_state[order_key]
                            
                            st.rerun()
                        
                        if selected_group_name:
                            selected_group_id = group_options[selected_group_name]
                            
                            try:
                                group_details = QuestionGroupService.get_group_details_with_verification(
                                    group_id=selected_group_id, session=session
                                )
                                
                                current_verification = group_details.get("verification_function")
                                
                                edit_basic_col1, edit_basic_col2, edit_basic_col3 = st.columns(3)
                                with edit_basic_col1:
                                    new_title = st.text_input(
                                        "Group Title",
                                        value=group_details["title"],
                                        key="admin_edit_group_title"
                                    )
                                with edit_basic_col2:
                                    new_is_reusable = st.checkbox(
                                        "Reusable across schemas",
                                        value=group_details["is_reusable"],
                                        key="admin_edit_group_reusable"
                                    )
                                with edit_basic_col3:
                                    new_is_auto_submit = st.checkbox(
                                        "Auto Submit",
                                        value=group_details["is_auto_submit"],
                                        key="admin_edit_group_auto_submit"
                                    )
                                
                                new_description = st.text_area(
                                    "Description",
                                    value=group_details["description"] or "",
                                    key="admin_edit_group_description"
                                )
                                
                                # Verification function editing
                                st.markdown("**🔧 Verification Function:**")
                                try:
                                    available_functions = QuestionGroupService.get_available_verification_functions()
                                    verification_options = ["None"] + available_functions
                                    current_index = 0
                                    if current_verification and current_verification in available_functions:
                                        current_index = verification_options.index(current_verification)
                                    
                                    new_verification_function = st.selectbox(
                                        "Select verification function",
                                        verification_options,
                                        index=current_index,
                                        key="admin_edit_group_verification",
                                        help="Optional function to validate answers"
                                    )
                                    
                                    func_col1, func_col2 = st.columns(2)
                                    
                                    with func_col1:
                                        st.markdown("**Current Function:**")
                                        if current_verification:
                                            try:
                                                current_func_info = QuestionGroupService.get_verification_function_info(current_verification)
                                                st.code(f"{current_func_info['name']}{current_func_info['signature']}")
                                                if current_func_info['docstring']:
                                                    st.caption(f"**Doc:** {current_func_info['docstring']}")
                                            except Exception as e:
                                                st.error(f"Error loading current function: {str(e)}")
                                        else:
                                            custom_info("No verification function set")
                                    
                                    with func_col2:
                                        st.markdown("**New Function:**")
                                        if new_verification_function != "None":
                                            try:
                                                new_func_info = QuestionGroupService.get_verification_function_info(new_verification_function)
                                                st.code(f"{new_func_info['name']}{new_func_info['signature']}")
                                                if new_func_info['docstring']:
                                                    st.caption(f"**Doc:** {new_func_info['docstring']}")
                                            except Exception as e:
                                                st.error(f"Error loading function info: {str(e)}")
                                        else:
                                            custom_info("No verification function will be set")
                                    
                                    new_verification_function = new_verification_function if new_verification_function != "None" else None
                                    
                                except Exception as e:
                                    st.error(f"Error loading verification functions: {str(e)}")
                                    new_verification_function = current_verification
                                
                                # Question order management
                                st.markdown("**📋 Question Order Management:**")
                                current_order = QuestionGroupService.get_question_order(group_id=selected_group_id, session=session)
                                
                                if current_order:
                                    questions_df = QuestionService.get_all_questions(session=session)
                                    
                                    order_key = f"edit_group_order_{selected_group_id}"
                                    if order_key not in st.session_state:
                                        st.session_state[order_key] = current_order.copy()
                                    
                                    working_order = st.session_state[order_key]
                                    
                                    custom_info("💡 Use the ⬆️ and ⬇️ buttons to reorder questions. Changes will be applied when you click 'Update Group'.")
                                    
                                    if len(working_order) > 5:
                                        search_term = st.text_input(
                                            "🔍 Search questions (to quickly find questions in large groups)",
                                            key=f"search_questions_{selected_group_id}",
                                            placeholder="Type part of a question..."
                                        )
                                    else:
                                        search_term = ""
                                    
                                    for i, q_id in enumerate(working_order):
                                        question_row = questions_df[questions_df["ID"] == q_id]
                                        if not question_row.empty:
                                            q_text = question_row.iloc[0]["Text"]
                                            
                                            if search_term and search_term.lower() not in q_text.lower():
                                                continue
                                            
                                            order_col1, order_col2, order_col3 = st.columns([0.1, 0.8, 0.1])
                                            
                                            with order_col1:
                                                if st.button("⬆️", key=f"up_{selected_group_id}_{q_id}_{i}", 
                                                            disabled=(i == 0), help="Move up"):
                                                    st.session_state[order_key][i], st.session_state[order_key][i-1] = \
                                                        st.session_state[order_key][i-1], st.session_state[order_key][i]
                                                    st.rerun()
                                            
                                            with order_col2:
                                                display_text = q_text[:80] + ('...' if len(q_text) > 80 else '')
                                                if search_term and search_term.lower() in q_text.lower():
                                                    st.write(f"**{i+1}.** {display_text} 🔍")
                                                else:
                                                    st.write(f"**{i+1}.** {display_text}")
                                                st.caption(f"ID: {q_id}")
                                            
                                            with order_col3:
                                                if st.button("⬇️", key=f"down_{selected_group_id}_{q_id}_{i}", 
                                                            disabled=(i == len(working_order) - 1), help="Move down"):
                                                    st.session_state[order_key][i], st.session_state[order_key][i+1] = \
                                                        st.session_state[order_key][i+1], st.session_state[order_key][i]
                                                    st.rerun()
                                    
                                    order_action_col1, order_action_col2 = st.columns(2)
                                    with order_action_col1:
                                        if st.button("🔄 Reset Order", key=f"reset_order_{selected_group_id}"):
                                            st.session_state[order_key] = current_order.copy()
                                            st.rerun()
                                    
                                    with order_action_col2:
                                        if working_order != current_order:
                                            st.warning("⚠️ Order changed - click 'Update Group' to save")
                                        else:
                                            st.success("✅ Order matches saved state")
                                    
                                    new_order = working_order
                                else:
                                    new_order = current_order
                                    custom_info("No questions in this group.")
                                
                                if st.button("💾 Update Question Group", key="admin_update_group_btn", type="primary", use_container_width=True):
                                    try:
                                        QuestionGroupService.edit_group(
                                            group_id=selected_group_id, new_title=new_title,
                                            new_description=new_description, is_reusable=new_is_reusable,
                                            verification_function=new_verification_function, is_auto_submit=new_is_auto_submit, session=session
                                        )
                                        
                                        if new_order != current_order:
                                            QuestionGroupService.update_question_order(
                                                group_id=selected_group_id, question_ids=new_order, session=session
                                            )
                                        
                                        order_key = f"edit_group_order_{selected_group_id}"
                                        if order_key in st.session_state:
                                            del st.session_state[order_key]
                                        
                                        st.success("✅ Question group updated successfully!")
                                        st.rerun(scope="fragment")
                                    except Exception as e:
                                        st.error(f"❌ Error updating group: {str(e)}")
                                        
                            except Exception as e:
                                st.error(f"Error loading group details: {str(e)}")
                    else:
                        custom_info("No non-archived question groups available to edit.")
                else:
                    custom_info("No question groups available to edit.")
        
        with q_tab2:
            questions_df = QuestionService.get_all_questions(session=session)
            st.dataframe(questions_df, use_container_width=True)
            
            question_management_tabs = st.tabs(["➕ Create Question", "✏️ Edit Question"])
            
            with question_management_tabs[0]:
                st.markdown("### 🆕 Create New Question")
                
                basic_info_col1, basic_info_col2 = st.columns(2)
                with basic_info_col1:
                    text = st.text_input("Question Text", key="admin_question_text", 
                                       placeholder="Enter the question text...")
                with basic_info_col2:
                    q_type = st.selectbox("Question Type", ["single", "description"], key="admin_question_type",
                                        help="Single: Multiple choice | Description: Text input")
                
                use_text_as_display = st.checkbox("Use question text as display text", value=True, 
                                                key="admin_question_use_text_as_display",
                                                help="Uncheck to provide custom display text")
                if not use_text_as_display:
                    display_text = st.text_input("Question to display to user", key="admin_question_display_text", 
                                                value=text, placeholder="Text shown to users...")
                else:
                    display_text = None
                
                options = []
                option_weights = []
                default = None
                
                if q_type == "single":
                    st.markdown("**🎯 Options and Weights:**")
                    custom_info("💡 Default weight is 1.0 for each option. Customize weights to influence scoring.")
                    
                    num_options = st.number_input("Number of options", 1, 10, 2, key="admin_question_num_options")
                    
                    for i in range(num_options):
                        opt_col1, opt_col2 = st.columns([3, 1])
                        with opt_col1:
                            option = st.text_input(f"Option {i+1}", key=f"admin_question_opt_{i}",
                                                 placeholder=f"Enter option {i+1}...")
                        with opt_col2:
                            weight = st.number_input(f"Weight {i+1}", min_value=0.0, value=1.0, step=0.1, 
                                                   key=f"admin_question_weight_{i}", 
                                                   help="Weight for scoring (default: 1.0)")
                        
                        if option:
                            options.append(option)
                            option_weights.append(weight)
                    
                    if options:
                        default = st.selectbox("Default option", [""] + options, key="admin_question_default",
                                             help="Option selected by default")
                        if default == "":
                            default = None
                
                if st.button("🚀 Create Question", key="admin_create_question_btn", type="primary", use_container_width=True):
                    if text:
                        try:
                            QuestionService.add_question(
                                text=text, qtype=q_type, options=options if q_type == "single" else None,
                                default=default if q_type == "single" else None, session=session,
                                display_text=display_text, option_weights=option_weights if q_type == "single" else None
                            )
                            st.success("✅ Question created successfully!")
                            st.rerun(scope="fragment")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            
            with question_management_tabs[1]:
                st.markdown("### ✏️ Edit Existing Question")
                
                if not questions_df.empty:
                    available_questions = questions_df[~questions_df["Archived"]]
                    if not available_questions.empty:
                        question_options = {f"{row['Text'][:50]}... (ID: {row['ID']})": row['ID'] for _, row in available_questions.iterrows()}
                        selected_question_name = st.selectbox(
                            "Select Question to Edit",
                            list(question_options.keys()),
                            key="admin_edit_question_select",
                            help="Choose a question to modify"
                        )
                        
                        if selected_question_name:
                            selected_question_id = question_options[selected_question_name]
                            
                            try:
                                current_question = QuestionService.get_question_by_id(
                                    question_id=selected_question_id, session=session
                                )
                                
                                st.text_input(
                                    "Question Text (immutable)",
                                    value=current_question["text"],
                                    key="admin_edit_question_text",
                                    disabled=True,
                                    help="Question text cannot be changed to preserve data integrity"
                                )
                                
                                new_display_text = st.text_input(
                                    "Question to display to user",
                                    value=current_question["display_text"],
                                    key="admin_edit_question_display_text"
                                )
                                
                                st.markdown(f"**Question Type:** `{current_question['type']}`")
                                
                                new_options = None
                                new_default = None
                                new_display_values = None
                                new_option_weights = None
                                
                                if current_question["type"] == "single":
                                    st.markdown("**🎯 Options, Weights & Order Management:**")
                                    
                                    current_options = current_question["options"] or []
                                    current_display_values = current_question["display_values"] or current_options
                                    current_option_weights = current_question["option_weights"] or [1.0] * len(current_options)
                                    current_default = current_question["default_option"] or ""
                                    
                                    # Current options display
                                    st.markdown("**Current Options:**")
                                    for i, (opt, disp, weight) in enumerate(zip(current_options, current_display_values, current_option_weights)):
                                        default_indicator = " 🌟 (DEFAULT)" if opt == current_default else ""
                                        st.markdown(f"`{i+1}.` **Value:** `{opt}` | **Display:** `{disp}` | **Weight:** `{weight}`{default_indicator}")
                                    
                                    # Option order management
                                    st.markdown("**📋 Option Order Management:**")
                                    option_order_key = f"edit_question_option_order_{selected_question_id}"
                                    if option_order_key not in st.session_state:
                                        st.session_state[option_order_key] = list(range(len(current_options)))
                                    
                                    working_option_order = st.session_state[option_order_key]
                                    
                                    if len(current_options) > 1:
                                        custom_info("💡 Use the ⬆️ and ⬇️ buttons to reorder options. This will affect the display order for users.")
                                        
                                        for i, option_idx in enumerate(working_option_order):
                                            if option_idx < len(current_options):
                                                opt = current_options[option_idx]
                                                disp = current_display_values[option_idx]
                                                weight = current_option_weights[option_idx]
                                                
                                                order_opt_col1, order_opt_col2, order_opt_col3 = st.columns([0.1, 0.8, 0.1])
                                                
                                                with order_opt_col1:
                                                    if st.button("⬆️", key=f"opt_up_{selected_question_id}_{option_idx}_{i}", 
                                                                disabled=(i == 0), help="Move up"):
                                                        st.session_state[option_order_key][i], st.session_state[option_order_key][i-1] = \
                                                            st.session_state[option_order_key][i-1], st.session_state[option_order_key][i]
                                                        st.rerun()
                                                
                                                with order_opt_col2:
                                                    default_indicator = " 🌟" if opt == current_default else ""
                                                    st.write(f"**{i+1}.** {disp} (Weight: {weight}){default_indicator}")
                                                    st.caption(f"Value: {opt}")
                                                
                                                with order_opt_col3:
                                                    if st.button("⬇️", key=f"opt_down_{selected_question_id}_{option_idx}_{i}", 
                                                                disabled=(i == len(working_option_order) - 1), help="Move down"):
                                                        st.session_state[option_order_key][i], st.session_state[option_order_key][i+1] = \
                                                            st.session_state[option_order_key][i+1], st.session_state[option_order_key][i]
                                                        st.rerun()
                                        
                                        opt_order_col1, opt_order_col2 = st.columns(2)
                                        with opt_order_col1:
                                            if st.button("🔄 Reset Option Order", key=f"reset_option_order_{selected_question_id}"):
                                                st.session_state[option_order_key] = list(range(len(current_options)))
                                                st.rerun()
                                        
                                        with opt_order_col2:
                                            original_order = list(range(len(current_options)))
                                            if working_option_order != original_order:
                                                st.warning("⚠️ Option order changed")
                                            else:
                                                st.success("✅ Original order")
                                    
                                    # Edit options and add new ones
                                    st.markdown("**✏️ Edit Options and Weights:**")
                                    custom_info("📝 Note: You can only add new options, not remove existing ones (to preserve data integrity).")
                                    
                                    num_options = st.number_input(
                                        "Total number of options", 
                                        min_value=len(current_options), 
                                        max_value=10, 
                                        value=len(current_options),
                                        key="admin_edit_question_num_options"
                                    )
                                    
                                    new_options = []
                                    new_display_values = []
                                    new_option_weights = []
                                    
                                    # Apply the working order to existing options
                                    reordered_options = [current_options[i] for i in working_option_order]
                                    reordered_display_values = [current_display_values[i] for i in working_option_order]
                                    reordered_weights = [current_option_weights[i] for i in working_option_order]
                                    
                                    for i in range(num_options):
                                        edit_opt_col1, edit_opt_col2, edit_opt_col3 = st.columns([2, 2, 1])
                                        
                                        with edit_opt_col1:
                                            if i < len(reordered_options):
                                                st.text_input(
                                                    f"Option {i+1} Value",
                                                    value=reordered_options[i],
                                                    disabled=True,
                                                    key=f"admin_edit_question_opt_val_{i}",
                                                    help="Cannot change existing option values"
                                                )
                                                new_options.append(reordered_options[i])
                                            else:
                                                new_opt = st.text_input(
                                                    f"Option {i+1} Value (NEW)",
                                                    key=f"admin_edit_question_opt_val_{i}",
                                                    placeholder="Enter new option value..."
                                                )
                                                if new_opt:
                                                    new_options.append(new_opt)
                                        
                                        with edit_opt_col2:
                                            if i < len(reordered_display_values):
                                                new_disp = st.text_input(
                                                    f"Option {i+1} Display",
                                                    value=reordered_display_values[i],
                                                    key=f"admin_edit_question_opt_disp_{i}"
                                                )
                                                new_display_values.append(new_disp if new_disp else reordered_display_values[i])
                                            else:
                                                new_disp = st.text_input(
                                                    f"Option {i+1} Display (NEW)",
                                                    value=new_options[i] if i < len(new_options) else "",
                                                    key=f"admin_edit_question_opt_disp_{i}",
                                                    placeholder="Display text for new option..."
                                                )
                                                if new_disp:
                                                    new_display_values.append(new_disp)
                                                elif i < len(new_options):
                                                    new_display_values.append(new_options[i])
                                        
                                        with edit_opt_col3:
                                            if i < len(reordered_weights):
                                                new_weight = st.number_input(
                                                    f"Weight {i+1}",
                                                    min_value=0.0,
                                                    value=reordered_weights[i],
                                                    step=0.1,
                                                    key=f"admin_edit_question_opt_weight_{i}",
                                                    help="Weight for scoring"
                                                )
                                                new_option_weights.append(new_weight)
                                            else:
                                                new_weight = st.number_input(
                                                    f"Weight {i+1} (NEW)",
                                                    min_value=0.0,
                                                    value=1.0,
                                                    step=0.1,
                                                    key=f"admin_edit_question_opt_weight_{i}",
                                                    help="Weight for scoring (default: 1.0)"
                                                )
                                                new_option_weights.append(new_weight)
                                    
                                    if new_options:
                                        new_default = st.selectbox(
                                            "Default option",
                                            [""] + new_options,
                                            index=new_options.index(current_default) + 1 if current_default in new_options else 0,
                                            key="admin_edit_question_default"
                                        )
                                        if new_default == "":
                                            new_default = None
                                
                                if st.button("💾 Update Question", key="admin_update_question_btn", type="primary", use_container_width=True):
                                    try:
                                        # Apply option reordering if changed
                                        final_options = new_options
                                        final_display_values = new_display_values
                                        final_option_weights = new_option_weights
                                        
                                        # Clear option order state after update
                                        option_order_key = f"edit_question_option_order_{selected_question_id}"
                                        if option_order_key in st.session_state:
                                            del st.session_state[option_order_key]
                                        
                                        QuestionService.edit_question(
                                            question_id=selected_question_id, new_display_text=new_display_text,
                                            new_opts=final_options, new_default=new_default,
                                            new_display_values=final_display_values, new_option_weights=final_option_weights,
                                            session=session
                                        )
                                        st.success("✅ Question updated successfully!")
                                        st.rerun(scope="fragment")
                                    except Exception as e:
                                        st.error(f"❌ Error updating question: {str(e)}")
                                        
                            except Exception as e:
                                st.error(f"Error loading question details: {str(e)}")
                    else:
                        custom_info("No non-archived questions available to edit.")
                else:
                    custom_info("No questions available to edit.")

@st.fragment 
def display_assignment_management(session: Session):
    """Optimized assignment management with fragments and user weight support"""
    st.markdown("### 🎯 Assign Users to Projects")
    
    # Initialize session state
    if "selected_project_ids" not in st.session_state:
        st.session_state.selected_project_ids = []
    if "selected_user_ids" not in st.session_state:
        st.session_state.selected_user_ids = []
    if "assignment_role" not in st.session_state:
        st.session_state.assignment_role = "annotator"
    if "assignment_user_weight" not in st.session_state:
        st.session_state.assignment_user_weight = 1.0
    
    # Project selection
    st.markdown("**Step 1: Select Projects**")
    
    try:
        projects_df = ProjectService.get_all_projects(session=session)
        if projects_df.empty:
            custom_infoing("No projects available.")
            return
    except Exception as e:
        st.error(f"Error loading projects: {str(e)}")
        return
    
    project_search = st.text_input("🔍 Search projects", placeholder="Project name...", key="proj_search_mgmt")
    
    filtered_projects = [
        project_row for _, project_row in projects_df.iterrows()
        if not project_search or project_search.lower() in project_row["Name"].lower()
    ]
    
    if not filtered_projects:
        custom_infoing("No projects match the search criteria.")
        return
    
    custom_info(f"Found {len(filtered_projects)} projects")
    
    select_col1, select_col2 = st.columns(2)
    with select_col1:
        if st.button("Select All Visible Projects", key="select_all_projects_mgmt"):
            st.session_state.selected_project_ids = [int(p["ID"]) for p in filtered_projects]
            st.rerun(scope="fragment")
    
    with select_col2:
        if st.button("Clear Project Selection", key="clear_projects_mgmt"):
            st.session_state.selected_project_ids = []
            st.rerun(scope="fragment")
    
    # Project selection grid
    project_cols = st.columns(4)
    current_selections = []
    
    for i, project_row in enumerate(filtered_projects):
        with project_cols[i % 4]:
            project_id = int(project_row["ID"])
            project_name = project_row["Name"]
            
            is_selected = project_id in st.session_state.selected_project_ids
            
            checkbox_value = st.checkbox(
                project_name,
                value=is_selected,
                key=f"proj_cb_mgmt_{project_id}",
                help=f"Project ID: {project_id}"
            )
            
            if checkbox_value:
                current_selections.append(project_id)
    
    if set(current_selections) != set(st.session_state.selected_project_ids):
        st.session_state.selected_project_ids = current_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_project_ids:
        custom_info("Please select projects above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_project_ids)} projects")
    
    # User selection
    st.markdown("**Step 2: Select Users**")
    
    try:
        users_df = AuthService.get_all_users(session=session)
        if users_df.empty:
            custom_infoing("No users available.")
            return
    except Exception as e:
        st.error(f"Error loading users: {str(e)}")
        return
    
    user_filter_col1, user_filter_col2 = st.columns(2)
    with user_filter_col1:
        user_search = st.text_input("Search users", placeholder="Name or email...", key="user_search_mgmt")
    with user_filter_col2:
        user_role_filter = st.selectbox("Filter by user role", ["All", "admin", "human", "model"], key="user_role_filter_mgmt")
    
    # Filter users
    filtered_users = []
    for _, user_row in users_df.iterrows():
        if user_role_filter != "All" and user_row["Role"] != user_role_filter:
            continue
        
        if user_search:
            if user_search.lower() not in user_row["User ID"].lower():
                if user_row["Email"] and user_search.lower() not in user_row["Email"].lower():
                    continue
        
        filtered_users.append(user_row)
    
    if not filtered_users:
        custom_infoing("No users match the search criteria.")
        return
    
    custom_info(f"Found {len(filtered_users)} users")
    
    # User pagination
    users_per_page = 12
    total_pages = (len(filtered_users) - 1) // users_per_page + 1 if len(filtered_users) > 0 else 1
    
    if total_pages > 1:
        page = st.selectbox(f"Page (showing {users_per_page} users per page)", 
                           range(1, total_pages + 1), key="user_page_mgmt") - 1
    else:
        page = 0
    
    start_idx = page * users_per_page
    end_idx = min(start_idx + users_per_page, len(filtered_users))
    page_users = filtered_users[start_idx:end_idx]
    
    user_select_col1, user_select_col2 = st.columns(2)
    with user_select_col1:
        if st.button("Select All on Page", key="select_all_users_mgmt"):
            page_user_ids = [int(u["ID"]) for u in page_users]
            st.session_state.selected_user_ids = list(set(st.session_state.selected_user_ids + page_user_ids))
            st.rerun(scope="fragment")
    
    with user_select_col2:
        if st.button("Clear User Selection", key="clear_users_mgmt"):
            st.session_state.selected_user_ids = []
            st.rerun(scope="fragment")
    
    # User selection grid
    user_cols = st.columns(4)
    current_user_selections = list(st.session_state.selected_user_ids)
    
    for i, user_row in enumerate(page_users):
        with user_cols[i % 4]:
            user_id = int(user_row["ID"])
            user_name = user_row["User ID"]
            user_email = user_row["Email"]
            user_role = user_row["Role"]
            
            is_selected = user_id in st.session_state.selected_user_ids
            
            checkbox_value = st.checkbox(
                user_name,
                value=is_selected,
                key=f"user_cb_mgmt_{user_id}",
                help=f"Email: {user_email}\nRole: {user_role}\nID: {user_id}"
            )
            
            if checkbox_value and user_id not in current_user_selections:
                current_user_selections.append(user_id)
            elif not checkbox_value and user_id in current_user_selections:
                current_user_selections.remove(user_id)
    
    if set(current_user_selections) != set(st.session_state.selected_user_ids):
        st.session_state.selected_user_ids = current_user_selections
        st.rerun(scope="fragment")
    
    if not st.session_state.selected_user_ids:
        custom_info("Please select users above to continue.")
        return
    
    st.success(f"✅ Selected {len(st.session_state.selected_user_ids)} users")
    
    # Assignment actions
    st.markdown("**Step 3: Assignment Role & Settings**")
    
    settings_col1, settings_col2 = st.columns(2)
    
    with settings_col1:
        role = st.selectbox("Assignment Role", ["annotator", "reviewer", "admin", "model"], 
                           index=["annotator", "reviewer", "admin", "model"].index(st.session_state.assignment_role),
                           key="assign_role_mgmt")
    
    with settings_col2:
        user_weight = st.number_input(
            "User Weight", 
            min_value=0.0, 
            value=st.session_state.assignment_user_weight, 
            step=0.1,
            key="assign_user_weight_mgmt",
            help="Weight for user's answers in scoring (default: 1.0)"
        )
    
    if role != st.session_state.assignment_role:
        st.session_state.assignment_role = role
    
    if user_weight != st.session_state.assignment_user_weight:
        st.session_state.assignment_user_weight = user_weight
    
    custom_info(f"Ready to assign {len(st.session_state.selected_user_ids)} users as {role} with weight {user_weight} to {len(st.session_state.selected_project_ids)} projects")
    
    action_col1, action_col2 = st.columns(2)
    
    with action_col1:
        if st.button("✅ Execute Assignments", key="execute_assignments", use_container_width=True):
            project_ids = st.session_state.selected_project_ids
            user_ids = st.session_state.selected_user_ids
            total_operations = len(user_ids) * len(project_ids)
            success_count = 0
            error_count = 0
            
            progress_bar = st.progress(0)
            status_container = st.empty()
            
            operation_counter = 0
            
            for project_id in project_ids:
                for user_id in user_ids:
                    try:
                        ProjectService.add_user_to_project(
                            project_id=project_id, user_id=user_id, role=role, 
                            session=session, user_weight=user_weight
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 3:
                            st.error(f"Failed to assign user {user_id} to project {project_id}: {str(e)}")
                    
                    operation_counter += 1
                    progress = operation_counter / total_operations
                    progress_bar.progress(progress)
                    status_container.text(f"Processing: {operation_counter}/{total_operations}")
            
            if success_count > 0:
                st.success(f"✅ Successfully completed {success_count} assignments with weight {user_weight}!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} assignments failed")
            
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")
    
    with action_col2:
        if st.button("🗑️ Remove Assignments", key="execute_removals", use_container_width=True):
            project_ids = st.session_state.selected_project_ids
            user_ids = st.session_state.selected_user_ids
            total_operations = len(user_ids) * len(project_ids)
            success_count = 0
            error_count = 0
            
            progress_bar = st.progress(0)
            status_container = st.empty()
            
            operation_counter = 0
            
            for project_id in project_ids:
                for user_id in user_ids:
                    try:
                        AuthService.archive_user_from_project(user_id=user_id, project_id=project_id, session=session)
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count <= 3:
                            st.error(f"Failed to remove user {user_id} from project {project_id}: {str(e)}")
                    
                    operation_counter += 1
                    progress = operation_counter / total_operations
                    progress_bar.progress(progress)
                    status_container.text(f"Processing: {operation_counter}/{total_operations}")
            
            if success_count > 0:
                st.success(f"🗑️ Successfully removed {success_count} assignments!")
            if error_count > 0:
                st.warning(f"⚠️ {error_count} removals failed")
            
            if success_count > 0:
                st.session_state.selected_project_ids = []
                st.session_state.selected_user_ids = []
                st.rerun(scope="fragment")

@st.fragment
def admin_projects():
    st.subheader("📁 Project Management")
    
    with get_db_session() as session:
        projects_df = ProjectService.get_all_projects(session=session)
        
        if not projects_df.empty:
            enhanced_projects = []
            for _, project in projects_df.iterrows():
                try:
                    progress = ProjectService.progress(project_id=project["ID"], session=session)
                    has_full_gt = check_project_has_full_ground_truth(project_id=project["ID"], session=session)
                    mode = "🎓 Training" if has_full_gt else "📝 Annotation"
                    
                    enhanced_projects.append({
                        "ID": project["ID"],
                        "Name": project["Name"],
                        "Videos": project["Videos"],
                        "Schema ID": project["Schema ID"],
                        "Mode": mode,
                        "GT Progress": f"{progress['completion_percentage']:.1f}%",
                        "GT Answers": f"{progress['ground_truth_answers']}/{progress['total_videos'] * progress['total_questions']}"
                    })
                except:
                    enhanced_projects.append({
                        "ID": project["ID"],
                        "Name": project["Name"],
                        "Videos": project["Videos"],
                        "Schema ID": project["Schema ID"],
                        "Mode": "Error",
                        "GT Progress": "Error",
                        "GT Answers": "Error"
                    })
            
            enhanced_df = pd.DataFrame(enhanced_projects)
            st.dataframe(enhanced_df, use_container_width=True)
        else:
            custom_info("No projects available.")
        
        with st.expander("➕ Create Project"):
            name = st.text_input("Project Name", key="admin_project_name")
            description = st.text_area("Description", key="admin_project_description")
            
            schemas_df = SchemaService.get_all_schemas(session=session)
            if schemas_df.empty:
                st.warning("No schemas available.")
                return
                
            schema_name = st.selectbox("Schema", schemas_df["Name"], key="admin_project_schema")
            
            videos_df = VideoService.get_all_videos(session=session)
            if videos_df.empty:
                st.warning("No videos available.")
                return
                
            selected_videos = st.multiselect("Videos", videos_df["Video UID"], key="admin_project_videos")
            
            if st.button("Create Project", key="admin_create_project_btn"):
                if name and schema_name:
                    try:
                        schema_id = SchemaService.get_schema_id_by_name(name=schema_name, session=session)
                        video_ids = ProjectService.get_video_ids_by_uids(video_uids=selected_videos, session=session)
                        ProjectService.create_project(name=name, schema_id=schema_id, video_ids=video_ids, session=session)
                        st.success("Project created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_schemas():
    st.subheader("📋 Schema Management")
    
    with get_db_session() as session:
        schemas_df = SchemaService.get_all_schemas(session=session)
        st.dataframe(schemas_df, use_container_width=True)
        
        with st.expander("➕ Create Schema"):
            schema_name = st.text_input("Schema Name", key="admin_schema_name")
            
            groups_df = QuestionGroupService.get_all_groups(session=session)
            if groups_df.empty:
                st.warning("No question groups available.")
                return
                
            available_groups = groups_df[~groups_df["Archived"]]
            selected_groups = st.multiselect(
                "Question Groups", 
                available_groups["ID"].tolist(),
                format_func=lambda x: available_groups[available_groups["ID"]==x]["Name"].iloc[0],
                key="admin_schema_groups"
            )
            
            if st.button("Create Schema", key="admin_create_schema_btn"):
                if schema_name and selected_groups:
                    try:
                        SchemaService.create_schema(name=schema_name, question_group_ids=selected_groups, session=session)
                        st.success("Schema created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")

@st.fragment
def admin_users():
    st.subheader("👥 User Management")
    
    with get_db_session() as session:
        users_df = AuthService.get_all_users(session=session)
        st.dataframe(users_df, use_container_width=True)
        
        create_tab, edit_tab = st.tabs(["➕ Create User", "✏️ Edit User"])
        
        with create_tab:
            user_id = st.text_input("User ID", key="admin_user_id")
            email = st.text_input("Email", key="admin_user_email")
            password = st.text_input("Password", type="password", key="admin_user_password")
            user_type = st.selectbox("User Type", ["human", "model", "admin"], key="admin_user_type")
            
            if st.button("Create User", key="admin_create_user_btn"):
                if user_id and email and password:
                    try:
                        AuthService.create_user(
                            user_id=user_id, email=email, password_hash=password, 
                            user_type=user_type, session=session
                        )
                        st.success(f"✅ User '{user_id}' created successfully!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                else:
                    custom_infoing("Please fill in all required fields (User ID, Email, Password)")
        
        with edit_tab:
            if not users_df.empty:
                user_options = {f"{row['User ID']} ({row['Email']})": row['ID'] for _, row in users_df.iterrows()}
                selected_user_display = st.selectbox(
                    "Select User to Edit",
                    list(user_options.keys()),
                    key="admin_edit_user_select"
                )
                
                if selected_user_display:
                    selected_user_id = user_options[selected_user_display]
                    current_user = users_df[users_df['ID'] == selected_user_id].iloc[0]
                    
                    st.markdown(f"**Editing User:** {current_user['User ID']}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        new_user_id = st.text_input(
                            "User ID",
                            value=current_user['User ID'],
                            key="admin_edit_user_id"
                        )
                        new_email = st.text_input(
                            "Email",
                            value=current_user['Email'],
                            key="admin_edit_user_email"
                        )
                    
                    with col2:
                        new_role = st.selectbox(
                            "User Type",
                            ["human", "model", "admin"],
                            index=["human", "model", "admin"].index(current_user['Role']),
                            key="admin_edit_user_role"
                        )
                        new_password = st.text_input(
                            "New Password (leave empty to keep current)",
                            type="password",
                            key="admin_edit_user_password"
                        )
                    
                    update_col, archive_col = st.columns(2)
                    
                    with update_col:
                        if st.button("💾 Update User", key="admin_update_user_btn", use_container_width=True):
                            if new_user_id and new_email:
                                try:
                                    changes_made = []
                                    
                                    if new_user_id != current_user['User ID']:
                                        AuthService.update_user_id(
                                            user_id=selected_user_id, new_user_id=new_user_id, session=session
                                        )
                                        changes_made.append("User ID")
                                    
                                    if new_email != current_user['Email']:
                                        AuthService.update_user_email(
                                            user_id=selected_user_id, new_email=new_email, session=session
                                        )
                                        changes_made.append("Email")
                                    
                                    if new_password:
                                        AuthService.update_user_password(
                                            user_id=selected_user_id, new_password=new_password, session=session
                                        )
                                        changes_made.append("Password")
                                    
                                    if new_role != current_user['Role']:
                                        AuthService.update_user_role(
                                            user_id=selected_user_id, new_role=new_role, session=session
                                        )
                                        changes_made.append("Role")
                                    
                                    if changes_made:
                                        st.success(f"✅ User '{new_user_id}' updated successfully! Changed: {', '.join(changes_made)}")
                                    else:
                                        custom_info("No changes were made")
                                    st.rerun(scope="fragment")
                                except Exception as e:
                                    st.error(f"Error updating user: {str(e)}")
                            else:
                                custom_infoing("User ID and Email are required")
                    
                    with archive_col:
                        archive_status = "Archived" if current_user['Archived'] else "Active"
                        action_text = "Unarchive" if current_user['Archived'] else "Archive"
                        
                        if st.button(f"🗄️ {action_text} User", key="admin_archive_user_btn", use_container_width=True):
                            if st.session_state.get(f"confirm_archive_{selected_user_id}", False):
                                try:
                                    AuthService.toggle_user_archived(user_id=selected_user_id, session=session)
                                    new_status = "Unarchived" if current_user['Archived'] else "Archived"
                                    st.success(f"✅ User '{current_user['User ID']}' {new_status.lower()} successfully!")
                                    if f"confirm_archive_{selected_user_id}" in st.session_state:
                                        del st.session_state[f"confirm_archive_{selected_user_id}"]
                                    st.rerun(scope="fragment")
                                except Exception as e:
                                    st.error(f"Error toggling user archive status: {str(e)}")
                            else:
                                st.session_state[f"confirm_archive_{selected_user_id}"] = True
                                st.warning(f"⚠️ Click {action_text} again to confirm")
                                st.rerun(scope="fragment")
                        
                        st.caption(f"**Current Status:** {archive_status}")
            else:
                custom_info("No users available to edit")

@st.fragment 
def admin_assignments():
    st.subheader("🔗 Project Assignments")
    
    def get_user_role_emoji(role):
        """Get appropriate emoji for user role"""
        role_emojis = {"human": "👤", "admin": "👑", "model": "🤖"}
        return role_emojis.get(role, "❓")
    
    with get_db_session() as session:
        assignments_df = AuthService.get_project_assignments(session=session)
        
        if not assignments_df.empty:
            with st.expander("➕ **Manage Project Assignments**", expanded=False):
                display_assignment_management(session)
            
            st.markdown("---")
            
            # Get data for enhanced display
            users_df = AuthService.get_all_users(session=session)
            projects_df = ProjectService.get_all_projects(session=session)
            
            # Get project groups
            try:
                project_groups = ProjectGroupService.list_project_groups(session=session)
                project_group_lookup = {}
                for group in project_groups:
                    group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                    for project in group_info["projects"]:
                        project_group_lookup[project.id] = group.name
            except:
                project_group_lookup = {}
            
            # Create lookups
            user_lookup = {row["ID"]: {"name": row["User ID"], "email": row["Email"], "role": row["Role"]} for _, row in users_df.iterrows()}
            project_lookup = {row["ID"]: row["Name"] for _, row in projects_df.iterrows()}
            
            # Process assignments
            user_assignments = {}
            
            for _, assignment in assignments_df.iterrows():
                user_id = assignment["User ID"]
                project_id = assignment["Project ID"]
                user_info = user_lookup.get(user_id, {"name": f"Unknown User {user_id}", "email": "Unknown", "role": "Unknown"})
                project_name = project_lookup.get(project_id, f"Unknown Project {project_id}")
                project_group = project_group_lookup.get(project_id, "Ungrouped")
                
                if user_id not in user_assignments:
                    user_assignments[user_id] = {
                        "name": user_info["name"],
                        "email": user_info["email"],
                        "user_role": user_info["role"],
                        "projects": {},
                        "is_archived": True
                    }
                
                project_key = project_id
                if project_key not in user_assignments[user_id]["projects"]:
                    user_assignments[user_id]["projects"][project_key] = {
                        "name": project_name,
                        "group": project_group,
                        "role_assignments": {}
                    }
                
                role = assignment["Role"]
                if role not in user_assignments[user_id]["projects"][project_key]["role_assignments"]:
                    user_assignments[user_id]["projects"][project_key]["role_assignments"][role] = {
                        "assigned_date": None,
                        "completed_date": None,
                        "archived": assignment.get("Archived", False)
                    }
                
                role_data = user_assignments[user_id]["projects"][project_key]["role_assignments"][role]
                
                # Set dates
                if assignment.get("Assigned At"):
                    try:
                        assigned_date = assignment["Assigned At"].strftime("%Y-%m-%d") if hasattr(assignment["Assigned At"], 'strftime') else str(assignment["Assigned At"])[:10]
                        role_data["assigned_date"] = assigned_date
                    except:
                        role_data["assigned_date"] = "Unknown"
                
                if assignment.get("Completed At"):
                    try:
                        completed_date = assignment["Completed At"].strftime("%Y-%m-%d") if hasattr(assignment["Completed At"], 'strftime') else str(assignment["Completed At"])[:10]
                        role_data["completed_date"] = completed_date
                    except:
                        role_data["completed_date"] = "Unknown"
                
                if not assignment.get("Archived", False):
                    user_assignments[user_id]["is_archived"] = False
            
            # Search and filter controls
            st.markdown("### 🔍 Search & Filter")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                search_term = st.text_input("🔍 Search users", placeholder="Name or email...")
            
            with col2:
                status_filter = st.selectbox("Status", ["All", "Active", "Archived"])
            
            with col3:
                role_filter = st.selectbox("User Role", ["All", "admin", "human", "model"])
            
            with col4:
                project_role_filter = st.selectbox("Assignment Role", ["All", "annotator", "reviewer", "admin", "model"])
            
            # Apply filters
            filtered_assignments = {}
            for user_id, user_data in user_assignments.items():
                # Search filter
                if search_term:
                    if (search_term.lower() not in user_data["name"].lower() and 
                        search_term.lower() not in user_data["email"].lower()):
                        continue
                
                # Status filter
                if status_filter == "Active" and user_data["is_archived"]:
                    continue
                elif status_filter == "Archived" and not user_data["is_archived"]:
                    continue
                
                # User role filter
                if role_filter != "All" and user_data["user_role"] != role_filter:
                    continue
                
                # Project role filter
                if project_role_filter != "All":
                    has_role = any(
                        project_role_filter in project_info["role_assignments"]
                        for project_info in user_data["projects"].values()
                    )
                    if not has_role:
                        continue
                
                filtered_assignments[user_id] = user_data
            
            total_users = len(user_assignments)
            filtered_count = len(filtered_assignments)
            custom_info(f"📊 Showing {filtered_count} of {total_users} total users")
            
            # Main assignments display
            if filtered_assignments:
                st.markdown("### 📋 Assignment Overview")
                
                display_data = []
                for user_id, user_data in filtered_assignments.items():
                    # Group projects by project group
                    grouped_projects = {}
                    for project_id, project_info in user_data["projects"].items():
                        project_group = project_info["group"]
                        project_name = project_info["name"]
                        
                        if project_group not in grouped_projects:
                            grouped_projects[project_group] = []
                        
                        role_parts = []
                        
                        for role, role_data in project_info["role_assignments"].items():
                            is_completed = (role != "admin" and 
                                          role_data["completed_date"] and 
                                          role_data["completed_date"] != "Unknown")
                            
                            completion_emoji = "✅ " if is_completed else ""
                            
                            # Add accuracy information for training mode projects
                            accuracy_info = ""
                            if is_completed:
                                try:
                                    if check_project_has_full_ground_truth(project_id=project_id, session=session):
                                        if role == "annotator":
                                            accuracy_data = GroundTruthService.get_annotator_accuracy(project_id=project_id, session=session)
                                            if user_id in accuracy_data:
                                                overall_accuracy = calculate_overall_accuracy(accuracy_data)
                                                accuracy = overall_accuracy.get(user_id)
                                                if accuracy is not None:
                                                    color = get_accuracy_color(accuracy)
                                                    accuracy_info = f" <span style='color: {color}; font-weight: bold;'>[{accuracy:.1f}%]</span>"
                                        elif role == "reviewer":
                                            accuracy_data = GroundTruthService.get_reviewer_accuracy(project_id=project_id, session=session)
                                            if user_id in accuracy_data:
                                                overall_accuracy = calculate_overall_accuracy(accuracy_data)
                                                accuracy = overall_accuracy.get(user_id)
                                                if accuracy is not None:
                                                    color = get_accuracy_color(accuracy)
                                                    accuracy_info = f" <span style='color: {color}; font-weight: bold;'>[{accuracy:.1f}%]</span>"
                                except Exception as e:
                                    print(f"Error getting accuracy data: {e}")
                                    pass
                            
                            # Build date info
                            if role == "admin":
                                date_part = f"({role_data['assigned_date'] or 'not set'})"
                            else:
                                assigned = role_data["assigned_date"] or "not set"
                                completed = role_data["completed_date"] if role_data["completed_date"] and role_data["completed_date"] != "Unknown" else None
                                
                                date_part = f"({assigned} → {completed})" if completed else f"({assigned})"
                            
                            archived_indicator = " 🗄️" if role_data["archived"] else ""
                            
                            role_parts.append(f"{completion_emoji}{role.title()}{date_part}{accuracy_info}{archived_indicator}")
                        
                        roles_text = ", ".join(role_parts)
                        project_display = f"  • **{project_name}**: {roles_text}"
                        
                        grouped_projects[project_group].append({
                            "display": project_display,
                            "name": project_name,
                            "roles": list(project_info["role_assignments"].keys())
                        })
                    
                    # Format grouped projects
                    projects_data = []
                    projects_display = ""
                    
                    for group_name in sorted(grouped_projects.keys()):
                        if projects_display:
                            projects_display += "\n---\n\n"
                        
                        if group_name != "Ungrouped":
                            projects_display += f"## 📁 {group_name}\n\n"
                        else:
                            projects_display += f"## 📄 Individual Projects\n\n"
                        
                        for project_info in grouped_projects[group_name]:
                            projects_display += f"{project_info['display']}\n\n"
                            projects_data.append({
                                "group": group_name,
                                "name": project_info["name"],
                                "roles": project_info["roles"],
                                "display": project_info["display"]
                            })
                    
                    display_data.append({
                        "User Name": user_data["name"],
                        "Email": user_data["email"],
                        "User Role": user_data["user_role"].upper(),
                        "Status": "🗄️ Archived" if user_data["is_archived"] else "✅ Active",
                        "Project Assignments": projects_display.strip(),
                        "Projects Data": projects_data,
                        "Total Projects": len(user_data["projects"])
                    })
                
                display_data.sort(key=lambda x: x["User Name"])
                
                # Display in card format
                for i, user_data in enumerate(display_data):
                    status_color = COLORS['danger'] if "Archived" in user_data["Status"] else COLORS['primary']
                    
                    sample_projects = user_data["Projects Data"][:3] if user_data["Projects Data"] else []
                    sample_text = ""
                    if sample_projects:
                        sample_names = [p["name"] for p in sample_projects]
                        if len(user_data["Projects Data"]) > 3:
                            sample_text = f"Recent: {', '.join(sample_names[:2])}... (+{len(user_data['Projects Data'])-2} more)"
                        else:
                            sample_text = f"Projects: {', '.join(sample_names)}"
                    
                    with st.container():
                        st.markdown(f"""
                        <div style="border: 2px solid {status_color}; border-radius: 10px; padding: 15px; margin: 10px 0; background: linear-gradient(135deg, #ffffff, #f8f9fa); box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div style="flex: 1;">
                                    <h4 style="margin: 0; color: #9553FE;">👤 {user_data['User Name']}</h4>
                                    <p style="margin: 5px 0; color: #6c757d;">📧 {user_data['Email']}</p>
                                    <p style="margin: 5px 0; color: #6c757d; font-size: 0.9rem; font-style: italic;">{sample_text}</p>
                                </div>
                                <div style="text-align: right;">
                                    <span style="background: {status_color}; color: white; padding: 5px 10px; border-radius: 15px; font-weight: bold; font-size: 0.9rem; margin-left: 10px;">{user_data["Status"]}</span>
                                    <br><br>
                                    <span style="color: #495057; font-weight: bold;">
                                        {get_user_role_emoji(user_data['User Role'].lower())} {user_data['User Role']}
                                    </span>
                                </div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        total_projects = user_data["Total Projects"]
                        
                        with st.expander(f"📁 View assignments ({total_projects} projects)", expanded=False):
                            if user_data["Projects Data"]:
                                search_col1, search_col2 = st.columns(2)
                                
                                with search_col1:
                                    user_search = st.text_input(
                                        f"🔍 Search projects for {user_data['User Name']}", 
                                        placeholder="Project name...",
                                        key=f"user_search_{i}"
                                    )
                                
                                with search_col2:
                                    role_options = ["All"] + list(set([role for project in user_data["Projects Data"] for role in project["roles"]]))
                                    user_role_filter = st.selectbox(
                                        "Filter by role",
                                        role_options,
                                        key=f"user_role_filter_{i}"
                                    )
                                
                                filtered_projects = user_data["Projects Data"]
                                
                                if user_search:
                                    filtered_projects = [p for p in filtered_projects if user_search.lower() in p["name"].lower()]
                                
                                if user_role_filter != "All":
                                    filtered_projects = [p for p in filtered_projects if user_role_filter in p["roles"]]
                                
                                if len(filtered_projects) != total_projects:
                                    custom_info(f"Showing {len(filtered_projects)} of {total_projects} projects")
                                
                                # Pagination
                                projects_per_page = 10
                                total_project_pages = (len(filtered_projects) - 1) // projects_per_page + 1 if filtered_projects else 1
                                
                                if total_project_pages > 1:
                                    project_page = st.selectbox(
                                        f"Page", 
                                        range(1, total_project_pages + 1),
                                        key=f"project_page_{i}"
                                    ) - 1
                                else:
                                    project_page = 0
                                
                                start_idx = project_page * projects_per_page
                                end_idx = min(start_idx + projects_per_page, len(filtered_projects))
                                page_projects = filtered_projects[start_idx:end_idx]
                                
                                if page_projects:
                                    display_groups = {}
                                    for project in page_projects:
                                        group = project["group"]
                                        if group not in display_groups:
                                            display_groups[group] = []
                                        display_groups[group].append(project["display"])
                                    
                                    for group_name in sorted(display_groups.keys()):
                                        if group_name != "Ungrouped":
                                            st.markdown(f"### 📁 {group_name}")
                                        else:
                                            st.markdown(f"### 📄 Individual Projects")
                                        
                                        for project_display in display_groups[group_name]:
                                            st.markdown(project_display, unsafe_allow_html=True)
                                        
                                        if len(display_groups) > 1:
                                            st.markdown("---")
                                else:
                                    custom_info("No projects match the current filters.")
                            else:
                                custom_info("No project assignments")
            else:
                custom_infoing("No users match the current filters.")
        else:
            custom_info("No project assignments found in the database.")
        
        with st.expander("🗄️ Raw Assignment Data (Database View)", expanded=False):
            if not assignments_df.empty:
                st.markdown("**Direct database table view:**")
                st.dataframe(assignments_df, use_container_width=True)
            else:
                custom_info("No raw assignment data available.")

@st.fragment 
def admin_project_groups():
    st.subheader("📊 Project Group Management")
    
    with get_db_session() as session:
        try:
            groups = ProjectGroupService.list_project_groups(session=session)
            if groups:
                group_data = []
                for group in groups:
                    try:
                        group_info = ProjectGroupService.get_project_group_by_id(group_id=group.id, session=session)
                        project_count = len(group_info["projects"])
                        group_data.append({
                            "ID": group.id,
                            "Name": group.name,
                            "Description": group.description,
                            "Project Count": project_count,
                            "Created": group.created_at
                        })
                    except:
                        group_data.append({
                            "ID": group.id,
                            "Name": group.name,
                            "Description": group.description,
                            "Project Count": "Error",
                            "Created": group.created_at
                        })
                
                st.dataframe(pd.DataFrame(group_data), use_container_width=True)
            else:
                custom_info("No project groups exist yet.")
        except Exception as e:
            st.error(f"Error loading project groups: {str(e)}")
        
        with st.expander("➕ Create Project Group"):
            group_name = st.text_input("Group Name", key="admin_pgroup_name")
            group_description = st.text_area("Description", key="admin_pgroup_description")
            
            projects_df = ProjectService.get_all_projects(session=session)
            if not projects_df.empty:
                selected_projects = st.multiselect(
                    "Projects (optional)",
                    projects_df["ID"].tolist(),
                    format_func=lambda x: projects_df[projects_df["ID"]==x]["Name"].iloc[0],
                    key="admin_pgroup_projects"
                )
            else:
                selected_projects = []
                custom_infoing("No projects available to add to group.")
            
            if st.button("Create Project Group", key="admin_create_pgroup_btn"):
                if group_name:
                    try:
                        ProjectGroupService.create_project_group(
                            name=group_name, description=group_description, 
                            project_ids=selected_projects, session=session
                        )
                        st.success("Project group created!")
                        st.rerun(scope="fragment")
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
        
        with st.expander("✏️ Edit Project Group"):
            try:
                groups = ProjectGroupService.list_project_groups(session=session)
                if groups:
                    group_options = {f"{g.name} (ID: {g.id})": g.id for g in groups}
                    selected_group_name = st.selectbox(
                        "Select Group to Edit", 
                        list(group_options.keys()),
                        key="admin_edit_project_group_select"
                    )
                    
                    if selected_group_name:
                        selected_group_id = group_options[selected_group_name]
                        
                        group_info = ProjectGroupService.get_project_group_by_id(
                            group_id=selected_group_id, session=session
                        )
                        current_group = group_info["group"]
                        current_projects = group_info["projects"]
                        
                        new_name = st.text_input(
                            "Group Name", 
                            value=current_group.name,
                            key="admin_edit_group_name"
                        )
                        new_description = st.text_area(
                            "Description", 
                            value=current_group.description or "",
                            key="admin_edit_group_description"
                        )
                        
                        st.markdown("**Project Management:**")
                        
                        if current_projects:
                            st.markdown("**Current Projects:**")
                            current_project_ids = [p.id for p in current_projects]
                            for project in current_projects:
                                st.write(f"- {project.name} (ID: {project.id})")
                        else:
                            custom_info("No projects currently in this group")
                            current_project_ids = []
                        
                        all_projects_df = ProjectService.get_all_projects(session=session)
                        
                        if not all_projects_df.empty:
                            available_to_add = all_projects_df[~all_projects_df["ID"].isin(current_project_ids)]
                            if not available_to_add.empty:
                                add_projects = st.multiselect(
                                    "Add Projects",
                                    available_to_add["ID"].tolist(),
                                    format_func=lambda x: available_to_add[available_to_add["ID"]==x]["Name"].iloc[0],
                                    key="admin_edit_group_add_projects"
                                )
                            else:
                                add_projects = []
                                custom_info("All projects are already in this group")
                            
                            if current_project_ids:
                                remove_projects = st.multiselect(
                                    "Remove Projects",
                                    current_project_ids,
                                    format_func=lambda x: all_projects_df[all_projects_df["ID"]==x]["Name"].iloc[0],
                                    key="admin_edit_group_remove_projects"
                                )
                            else:
                                remove_projects = []
                        else:
                            add_projects = []
                            remove_projects = []
                            custom_infoing("No projects available in the system")
                        
                        if st.button("Update Project Group", key="admin_update_pgroup_btn"):
                            try:
                                ProjectGroupService.edit_project_group(
                                    group_id=selected_group_id,
                                    name=new_name if new_name != current_group.name else None,
                                    description=new_description if new_description != (current_group.description or "") else None,
                                    add_project_ids=add_projects if add_projects else None,
                                    remove_project_ids=remove_projects if remove_projects else None,
                                    session=session
                                )
                                st.success("Project group updated!")
                                st.rerun(scope="fragment")
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                else:
                    custom_info("No project groups available to edit")
            except Exception as e:
                st.error(f"Error loading groups for editing: {str(e)}")


# def get_project_videos(project_id: int, session: Session) -> List[Dict]:
#     """Get videos in a project"""
#     try:
#         return VideoService.get_project_videos(project_id=project_id, session=session)
#     except ValueError as e:
#         st.error(f"Error getting project videos: {str(e)}")
#         return []

def calculate_user_overall_progress(user_id: int, project_id: int, session: Session) -> float:
    """Calculate user's overall progress"""
    try:
        return AnnotatorService.calculate_user_overall_progress(user_id=user_id, project_id=project_id, session=session)
    except ValueError as e:
        st.error(f"Error calculating user progress: {str(e)}")
        return 0.0

def show_training_feedback(video_id: int, project_id: int, group_id: int, user_answers: Dict[str, str], session: Session):
    """Show training feedback comparing user answers to ground truth"""
    try:
        gt_df = GroundTruthService.get_ground_truth(video_id=video_id, project_id=project_id, session=session)
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

###############################################################################
# MAIN APP
###############################################################################

def main():
    st.set_page_config(
        page_title="Label Pizza",
        page_icon="🍕",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    import atexit
    from db import cleanup_connections
    
    # Ensure cleanup happens
    if 'cleanup_registered' not in st.session_state:
        atexit.register(cleanup_connections)
        st.session_state.cleanup_registered = True
    
    # Enhanced Custom CSS - Modern design with elegant sidebar
    st.markdown("""
        <style>
        /* Override Streamlit's primary color system */

        # .st-dz,
        # .st-dw {
        #     background-color: #9553FE !important;
        # }

        # .st-emotion-cache-3urlvs {
        #     background-color: #9553FE !important;
        #     border: #9553FE !important;
        # }

        # .st-emotion-cache-3urlvs:hover {
        #     background-color: #750FE1 !important;
        #     border: #750FE1 !important;
        # }

        # .st-emotion-cache-3urlvs:active {
        #     background-color: #9553FE !important;
        #     border: #9553FE !important;
        # }

        # .st-emotion-cache-3urlvs:focus:not(:active) {
        #     background-color: #9553FE !important;
        #     border: #9553FE !important;
        # }

        # .st-emotion-cache-5d2d9l:hover {
        #     color: black !important;
        #     border-color: #9553FE !important;
        # }

        # .st-emotion-cache-5d2d9l:focus:not(:active)  {
        #     color: black !important;
        #     border-color: #9553FE !important;
        # }

        # .st-emotion-cache-5d2d9l:active  {
        #     color: white !important;
        #     border-color: #9553FE !important;
        #     background-color: #9553FE !important;
        # }

        # .st-emotion-cache-1dj3ksd {
        #     background-color: #9553FE !important;
        # }

        # .st-ig, .st-i4 {
        #     background: #9553FE !important;
        # }

        # .st-jw {
        #     background: #9553FE !important;
        # }

        # .st-hr {
        #     background: #9553FE !important;
        # }

        # .st-hm {
        #     background-color: #9553FE !important;
        #     background: #9553FE !important;
        # }

        # .st-hn {
        #     background-color: #9553FE !important;
        #     background: #9553FE !important;
        # }

        # .st-hq {
        #     background-color: #9553FE !important;
        #     background: #9553FE !important;
        # }

        # .st-cv {
        #     border-bottom-color: #9553FE !important;
        # }

        # .st-cu {
        #     border-top-color: #9553FE !important;
        # }

        # .st-ct {
        #     border-right-color: #9553FE !important;
        # }

        # .st-cs {
        #     border-left-color: #9553FE !important;
        # }

        # .st-cc {
        #     border-bottom-color: #9553FE !important;
        # }

        # .st-cb {
        #     border-top-color: #9553FE !important;
        # }

        # .st-ca {
        #     border-right-color: #9553FE !important;
        # }

        # .st-c9 {
        #     border-left-color: #9553FE !important;
        # }
        

        /* Global improvements */
        .stProgress > div > div > div > div {
            background-color: #9553FE;
        }
        
        /* Clean, modern tabs styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
            background: transparent;
            border-radius: 0;
            padding: 4px;
            border: none;
            border-bottom: 1px solid #dee2e6;
            box-shadow: none;
        }

        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            background-color: transparent !important;
            border-radius: 0;
            color: #495057;
            font-weight: 600;
            border: none !important;
            padding: 8px 16px;
            transition: all 0.2s ease;
            font-size: 0.9rem;
        }

        .stTabs [data-baseweb="tab"]:hover {
            background-color: transparent !important;
            color: #750FE1 !important;
            transform: translateY(-1px);
        }

        .stTabs [aria-selected="true"] {
            background: transparent !important;
            color: #750FE1 !important;
            border: none !important;
            box-shadow: none !important;
            transform: translateY(-1px);
        }

        /* Change the original Streamlit tab indicator from red to purple */
        .stTabs [data-baseweb="tab-list"] > div:last-child {
            background-color: #A46CFE !important;
        }

        /* Alternative selectors for the indicator */
        .stTabs div[style*="background-color"] {
            background-color: #A46CFE !important;
        }
        
        /* Remove form borders */
        .stForm {
            border: none !important;
            padding: 0 !important;
            margin: 0 !important;
            background: transparent !important;
        }
        
        /* Sleek radio button styling */
        .stRadio > div {
            gap: 0.5rem;
            background: transparent !important;
            padding: 0 !important;
            border: none !important;
            border-radius: 0 !important;
            margin: 0 !important;
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: wrap !important;
            align-items: flex-start !important;
        }
        
        .stRadio > div > label {
            margin-bottom: 0.5rem;
            font-size: 0.85rem;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 6px 12px;
            border-radius: 6px;
            border: 1px solid #e9ecef;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            flex: 0 0 auto;
            min-width: fit-content;
            max-width: 280px;
            font-weight: 500;
            line-height: 1.2;
        }
        
        .stRadio > div > label:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #B180FF;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.15);
        }
        
        .stRadio > div > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 600;
            color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2);
        }
        
        /* Enhanced button styling */
        .stButton > button {
            # border-radius: 8px;
            # border: none;
            transition: all 0.2s ease;
            font-weight: 600;
            background: linear-gradient(135deg, #9553FE, #7C3AED);
            color: white;
            # padding: 10px 20px;
            # font-size: 0.9rem;
            box-shadow: 0 2px 6px rgba(149, 83, 254, 0.2);
            position: relative;
            z-index: 100;
            letter-spacing: 0.3px;
        }

        .stButton > button:hover {
            box-shadow: 0 4px 12px rgba(149, 83, 254, 0.3);
            transform: translateY(-2px);
            background: linear-gradient(135deg, #7C3AED, #6D28D9);
            color: white;
        }
        
        .stButton > button:disabled {
            background: #e4e4e4;
            transform: none;
            box-shadow: 0 1px 3px rgba(0,0,0,0.15);
            opacity: 0.6;
        }
        
        /* Segmented control */
        .stSegmentedControl {
            margin: 12px 0;
            padding: 3px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 8px;
            border: 1px solid #dee2e6;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        .stSegmentedControl > div {
            gap: 2px;
        }
        
        .stSegmentedControl button {
            border-radius: 6px !important;
            font-weight: 500 !important;
            transition: all 0.2s ease !important;
            border: none !important;
            font-size: 0.85rem !important;
            padding: 6px 12px !important;
        }
        
        .stSegmentedControl button:hover {
            transform: translateY(-1px) !important;
            box-shadow: 0 1px 4px rgba(31, 119, 180, 0.15) !important;
        }
        
        .stSegmentedControl button[aria-selected="true"] {
            background: linear-gradient(135deg, #1f77b4, #4a90e2) !important;
            color: white !important;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.25) !important;
        }
        
        /* Enhanced alert styling */
        .stAlert {
            border-radius: 8px;
            border: none;
            box-shadow: 0 1px 4px rgba(0,0,0,0.08);
            font-weight: 500;
        }
        
        /* Remove container borders */
        .element-container {
            border: none !important;
        }
        
        /* Custom scrollbars */
        ::-webkit-scrollbar {
            width: 8px;
        }
        
        ::-webkit-scrollbar-track {
            background: linear-gradient(180deg, #f1f1f1, #e9ecef);
            border-radius: 4px;
        }
        
        ::-webkit-scrollbar-thumb {
            background-color: linear-gradient(180deg, #1a6ca8, #4088d4);
            border-radius: 4px;
            border: 1px solid #f1f1f1;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background-color: #9553FE;
        }
        
        /* Enhanced success styling */
        .stSuccess {
            font-weight: 600;
            text-align: center;
            background: linear-gradient(135deg, #d4edda, #c3e6cb);
            border: 1px solid #28a745;
            border-radius: 8px;
        }
        
        /* Tab content padding */
        .stTabs [data-baseweb="tab-panel"] {
            padding: 16px 0;
        }
        
        /* Helper text styling */
        .stCaption {
            font-size: 0.8rem;
            color: #6c757d;
            font-style: italic;
            margin-top: 8px;
            padding: 6px 10px;
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 4px;
            border-left: 2px solid #1f77b4;
        }
        
        /* Enhanced checkbox styling with uniform width */
        .stCheckbox > label {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            padding: 6px 8px;
            border-radius: 6px;
            border: 1px solid #e9ecef;
            margin-bottom: 4px;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 500;
            font-size: 0.75rem;
            width: 100%;
            min-height: 40px;
            text-align: center;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .stCheckbox > label > div {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            width: 100%;
            text-align: center;
        }
        
        .stCheckbox > label:hover {
            background: #F2ECFC;
            border-color: #B180FF;
            transform: translateY(-1px);
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.15);
        }
        
        .stCheckbox > label[data-checked="true"] {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-color: #1f77b4;
            font-weight: 600;
            color: #1f77b4;
            box-shadow: 0 2px 6px rgba(31, 119, 180, 0.2);
        }
        
        /* Responsive improvements */
        @media (max-width: 768px) {
            .stRadio > div {
                gap: 0.4rem;
            }
            
            .stRadio > div > label {
                padding: 5px 10px;
                font-size: 0.8rem;
                max-width: 200px;
            }
            
            .stTabs [data-baseweb="tab"] {
                padding: 6px 12px;
                font-size: 0.8rem;
            }
            
            .stCheckbox > label {
                padding: 6px 10px;
                font-size: 0.8rem;
                min-height: 50px;
            }
        }
        
        /* Enhanced multiselect styling */
        .stMultiSelect > div > div {
            border-radius: 8px;
            border: 1px solid #e9ecef;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            min-height: 42px;
        }
        
        .stMultiSelect > div > div:focus-within {
            border-color: #1f77b4;
            box-shadow: 0 0 0 2px rgba(31, 119, 180, 0.1);
        }
        
        /* Enhanced metrics styling */
        .stMetric {
            background: #F3F2F4;
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #dee2e6;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }
        
        /* Enhanced expander styling */
        .streamlit-expanderHeader {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-radius: 8px;
            padding: 8px 12px;
            font-weight: 600;
            border: 1px solid #dee2e6;
            transition: all 0.2s ease;
        }
        
        .streamlit-expanderHeader:hover {
            background: linear-gradient(135deg, #e9ecef, #dee2e6);
            transform: translateY(-1px);
        }
        
        /* Enhanced dataframe styling */
        .stDataFrame {
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }

        /* Search Portal specific styling */
        .search-video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 16px;
            margin: 16px 0;
        }

        .search-criteria-card {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border: 2px solid #dee2e6;
            border-radius: 12px;
            padding: 12px;
            margin: 8px 0;
            transition: all 0.2s ease;
        }

        .search-criteria-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }

        .search-result-card {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border: 2px solid #e9ecef;
            border-radius: 12px;
            padding: 16px;
            margin: 8px 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            transition: all 0.3s ease;
            cursor: pointer;
        }

        .search-result-card:hover {
            transform: translateY(-3px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.15);
            border-color: #1f77b4;
        }

        .search-expandable-content {
            max-height: 600px;
            overflow-y: auto;
            padding: 8px;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            background: #fafbfc;
        }

        .search-question-answer {
            background: #ffffff;
            border: 1px solid #e9ecef;
            border-radius: 8px;
            padding: 12px;
            margin: 8px 0;
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }

        .search-ground-truth {
            background: linear-gradient(135deg, #d4edda, #c3e6cb);
            border-left: 4px solid #28a745;
            padding: 8px 12px;
            margin: 4px 0;
            border-radius: 4px;
        }

        .search-annotator-answer {
            background: linear-gradient(135deg, #e7f3ff, #cce7ff);
            border-left: 4px solid #1f77b4;
            padding: 8px 12px;
            margin: 4px 0;
            border-radius: 4px;
        }

        .search-status-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.8rem;
            font-weight: 600;
            text-align: center;
        }

        .search-stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 12px;
            margin: 16px 0;
        }

        /* Enhanced expander styling for search */
        .search-portal .streamlit-expanderHeader {
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border: 2px solid #e9ecef;
            border-radius: 10px;
            padding: 10px 16px;
            font-weight: 600;
            margin: 8px 0;
            transition: all 0.2s ease;
        }

        .search-portal .streamlit-expanderHeader:hover {
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            border-color: #1f77b4;
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(31, 119, 180, 0.15);
        }

        /* Search input enhancement */
        .search-portal .stTextInput > div > div > input {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            padding: 12px 16px;
            font-size: 0.95rem;
            transition: all 0.2s ease;
        }

        .search-portal .stTextInput > div > div > input:focus {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1);
        }

        /* Search multiselect enhancement */
        .search-portal .stMultiSelect > div > div {
            border-radius: 10px;
            border: 2px solid #e9ecef;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            transition: all 0.2s ease;
        }

        .search-portal .stMultiSelect > div > div:focus-within {
            border-color: #1f77b4;
            box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.1);
        }

        /* Video grid responsive design */
        @media (max-width: 768px) {
            .search-video-grid {
                grid-template-columns: 1fr;
            }
            
            .search-stats-grid {
                grid-template-columns: repeat(2, 1fr);
            }
        }

        /* Enhanced criteria search styling */
        .criteria-search-highlight {
            background: linear-gradient(135deg, #fff3cd, #ffeaa7);
            border-left: 4px solid #ffc107;
            padding: 8px 12px;
            margin-bottom: 8px;
            border-radius: 4px;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { box-shadow: 0 0 0 0 rgba(255, 193, 7, 0.7); }
            70% { box-shadow: 0 0 0 10px rgba(255, 193, 7, 0); }
            100% { box-shadow: 0 0 0 0 rgba(255, 193, 7, 0); }
        }

        .criteria-match-card {
            transition: all 0.3s ease;
            cursor: pointer;
        }

        .criteria-match-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if "current_view" not in st.session_state:
        st.session_state.current_view = "dashboard"
    if "selected_project_id" not in st.session_state:
        st.session_state.selected_project_id = None
    
    # Check authentication
    if "user" not in st.session_state:
        login_page()
        return
    
    user = st.session_state.user
    available_portals = st.session_state.get("available_portals", [])
    
    # Modern, compact sidebar with elegant Label Pizza card
    with st.sidebar:
        st.markdown("""
        <div style="
            width: 100%;
            padding: 20px;
            background: linear-gradient(135deg, #ffffff, #f8f9fa);
            border-radius: 16px;
            margin: 10px 0 20px 0;
            border: 2px solid #e9ecef;
            text-align: center;
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -20px;
                right: -20px;
                width: 60px;
                height: 60px;
                background: #9553FE;
                border-radius: 50%;
                opacity: 0.1;
            "></div>
            <div style="
                position: absolute;
                bottom: -15px;
                left: -15px;
                width: 40px;
                height: 40px;
                background: #9553FE;
                border-radius: 50%;
                opacity: 0.1;
            "></div>
            <div style="
                font-size: 2.2rem;
                margin-bottom: 8px;
                filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
            ">🍕</div>
            <div style="
                font-size: 1.4rem;
                font-weight: 800;
                color: #2c3e50;
                margin-bottom: 4px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                letter-spacing: 0.5px;
            ">Label Pizza</div>
            <div style="
                font-size: 0.85rem;
                color: #6c757d;
                font-weight: 500;
                font-style: italic;
            ">Video Annotation Made Simple</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### 👋 Welcome!")
        
        user_email = user.get('email', 'No email')
        if not user_email or user_email == 'No email':
            user_email = user.get('Email', user.get('user_email', 'No email'))
        
        display_user_simple(user['name'], user_email, is_ground_truth=(user['role'] == 'admin'))
        
        # role_color = COLORS['danger'] if user['role'] == 'admin' else COLORS['info']
        role_color = '#9553FE'
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {role_color}, {role_color}dd);
            color: white;
            padding: 6px 12px;
            border-radius: 8px;
            text-align: center;
            font-weight: 600;
            font-size: 0.9rem;
            margin: 8px 0;
        ">
            Role: {user['role'].title()}
        </div>
        """, unsafe_allow_html=True)
        
        # Portal selection
        if available_portals:
            st.markdown("---")
            st.markdown("**Portal:**")
            
            portal_labels = {
                "annotator": "👥 Annotator",
                "reviewer": "🔍 Reviewer", 
                "meta_reviewer": "🎯 Meta-Reviewer",
                "search": "🔍 Search",
                "admin": "⚙️ Admin"
            }
            
            if "selected_portal" not in st.session_state:
                if user["role"] == "admin":
                    st.session_state.selected_portal = "admin"
                else:
                    st.session_state.selected_portal = available_portals[0]
            
            selected_portal = st.radio(
                "Portal",
                available_portals,
                format_func=lambda x: portal_labels[x],
                index=available_portals.index(st.session_state.selected_portal) if st.session_state.selected_portal in available_portals else 0,
                label_visibility="collapsed"
            )
            
            if selected_portal != st.session_state.get("selected_portal"):
                st.session_state.selected_portal = selected_portal
                st.session_state.current_view = "dashboard"
                st.session_state.selected_project_id = None
                if "selected_annotators" in st.session_state:
                    del st.session_state.selected_annotators
                st.rerun()
        
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Route to selected portal
    if not available_portals:
        custom_infoing("No portals available. You may not be assigned to any projects or your account may not have the necessary permissions. Please contact an administrator.")
        return
    
    selected_portal = st.session_state.get("selected_portal", available_portals[0])
    
    portal_functions = {
        "admin": admin_portal,
        "meta_reviewer": meta_reviewer_portal,
        "reviewer": reviewer_portal,
        "annotator": annotator_portal,
        "search": search_portal
    }
    
    portal_function = portal_functions.get(selected_portal)
    if portal_function:
        portal_function()
    else:
        st.error(f"Unknown portal: {selected_portal}")

if __name__ == "__main__":
    main()