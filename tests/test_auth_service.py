import pytest
from label_pizza.services import AuthService, ProjectService
import pandas as pd

def test_auth_service_create_user(session):
    """Test creating a new user."""
    user = AuthService.create_user(
        user_id="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    assert user.user_id_str == "test_user"
    assert user.email == "test@example.com"
    assert user.password_hash == "test_hash"
    assert user.user_type == "admin"
    assert not user.is_archived

def test_auth_service_create_model_user(session):
    """Test creating a model user without email."""
    user = AuthService.create_user(
        user_id="model_user",
        email=None,
        password_hash="test_hash",
        user_type="model",
        session=session
    )
    assert user.user_id_str == "model_user"
    assert user.email is None
    assert user.password_hash == "test_hash"
    assert user.user_type == "model"
    assert not user.is_archived

def test_auth_service_create_model_user_with_email(session):
    """Test that model users cannot have emails."""
    with pytest.raises(ValueError, match="Model users cannot have emails"):
        AuthService.create_user(
            user_id="model_user",
            email="model@example.com",
            password_hash="test_hash",
            user_type="model",
            session=session
        )

def test_auth_service_create_human_user_without_email(session):
    """Test that human users must have emails."""
    with pytest.raises(ValueError, match="Email is required for human and admin users"):
        AuthService.create_user(
            user_id="human_user",
            email=None,
            password_hash="test_hash",
            user_type="human",
            session=session
        )

def test_auth_service_create_admin_user_without_email(session):
    """Test that admin users must have emails."""
    with pytest.raises(ValueError, match="Email is required for human and admin users"):
        AuthService.create_user(
            user_id="admin_user",
            email=None,
            password_hash="test_hash",
            user_type="admin",
            session=session
        )

def test_auth_service_create_user_duplicate_id(session, test_user):
    """Test creating a user with duplicate user ID."""
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            user_id="test_user",
            email="other@example.com",
            password_hash="test_hash",
            user_type="admin",
            session=session
        )

def test_auth_service_create_user_duplicate_email(session, test_user):
    """Test creating a user with duplicate email."""
    with pytest.raises(ValueError, match="already exists"):
        AuthService.create_user(
            user_id="other_user",
            email="test@example.com",
            password_hash="test_hash",
            user_type="admin",
            session=session
        )

def test_auth_service_create_user_invalid_type(session):
    """Test creating a user with invalid type."""
    with pytest.raises(ValueError, match="Invalid user type"):
        AuthService.create_user(
            user_id="test_user",
            email="test@example.com",
            password_hash="test_hash",
            user_type="invalid",
            session=session
        )

def test_auth_service_get_user_by_id(session, test_user):
    """Test getting a user by ID."""
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.user_id_str == "test_user"
    assert user.email == "test@example.com"

def test_auth_service_get_user_by_id_not_found(session):
    """Test getting a non-existent user by ID."""
    with pytest.raises(ValueError, match="User with ID 'non_existent_user' not found"):
        AuthService.get_user_by_id("non_existent_user", session)

def test_auth_service_get_user_by_id_archived(session, test_user):
    """Test getting an archived user by ID."""
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.is_archived

def test_auth_service_get_user_by_email(session, test_user):
    """Test getting a user by email."""
    user = AuthService.get_user_by_email("test@example.com", session)
    assert user is not None
    assert user.id == test_user.id

def test_auth_service_get_user_by_email_not_found(session):
    """Test getting a non-existent user by email."""
    with pytest.raises(ValueError, match="User with email 'nonexistent@example.com' not found"):
        AuthService.get_user_by_email("nonexistent@example.com", session)

def test_auth_service_get_user_by_email_archived(session, test_user):
    """Test getting an archived user by email."""
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_email("test@example.com", session)
    assert user is not None
    assert user.is_archived

def test_auth_service_archive_user(session, test_user):
    """Test archiving a user."""
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.is_archived

def test_auth_service_archive_user_not_found(session):
    """Test archiving a non-existent user."""
    with pytest.raises(ValueError, match="User with ID 999 not found"):
        AuthService.toggle_user_archived(999, session)

def test_auth_service_get_user_by_id_include_archived(session, test_user):
    """Test getting a user by ID including archived ones."""
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.is_archived

def test_auth_service_get_user_by_email_include_archived(session, test_user):
    """Test getting a user by email including archived ones."""
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_email("test@example.com", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.is_archived

def test_auth_service_get_all_users(session, test_user):
    """Test getting all users."""
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == test_user.id
    assert df.iloc[0]["User ID"] == "test_user"
    assert df.iloc[0]["Email"] == "test@example.com"
    assert df.iloc[0]["Role"] == "admin"
    assert not df.iloc[0]["Archived"]

def test_auth_service_get_all_users_empty(session):
    """Test getting all users when none exist."""
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_auth_service_get_all_users_with_archived(session, test_user):
    """Test getting all users including archived ones."""
    AuthService.toggle_user_archived(test_user.id, session)
    df = AuthService.get_all_users(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["ID"] == test_user.id
    assert df.iloc[0]["Archived"]

def test_auth_service_get_users_by_type(session, test_user):
    """Test getting users by type."""
    users = AuthService.get_users_by_type("admin", session)
    assert len(users) == 1
    assert users[0].id == test_user.id
    assert users[0].user_type == "admin"

def test_auth_service_get_users_by_type_empty(session):
    """Test getting users by type when none exist."""
    users = AuthService.get_users_by_type("admin", session)
    assert len(users) == 0

def test_auth_service_get_users_by_type_with_archived(session, test_user):
    """Test getting users by type including archived ones."""
    AuthService.toggle_user_archived(test_user.id, session)
    users = AuthService.get_users_by_type("admin", session)
    assert len(users) == 1
    assert users[0].id == test_user.id
    assert users[0].is_archived

def test_auth_service_update_user_id(session, test_user):
    """Test updating a user's ID."""
    AuthService.update_user_id(test_user.id, "new_user_id", session)
    user = AuthService.get_user_by_id("new_user_id", session)
    assert user is not None
    assert user.id == test_user.id
    assert user.user_id_str == "new_user_id"

def test_auth_service_update_user_id_duplicate(session, test_user):
    """Test updating a user's ID to one that already exists."""
    # Create another user
    AuthService.create_user(
        user_id="other_user",
        email="other@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    with pytest.raises(ValueError, match="already exists"):
        AuthService.update_user_id(test_user.id, "other_user", session)

def test_auth_service_update_user_email(session, test_user):
    """Test updating a user's email."""
    AuthService.update_user_email(test_user.id, "new@example.com", session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.email == "new@example.com"

def test_auth_service_update_user_email_duplicate(session, test_user):
    """Test updating a user's email to one that already exists."""
    # Create another user
    AuthService.create_user(
        user_id="other_user",
        email="other@example.com",
        password_hash="test_hash",
        user_type="admin",
        session=session
    )
    with pytest.raises(ValueError, match="already exists"):
        AuthService.update_user_email(test_user.id, "other@example.com", session)

def test_auth_service_update_user_password(session, test_user):
    """Test updating a user's password."""
    AuthService.update_user_password(test_user.id, "new_password", session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.password_hash == "new_password"
    assert user.updated_at is not None

def test_auth_service_update_user_role(session, test_user):
    """Test updating a user's role."""
    # First create a human user since test_user is admin
    human_user = AuthService.create_user(
        user_id="human_user",
        email="human@example.com",
        password_hash="test_hash",
        user_type="human",
        session=session
    )
    AuthService.update_user_role(human_user.id, "admin", session)
    user = AuthService.get_user_by_id("human_user", session)
    assert user is not None
    assert user.user_type == "admin"

def test_auth_service_update_user_role_invalid(session, test_user):
    """Test updating a user's role to an invalid value."""
    with pytest.raises(ValueError, match="Invalid role"):
        AuthService.update_user_role(test_user.id, "invalid", session)

def test_auth_service_update_user_role_model_to_non_model(session):
    """Test that model users cannot be changed to non-model roles."""
    # Create a model user
    model_user = AuthService.create_user(
        user_id="model_user",
        email=None,
        password_hash="test_hash",
        user_type="model",
        session=session
    )
    with pytest.raises(ValueError, match="Cannot change from model to human/admin"):
        AuthService.update_user_role(model_user.id, "human", session)

def test_auth_service_update_user_role_to_model(session):
    """Test that only model users can be assigned model role."""
    # Create a human user
    human_user = AuthService.create_user(
        user_id="human_user",
        email="human@example.com",
        password_hash="test_hash",
        user_type="human",
        session=session
    )
    with pytest.raises(ValueError, match="Cannot change from human/admin to model"):
        AuthService.update_user_role(human_user.id, "model", session)

def test_auth_service_update_user_role_admin_to_human_archives_roles(session, test_user, test_project):
    """Test that changing from admin to human archives project roles."""
    # Add user to project as admin
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 3 # 1 admin, 1 annotator, 1 reviewer
    
    # Change role to human
    AuthService.update_user_role(test_user.id, "human", session)
    
    # Check that project role is archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 0

def test_auth_service_toggle_user_archived(session, test_user):
    """Test toggling a user's archived status."""
    # Archive user
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert user.is_archived
    # Unarchive user
    AuthService.toggle_user_archived(test_user.id, session)
    user = AuthService.get_user_by_id("test_user", session)
    assert user is not None
    assert not user.is_archived

def test_auth_service_get_project_assignments(session, test_user, test_project):
    """Test getting project assignments."""
    # Add user to project
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    
    # Get assignments
    df = AuthService.get_project_assignments(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3 # 1 admin, 1 annotator, 1 reviewer


def test_auth_service_get_project_assignments_with_archived(session, test_user, test_project):
    """Test getting project assignments including archived ones."""
    # Add user to project
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    
    # Archive the assignment
    AuthService.remove_user_from_project(test_user.id, test_project.id, session)
    
    # Get assignments
    df = AuthService.get_project_assignments(session)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0

def test_auth_service_remove_user_from_project(session, test_user, test_project):
    """Test archiving a user from a project."""
    # Add user to project
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    
    # Archive user from project
    AuthService.remove_user_from_project(test_user.id, test_project.id, session)
    
    # Verify assignment is archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 0


def test_auth_service_unremove_user_from_project(session, test_user, test_project):
    """Test unarchiving a user from a project."""
    # Add user to project
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    
    # Archive user from project
    AuthService.remove_user_from_project(test_user.id, test_project.id, session)
    # Verify assignment is not archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 0
    
    ProjectService.add_user_to_project(test_user.id, test_project.id, "admin", session)
    # Verify assignment is not archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 3 # 1 admin, 1 annotator, 1 reviewer


def test_auth_service_unarchive_reviewer_from_project(session, test_project):
    """Test unarchiving a reviewer from a project."""
    # Add user to project
    test_user = AuthService.create_user(
        user_id="test_user",
        email="test@example.com",
        password_hash="test_hash",
        user_type="human",
        session=session
    )
    ProjectService.add_user_to_project(test_user.id, test_project.id, "reviewer", session)
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 2 # 1 reviewer, 1 annotator

    # Archive user from project
    AuthService.remove_user_from_project(test_user.id, test_project.id, session)
    # Verify assignment is not archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 0
    
    ProjectService.add_user_to_project(test_user.id, test_project.id, "annotator", session)
    # Verify assignment is not archived
    assignments = AuthService.get_project_assignments(session)
    assert len(assignments) == 1 # 1 annotator

def test_auth_service_update_user_email_model_user(session):
    """Test that model users cannot have emails updated."""
    # Create a model user
    model_user = AuthService.create_user(
        user_id="model_user",
        email=None,
        password_hash="test_hash",
        user_type="model",
        session=session
    )
    with pytest.raises(ValueError, match="Model users cannot have emails"):
        AuthService.update_user_email(model_user.id, "model@example.com", session)

def test_auth_service_update_user_email_to_none(session, test_user):
    """Test that human/admin users cannot have their email set to None."""
    with pytest.raises(ValueError, match="Email is required for human and admin users"):
        AuthService.update_user_email(test_user.id, None, session) 