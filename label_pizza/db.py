# db.py  – lives next to models.py and app.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import atexit

load_dotenv(".env")  # loads DBURL

# Base class for all models
Base = declarative_base()

# Import models to ensure they are registered with Base
from label_pizza.models import *  # This ensures all models are registered with Base

# Placeholder variables that will be set by init_database()
engine = None
SessionLocal = None

def init_database(database_url_name="DBURL"):
    """Initialize database engine and session maker"""
    global engine, SessionLocal
    
    url = os.environ.get(database_url_name)
    if not url:
        raise ValueError(f"Database URL '{database_url_name}' not found in environment variables")
    
    # # ← ADD: Enhance URL with database timeout for PostgreSQL
    # if url.startswith('postgresql') and 'idle_in_transaction_session_timeout' not in url:
    #     from urllib.parse import quote_plus
    #     timeout_option = "-c idle_in_transaction_session_timeout=1800000"  # 30 minutes
    #     separator = "&" if "?" in url else "?"
    #     url = f"{url}{separator}options={quote_plus(timeout_option)}"
    
    engine = create_engine(
        url,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_reset_on_return='commit'
    )
    
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def cleanup_connections():
    """Clean up all database connections"""
    try:
        engine.dispose()
        print("Database connections cleaned up")
    except Exception as e:
        print(f"Error cleaning up connections: {e}")

# Test database configuration
test_engine = create_engine(os.environ.get("TEST_DBURL", "sqlite:///:memory:"), echo=False, future=True)
TestSessionLocal = sessionmaker(bind=test_engine, expire_on_commit=False)
# Register cleanup function to run on exit
atexit.register(cleanup_connections)

def init_test_db():
    """Initialize the test database by creating all tables."""
    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)

def get_test_session():
    """Get a new test database session."""
    return TestSessionLocal()