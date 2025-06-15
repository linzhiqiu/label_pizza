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
# from label_pizza.models import *  # This ensures all models are registered with Base
from label_pizza.models import *

# Production database configuration with STRICT connection limits for Supabase
engine = create_engine(
    os.environ["DBURL"],
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_recycle=300,
    # CRITICAL: Supabase connection limits
    pool_size=5,           # Max 5 persistent connections
    max_overflow=10,       # Max 10 additional connections
    pool_timeout=30,       # Wait 30 seconds for connection
    pool_reset_on_return='commit'  # Reset connections properly
)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# Test database configuration
test_engine = create_engine(os.environ.get("TEST_DBURL", "sqlite:///:memory:"), echo=False, future=True)
TestSessionLocal = sessionmaker(bind=test_engine, expire_on_commit=False)

def cleanup_connections():
    """Clean up all database connections"""
    try:
        engine.dispose()
        test_engine.dispose()
        print("Database connections cleaned up")
    except Exception as e:
        print(f"Error cleaning up connections: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_connections)

def init_test_db():
    """Initialize the test database by creating all tables."""
    Base.metadata.drop_all(test_engine)
    Base.metadata.create_all(test_engine)

def get_test_session():
    """Get a new test database session."""
    return TestSessionLocal()