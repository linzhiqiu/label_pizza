#!/usr/bin/env python3
"""
Database Initialization/Reset Script for Label Pizza
====================================================
This script provides nuclear database reset functionality that ACTUALLY works.

FIXES:
- Uses DROP TABLE CASCADE for each table individually (nuclear option)
- Completely destroys and recreates all tables from SQLAlchemy models
- Works around all Supabase limitations and connection routing issues
- Properly closes all sessions before operations
- Integrates backup functionality

Usage:
    # Initialize database (safe, won't affect existing tables)
    python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode init --email admin@example.com --password mypass --user-id "Admin"
    
    # Nuclear reset with automatic backup (RECOMMENDED)
    python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup
    
    # Nuclear reset with custom backup location
    python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup --backup-dir ./backups --backup-file my_backup.sql.gz
    
    # Reset from existing backup
    python label_pizza/init_or_reset_db.py --database-url-name DBURL --mode restore --backup-dir ./backups --backup-file my_backup.sql.gz --email admin@example.com --password mypass --user-id "Admin"
"""

import argparse
import os
import sys
import datetime
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text, create_engine, Engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

# Load environment variables
load_dotenv()

# Import your models and services
try:
    from label_pizza.models import Base, User
    from label_pizza.services import AuthService
    from label_pizza.db import init_database as init_db
except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print("Make sure you're running this from the correct directory.")
    sys.exit(1)

# Import backup functionality
try:
    from backup_restore import DatabaseBackupRestore
    BACKUP_AVAILABLE = True
except ImportError:
    print("⚠️  backup_restore.py not found. Backup functionality disabled.")
    BACKUP_AVAILABLE = False

class NuclearDatabaseManager:
    """Nuclear database management - completely destroys and recreates everything"""
    
    def __init__(self, engine: Engine, session_local, db_url: str = None):
        self.engine = engine
        self.session_local = session_local
        self.db_url = db_url or os.getenv("DBURL")
        
    def close_all_sessions(self):
        """Close all active sessions to prevent locks"""
        try:
            print("🔄 Closing all database sessions...")
            
            # Close all sessions in the session pool
            if hasattr(self.session_local, 'close_all'):
                self.session_local.close_all()
            
            # For SQLAlchemy 2.x
            try:
                from sqlalchemy.orm.session import close_all_sessions
                close_all_sessions()
                print("   ✅ All sessions closed")
            except ImportError:
                # For SQLAlchemy 1.x
                try:
                    self.session_local.close_all()
                    print("   ✅ All sessions closed (legacy method)")
                except AttributeError:
                    print("   ⚠️  Could not close sessions automatically")
            
            # Dispose engine connection pool
            self.engine.dispose()
            print("   ✅ Connection pool disposed")
            
        except Exception as e:
            print(f"   ⚠️  Error closing sessions: {e}")
    
    def nuclear_table_destruction(self) -> bool:
        """
        Nuclear option: DROP every single table individually with CASCADE
        This is the most aggressive approach that definitely works
        """
        try:
            print("💥 NUCLEAR OPTION: Destroying all tables individually...")
            print("   Method: DROP TABLE CASCADE for each table (no mercy!)")
            
            # Close all sessions first
            self.close_all_sessions()
            
            if not self.db_url:
                print("   ❌ Database URL not available")
                return False
            
            # Create a completely fresh engine with no pooling
            nuclear_engine = create_engine(
                self.db_url, 
                poolclass=NullPool,
                echo=False,
                isolation_level="AUTOCOMMIT"  # Force autocommit
            )
            
            with nuclear_engine.connect() as conn:
                # Get current state
                print("   🔍 Pre-destruction scan...")
                result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"))
                table_count = result.scalar()
                print(f"   📋 Target tables: {table_count}")
                
                try:
                    result = conn.execute(text("SELECT COUNT(*) FROM public.users;"))
                    user_count = result.scalar()
                    print(f"   👥 Target users: {user_count}")
                except Exception as e:
                    print(f"   ⚠️  Could not count users: {e}")
                
                # Get all table names for destruction
                print("   🎯 Acquiring targets for destruction...")
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """))
                target_tables = [row[0] for row in result.fetchall()]
                
                if not target_tables:
                    print("   ⚠️  No tables found to destroy")
                    return True
                
                print(f"   💣 DESTROYING {len(target_tables)} tables with extreme prejudice...")
                
                destroyed_count = 0
                resistant_tables = []
                
                # Phase 1: Individual table destruction with CASCADE
                for table_name in target_tables:
                    try:
                        print(f"   💥 DESTROYING: {table_name}")
                        conn.execute(text(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;'))
                        destroyed_count += 1
                    except Exception as e:
                        print(f"   🛡️  {table_name} resisted destruction: {e}")
                        resistant_tables.append(table_name)
                
                print(f"   📊 Destruction summary: {destroyed_count} obliterated, {len(resistant_tables)} survived")
                
                # Phase 2: Double-tap any survivors
                if resistant_tables:
                    print("   🔄 Phase 2: Eliminating survivors...")
                    for table_name in resistant_tables:
                        try:
                            # Try different destruction methods
                            print(f"   💥 DOUBLE-TAP: {table_name}")
                            conn.execute(text(f'DROP TABLE public."{table_name}" CASCADE;'))  # No IF EXISTS
                            print(f"   ✅ {table_name} eliminated")
                        except Exception as e:
                            print(f"   🛡️  {table_name} is immortal: {e}")
                
                # Phase 3: Verification of total destruction
                print("   🔍 Post-destruction verification...")
                result = conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"))
                survivors = result.scalar()
                print(f"   📋 Surviving tables: {survivors}")
                
                if survivors > 0:
                    print("   ❌ Some tables survived the nuclear option!")
                    # Show survivors
                    result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"))
                    survivor_names = [row[0] for row in result.fetchall()]
                    print(f"   🛡️  Immortal tables: {survivor_names}")
                    return False
                else:
                    print("   ✅ TOTAL ANNIHILATION ACHIEVED!")
                
                # Phase 4: Verify users table is gone
                try:
                    result = conn.execute(text("SELECT COUNT(*) FROM public.users;"))
                    surviving_users = result.scalar()
                    print(f"   ❌ CRITICAL ERROR: {surviving_users} users survived nuclear destruction!")
                    return False
                except Exception as e:
                    if "does not exist" in str(e):
                        print("   ✅ CONFIRMED: Users table obliterated")
                    else:
                        print(f"   ✅ Users table inaccessible: {e}")
            
            nuclear_engine.dispose()
            
            print("   📝 Nuclear destruction complete - all tables eliminated")
            print("   📝 Fresh tables will be created from SQLAlchemy models")
            
            print("   ✅ Nuclear option succeeded")
            return True
            
        except Exception as e:
            print(f"   ❌ Nuclear option failed: {e}")
            import traceback
            print(f"   Full error: {traceback.format_exc()}")
            return False
    
    def create_all_tables(self, mode="reset"):
        """Create all tables from models"""
        try:
            if mode == "init":
                print("🏗️  Creating missing tables (safe mode)...")
            else:
                print("🏗️  Creating fresh tables from models...")
            
            # Create all tables defined in models
            Base.metadata.create_all(bind=self.engine)
            
            # Count created tables
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """))
                table_count = result.scalar()
                
            print(f"   ✅ Created/verified {table_count} tables")
            return True
            
        except Exception as e:
            print(f"   ❌ Error creating tables: {e}")
            return False
    
    def seed_admin_user(self, email: str, password: str, user_id: str) -> bool:
        """Create an admin user with specified credentials"""
        try:
            with self.session_local() as session:
                AuthService.seed_admin(
                    session=session,
                    email=email,
                    password=password,
                    user_id=user_id
                )
                
                print(f"✅ Admin user created successfully!")
                print(f"   📧 Email: {email}")
                print(f"   👤 User ID: {user_id}")
                print(f"   🔑 Password: {'*' * len(password)}")
                
                return True
                
        except Exception as e:
            print(f"❌ Failed to create admin user: {e}")
            return False
    
    def verify_database(self, email: str):
        """Verify the database setup was successful"""
        try:
            print("🔍 Verifying database...")
            
            with self.engine.connect() as conn:
                # Check that admin user exists
                admin_count = conn.execute(
                    text("SELECT COUNT(*) FROM users WHERE user_type = 'admin'")
                ).scalar()
                
                if admin_count > 0:
                    print("   ✅ Admin user verified")
                else:
                    print("   ❌ Admin user not found")
                    return False
                    
                # Check tables exist
                tables = conn.execute(
                    text("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """)
                ).fetchall()
                
                table_names = [row[0] for row in tables]
                expected_tables = [
                    'users', 'videos', 'projects', 'schemas', 'questions',
                    'question_groups', 'annotator_answers', 'reviewer_ground_truth'
                ]
                
                missing_tables = []
                for table in expected_tables:
                    if table in table_names:
                        print(f"   ✅ Table '{table}' exists")
                    else:
                        print(f"   ❌ Table '{table}' missing")
                        missing_tables.append(table)
                
                if missing_tables:
                    print(f"   ⚠️  Missing tables: {missing_tables}")
                    return False
                    
                print(f"   ✅ Database verification completed ({len(table_names)} tables)")
                return True
                    
        except Exception as e:
            print(f"   ❌ Verification failed: {e}")
            return False

def create_backup_if_requested(db_url: str, backup_dir: str = "./backups", 
                             backup_file: Optional[str] = None, compress: bool = True) -> Optional[str]:
    """Create a backup before reset if requested"""
    if not BACKUP_AVAILABLE:
        print("❌ Backup functionality not available (backup_restore.py not found)")
        return None
        
    try:
        handler = DatabaseBackupRestore(db_url)
        
        # Create backup directory if it doesn't exist
        backup_path = Path(backup_dir)
        backup_path.mkdir(exist_ok=True)
        
        # Handle output filename
        if backup_file is None:
            # Auto-generate timestamped filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            extension = ".sql.gz" if compress else ".sql"
            backup_file = f"backup_before_nuclear_{timestamp}{extension}"
        
        # If backup_file is just a filename (no path separator), combine with backup_dir
        if not os.path.sep in backup_file and not os.path.isabs(backup_file):
            output_file = str(backup_path / backup_file)
        else:
            output_file = backup_file
        
        print(f"💾 Creating backup before nuclear reset: {output_file}")
        
        success = handler.create_backup(
            output_file=output_file,
            compress=compress,
            schema_only=False
        )
        
        if success:
            print(f"   ✅ Backup created: {output_file}")
            return output_file
        else:
            print("   ❌ Backup failed")
            return None
            
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return None

def restore_from_backup(db_url: str, backup_dir: str, backup_file: str, force: bool = False) -> bool:
    """Restore database from backup file"""
    if not BACKUP_AVAILABLE:
        print("❌ Restore functionality not available (backup_restore.py not found)")
        return False
        
    try:
        # Handle input file path
        if not os.path.sep in backup_file and not os.path.isabs(backup_file):
            input_file = os.path.join(backup_dir, backup_file)
        else:
            input_file = backup_file
            
        if not os.path.exists(input_file):
            print(f"❌ Backup file not found: {input_file}")
            return False
            
        handler = DatabaseBackupRestore(db_url)
        
        print(f"📥 Restoring from backup: {input_file}")
        
        success = handler.restore_backup(
            input_file=input_file,
            force=force
        )
        
        if success:
            print("   ✅ Restore completed")
            return True
        else:
            print("   ❌ Restore failed")
            return False
            
    except Exception as e:
        print(f"❌ Restore failed: {e}")
        return False

def confirm_nuclear_reset() -> bool:
    """Ask user to confirm nuclear database reset"""
    print("☢️  NUCLEAR WARNING: This will OBLITERATE ALL DATA in your database!")
    print("This is the nuclear option - complete table destruction and recreation.")
    print("This action cannot be undone and is EXTREMELY DESTRUCTIVE.")
    print()
    
    # Show current database URL (masked for security)
    db_url = os.getenv("DBURL", "Not found")
    if db_url != "Not found":
        # Mask password in URL for display
        masked_url = db_url
        if "@" in masked_url:
            parts = masked_url.split("@")
            user_pass = parts[0].split("//")[1]
            if ":" in user_pass:
                user, password = user_pass.split(":", 1)
                masked_password = password[:3] + "*" * (len(password) - 3) if len(password) > 3 else "*" * len(password)
                masked_url = masked_url.replace(f":{password}@", f":{masked_password}@")
        print(f"Target Database: {masked_url}")
    
    print()
    response = input("Type 'NUCLEAR' to confirm complete database destruction: ")
    return response.strip() == "NUCLEAR"

def init_database(email: str, password: str, user_id: str, force: bool = False, 
                 engine: Engine = None, session_local = None, db_url: str = None) -> bool:
    """Initialize database safely (won't affect existing tables)"""
    print("🍕 Label Pizza Database Initialization")
    print("=" * 40)
    print("Mode: INIT (safe for existing databases)")
    print()
    
    if not force:
        print(f"📧 Email: {email}")
        print(f"👤 User ID: {user_id}")
        print(f"🔑 Password: {'*' * len(password)}")
        print()
        response = input("Initialize database with these settings? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("❌ Initialization cancelled")
            return False
    
    try:
        print("\n🚀 Starting database initialization...")
        
        manager = NuclearDatabaseManager(engine, session_local, db_url)
        
        # Create missing tables (safe operation)
        if not manager.create_all_tables(mode="init"):
            return False
        
        # Seed admin user
        if not manager.seed_admin_user(email, password, user_id):
            return False
        
        # Verify setup
        if not manager.verify_database(email):
            return False
        
        print("\n🎉 Database initialization completed successfully!")
        print()
        print("You can now run your Streamlit app:")
        print("  streamlit run app.py")
        print()
        print("Login credentials:")
        print(f"  Email: {email}")
        print(f"  Password: {password}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Initialization failed: {e}")
        return False

def nuclear_reset_database(email: str, password: str, user_id: str, force: bool = False,
                          auto_backup: bool = False, backup_dir: str = "./backups", 
                          backup_file: Optional[str] = None, compress: bool = True,
                          engine: Engine = None, session_local = None, db_url: str = None) -> bool:
    """Nuclear reset: completely destroy and recreate database"""
    print("☢️  Label Pizza Nuclear Database Reset")
    print("=" * 40)
    print("Mode: NUCLEAR RESET (COMPLETE DESTRUCTION!)")
    print("Method: DROP TABLE CASCADE for every single table")
    print()
    
    # Get database URL for backup
    if not db_url:
        db_url = os.getenv("DBURL")
    if not db_url:
        print("❌ DBURL environment variable not found")
        return False
    
    # Create backup if requested
    backup_created = None
    if auto_backup:
        backup_created = create_backup_if_requested(db_url, backup_dir, backup_file, compress)
        if backup_created is None and not force:
            response = input("Backup failed. Continue with nuclear option anyway? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("❌ Nuclear reset cancelled due to backup failure")
                return False
    
    # Confirm nuclear reset
    if not force and not confirm_nuclear_reset():
        print("❌ Nuclear reset cancelled")
        return False
    
    try:
        print("\n☢️  INITIATING NUCLEAR DATABASE DESTRUCTION...")
        
        manager = NuclearDatabaseManager(engine, session_local, db_url)
        
        # Nuclear table destruction (the most aggressive approach!)
        if not manager.nuclear_table_destruction():
            return False
        
        # Create tables
        if not manager.create_all_tables(mode="reset"):
            return False
        
        # Seed admin user
        if not manager.seed_admin_user(email, password, user_id):
            return False
        
        # Verify setup
        if not manager.verify_database(email):
            return False
        
        print("\n🎉 Nuclear reset completed successfully!")
        
        if backup_created:
            print(f"💾 Backup saved to: {backup_created}")
        
        print()
        print("You can now run your Streamlit app:")
        print("  streamlit run app.py")
        print()
        print("Login credentials:")
        print(f"  Email: {email}")
        print(f"  Password: {password}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Nuclear reset failed: {e}")
        if backup_created:
            print(f"💾 You can restore from backup: {backup_created}")
            print(f"   python label_pizza/init_or_reset_db.py --mode restore --backup-dir {backup_dir} --backup-file {os.path.basename(backup_created)}")
        return False

def restore_mode(backup_dir: str, backup_file: str, email: str, password: str, user_id: str, 
                force: bool = False, engine: Engine = None, session_local = None, db_url: str = None) -> bool:
    """Restore mode: restore from backup and recreate admin user"""
    print("🍕 Label Pizza Database Restore")
    print("=" * 40)
    print(f"Backup directory: {backup_dir}")
    print(f"Backup file: {backup_file}")
    print()
    
    # Get database URL
    if not db_url:
        db_url = os.getenv("DBURL")
    if not db_url:
        print("❌ DBURL environment variable not found")
        return False
    
    try:
        # Restore from backup
        if not restore_from_backup(db_url, backup_dir, backup_file, force):
            return False
        
        manager = NuclearDatabaseManager(engine, session_local, db_url)
        
        # Recreate admin user (in case it wasn't in backup or password changed)
        if not manager.seed_admin_user(email, password, user_id):
            print("⚠️  Could not create admin user (may already exist)")
        
        # Verify setup
        if not manager.verify_database(email):
            print("⚠️  Database verification had issues")
        
        print("\n🎉 Database restore completed successfully!")
        print()
        print("Login credentials:")
        print(f"  Email: {email}")
        print(f"  Password: {password}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Restore failed: {e}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Nuclear database init/reset script (complete table destruction)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Safe initialization
  python label_pizza/init_or_reset_db.py --mode init --email admin@example.com --password mypass --user-id "Admin"
  
  # Nuclear reset with automatic backup (RECOMMENDED)
  python label_pizza/init_or_reset_db.py --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup
  
  # Nuclear reset with custom backup directory
  python label_pizza/init_or_reset_db.py --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup --backup-dir ./my_backups --compress
  
  # Nuclear reset with specific backup filename
  python label_pizza/init_or_reset_db.py --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup --backup-dir ./backups --backup-file nuclear_backup.sql.gz
  
  # Restore from backup (filename + backup dir)
  python label_pizza/init_or_reset_db.py --mode restore --backup-dir ./backups --backup-file nuclear_backup.sql.gz --email admin@example.com --password mypass --user-id "Admin"
  
  # Restore from backup (full path)
  python label_pizza/init_or_reset_db.py --mode restore --backup-file ./backups/nuclear_backup.sql.gz --email admin@example.com --password mypass --user-id "Admin"
  
  # Force nuclear operations (skip confirmations)
  python label_pizza/init_or_reset_db.py --mode reset --email admin@example.com --password mypass --user-id "Admin" --auto-backup --force

Nuclear Improvements:
  - Uses DROP TABLE CASCADE for every single table individually
  - Complete table destruction and recreation from SQLAlchemy models
  - Works around ALL Supabase limitations and connection routing issues
  - Requires typing 'NUCLEAR' to confirm (prevents accidents)
  - Most aggressive database reset possible - guaranteed to work!
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["init", "reset", "restore"],
        default="init",
        help="Operation mode"
    )
    
    parser.add_argument(
        "--email",
        default="admin@example.com",
        help="Email address for the admin user"
    )
    
    parser.add_argument(
        "--password",
        default="password123",
        help="Password for the admin user"
    )
    
    parser.add_argument(
        "--user-id",
        default="Admin User",
        help="User ID for the admin user"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip all confirmation prompts"
    )
    
    parser.add_argument(
        "--auto-backup",
        action="store_true",
        help="Create automatic backup before nuclear reset"
    )
    
    parser.add_argument(
        "--backup-dir",
        default="./backups",
        help="Backup directory for auto-named files or restore operations"
    )
    
    parser.add_argument(
        "--backup-file",
        help="Backup file path (filename or full path) for backup or restore modes"
    )
    
    parser.add_argument(
        "--compress",
        action="store_true",
        default=True,
        help="Compress backup with gzip (default: True)"
    )
    
    parser.add_argument(
        "--no-compress",
        action="store_true",
        help="Disable backup compression"
    )

    parser.add_argument(
        "--database-url-name",
        default="DBURL", 
        help="Environment variable name for database URL"
    )
    
    args = parser.parse_args()
    
    # Handle compression flag
    if args.no_compress:
        compress = False
    else:
        compress = args.compress

    # Initialize database connection
    try:
        init_db(args.database_url_name)
        print(f"✅ Database initialized using {args.database_url_name}")
        from label_pizza.db import engine, SessionLocal
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        sys.exit(1)
    
    # Check database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Check your DBURL in .env file")
        sys.exit(1)
    
    # Get database URL
    db_url = os.getenv(args.database_url_name)
    
    # Run the appropriate mode
    if args.mode == "init":
        success = init_database(
            args.email, args.password, args.user_id, args.force, 
            engine, SessionLocal, db_url
        )
    elif args.mode == "reset":
        success = nuclear_reset_database(
            args.email, args.password, args.user_id, args.force,
            args.auto_backup, args.backup_dir, args.backup_file, compress,
            engine, SessionLocal, db_url
        )
    elif args.mode == "restore":
        if not args.backup_file:
            print("❌ --backup-file is required for restore mode")
            sys.exit(1)
        success = restore_mode(
            args.backup_dir, args.backup_file, args.email, args.password, args.user_id,
            args.force, engine, SessionLocal, db_url
        )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()