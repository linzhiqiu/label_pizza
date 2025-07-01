#!/usr/bin/env python3
"""
Migration script to add new schema columns and ProjectVideoQuestionDisplay table.
This script:
1. Adds has_custom_display column to schemas table if it doesn't exist
2. Adds instructions_url column to schemas table if it doesn't exist
3. Creates ProjectVideoQuestionDisplay table if it doesn't exist
4. Handles column rename from custom_display_values to custom_option_display_map
5. Populates has_custom_display with default values
"""

import os
import argparse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url-name", default="DBURL")
    args = parser.parse_args()
    
    # Load environment variables from .env file (same as your db.py)
    print("🔧 Loading environment variables...")
    
    # Try to find .env file in current directory or parent directory
    env_paths = [".env", "../.env", "label_pizza/.env"]
    env_loaded = False
    
    for env_path in env_paths:
        if os.path.exists(env_path):
            print(f"📁 Found .env file at: {env_path}")
            load_dotenv(env_path)
            env_loaded = True
            break
    
    if not env_loaded:
        print("⚠️  No .env file found, trying environment variables directly...")
        load_dotenv()  # Try to load from default locations
    
    # Get database URL from environment
    db_url = os.environ.get(args.database_url_name)
    if not db_url:
        print(f"❌ Environment variable {args.database_url_name} not found")
        print(f"\n💡 Available environment variables containing 'DB' or 'URL':")
        relevant_vars = []
        for key in sorted(os.environ.keys()):
            if 'DB' in key.upper() or 'URL' in key.upper():
                # Don't show the full value for security, just confirm it exists
                value_preview = f"{os.environ[key][:20]}..." if len(os.environ[key]) > 20 else os.environ[key]
                relevant_vars.append(f"   - {key}: {value_preview}")
        
        if relevant_vars:
            for var in relevant_vars:
                print(var)
        else:
            print("   (No environment variables found containing 'DB' or 'URL')")
        
        print(f"\n🔍 Current working directory: {os.getcwd()}")
        print(f"📁 Looking for .env file in: {', '.join(env_paths)}")
        
        return
    
    print(f"✅ Using database URL from {args.database_url_name}")
    print(f"🔧 Connecting to database...")
    
    engine = create_engine(db_url)
    
    try:
        with engine.begin() as conn:
            # Step 1: Check if new columns exist in schemas table
            print("🔍 Checking if new schema columns exist...")
            
            check_has_custom_display = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = 'schemas' 
                AND column_name = 'has_custom_display'
            """))
            
            check_instructions_url = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = 'schemas' 
                AND column_name = 'instructions_url'
            """))
            
            has_custom_display_exists = check_has_custom_display.fetchone()[0] > 0
            instructions_url_exists = check_instructions_url.fetchone()[0] > 0
            
            # Step 2: Add missing columns
            if has_custom_display_exists:
                print("✅ has_custom_display column already exists")
            else:
                print("➕ Adding has_custom_display column...")
                conn.execute(text("""
                    ALTER TABLE schemas 
                    ADD COLUMN has_custom_display BOOLEAN DEFAULT FALSE
                """))
                print("✅ has_custom_display column added successfully")
            
            if instructions_url_exists:
                print("✅ instructions_url column already exists")
            else:
                print("➕ Adding instructions_url column...")
                conn.execute(text("""
                    ALTER TABLE schemas 
                    ADD COLUMN instructions_url TEXT
                """))
                print("✅ instructions_url column added successfully")
            
            # Step 3: Check if ProjectVideoQuestionDisplay table exists
            print("\n🔍 Checking if project_video_question_displays table exists...")
            
            check_table = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name = 'project_video_question_displays'
            """))
            
            table_exists = check_table.fetchone()[0] > 0
            
            if table_exists:
                print("✅ project_video_question_displays table already exists")
                
                # Step 3a: Check for column rename (custom_display_values -> custom_option_display_map)
                print("🔍 Checking for column name updates in project_video_question_displays...")
                
                check_old_column = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_name = 'project_video_question_displays' 
                    AND column_name = 'custom_display_values'
                """))
                
                check_new_column = conn.execute(text("""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_name = 'project_video_question_displays' 
                    AND column_name = 'custom_option_display_map'
                """))
                
                old_column_exists = check_old_column.fetchone()[0] > 0
                new_column_exists = check_new_column.fetchone()[0] > 0
                
                if old_column_exists and not new_column_exists:
                    print("🔄 Renaming custom_display_values to custom_option_display_map...")
                    conn.execute(text("""
                        ALTER TABLE project_video_question_displays 
                        RENAME COLUMN custom_display_values TO custom_option_display_map
                    """))
                    print("✅ Column renamed successfully")
                elif old_column_exists and new_column_exists:
                    print("⚠️  Both old and new columns exist! Manual intervention may be required.")
                    print("   Old column: custom_display_values")
                    print("   New column: custom_option_display_map")
                    print("   Consider migrating data and dropping the old column.")
                elif new_column_exists:
                    print("✅ custom_option_display_map column already exists")
                else:
                    print("➕ Adding missing custom_option_display_map column...")
                    conn.execute(text("""
                        ALTER TABLE project_video_question_displays 
                        ADD COLUMN custom_option_display_map JSONB
                    """))
                    print("✅ custom_option_display_map column added successfully")
                    
            else:
                print("➕ Creating project_video_question_displays table...")
                
                # Create the table with all constraints and indexes
                conn.execute(text("""
                    CREATE TABLE project_video_question_displays (
                        project_id INTEGER NOT NULL,
                        video_id INTEGER NOT NULL,
                        question_id INTEGER NOT NULL,
                        custom_display_text TEXT,
                        custom_option_display_map JSONB,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (project_id, video_id, question_id)
                    )
                """))
                
                # Add indexes
                conn.execute(text("""
                    CREATE INDEX ix_project_display_lookup 
                    ON project_video_question_displays (project_id)
                """))
                
                conn.execute(text("""
                    CREATE INDEX ix_video_display_lookup 
                    ON project_video_question_displays (project_id, video_id)
                """))
                
                print("✅ project_video_question_displays table created successfully")
            
            # Step 4: Check current state of schemas
            print("\n📊 Checking current schemas...")
            
            result = conn.execute(text("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN has_custom_display IS NOT NULL THEN 1 ELSE 0 END) as with_has_custom_display,
                       SUM(CASE WHEN has_custom_display = TRUE THEN 1 ELSE 0 END) as custom_display_enabled
                FROM schemas
            """))
            
            row = result.fetchone()
            total_schemas = row[0]
            schemas_with_column = row[1]
            custom_display_enabled = row[2]
            
            print(f"📋 Found {total_schemas} schemas")
            print(f"✅ Have has_custom_display column: {schemas_with_column}")
            print(f"🎨 Have custom display enabled: {custom_display_enabled}")
            
            # Step 5: Update any NULL values to default (FALSE) for has_custom_display
            if not has_custom_display_exists:
                print("\n🔄 Setting default values for has_custom_display...")
                
                update_result = conn.execute(text("""
                    UPDATE schemas 
                    SET has_custom_display = FALSE 
                    WHERE has_custom_display IS NULL
                """))
                
                updated_count = update_result.rowcount
                print(f"✅ Updated {updated_count} schemas with default has_custom_display value")
            
            # Step 6: Show current state
            print("\n📋 Current schema summary:")
            
            summary_result = conn.execute(text("""
                SELECT id, name, 
                       CASE WHEN has_custom_display THEN 'Yes' ELSE 'No' END as custom_display,
                       CASE WHEN instructions_url IS NOT NULL THEN 'Set' ELSE 'NULL' END as instructions
                FROM schemas 
                ORDER BY id 
                LIMIT 10
            """))
            
            print("ID | Name | Custom Display | Instructions URL")
            print("---|------|----------------|------------------")
            for row in summary_result:
                name_truncated = row[1][:20] + "..." if len(row[1]) > 20 else row[1]
                print(f"{row[0]:2} | {name_truncated:20} | {row[2]:14} | {row[3]}")
            
            if total_schemas > 10:
                print(f"... and {total_schemas - 10} more schemas")
            
            # Step 7: Show ProjectVideoQuestionDisplay table status and column info
            print("\n🔍 Checking project_video_question_displays table structure...")
            
            # Check current columns
            columns_result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'project_video_question_displays'
                ORDER BY ordinal_position
            """))
            
            print("📋 Current table columns:")
            for column in columns_result:
                nullable = "NULL" if column[2] == "YES" else "NOT NULL"
                print(f"   • {column[0]} ({column[1]}) - {nullable}")
            
            display_count = conn.execute(text("""
                SELECT COUNT(*) FROM project_video_question_displays
            """))
            
            display_rows = display_count.fetchone()[0]
            print(f"\n📊 project_video_question_displays table has {display_rows} rows")
            
            print("\n🎉 Migration completed successfully!")
            print("\nSummary of changes:")
            if not has_custom_display_exists:
                print("  ✅ Added has_custom_display column to schemas table")
            if not instructions_url_exists:
                print("  ✅ Added instructions_url column to schemas table")
            if not table_exists:
                print("  ✅ Created project_video_question_displays table")
            else:
                print("  ✅ Verified project_video_question_displays table structure")
            print("  ✅ All schemas have proper default values")
                
    except Exception as e:
        print(f"💥 Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🍕 Schema Enhancement Migration")
    print("=" * 35)
    print("This script will:")
    print("1. 📁 Load .env file (like your db.py)")
    print("2. ➕ Add has_custom_display column to schemas (if needed)")
    print("3. ➕ Add instructions_url column to schemas (if needed)")
    print("4. 🏗️  Create project_video_question_displays table (if needed)")
    print("5. 🔄 Handle column rename: custom_display_values → custom_option_display_map")
    print("6. 🔄 Set default values for new columns")
    print("7. ✅ Verify the migration")
    print()
    main()