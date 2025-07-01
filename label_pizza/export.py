#!/usr/bin/env python3
"""
Ground Truth Export Script

This script exports ground truth data from one or more projects to JSON or Excel format.
It validates that reusable question groups have consistent answers across projects before exporting.
Supports both project IDs (integers) and project names (strings).

Usage:
    python label_pizza/export.py [projects...] --format [json|excel] --output output_file

Examples:
    python label_pizza/export.py 1 2 3 --format json --output export.json
    python label_pizza/export.py "Project Alpha" "Project Beta" --format excel --output results.xlsx
    python label_pizza/export.py 1 "Project Beta" 3 --format json --output mixed.json
"""

import argparse
import os
import json
import sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from label_pizza.services import GroundTruthExportService, ProjectService, save_export_as_json, save_export_as_excel
from label_pizza.db import init_database

def resolve_projects_to_ids(projects, session):
    """Convert project names/IDs to a list of project IDs.
    
    Args:
        projects: List of project IDs (int) or project names (str)
        session: Database session
        
    Returns:
        List of project IDs
        
    Raises:
        ValueError: If any project is not found
    """
    project_ids = []
    
    for project in projects:
        if isinstance(project, int):
            # It's already an ID, validate it exists
            try:
                ProjectService.get_project_dict_by_id(project, session)
                project_ids.append(project)
            except ValueError as e:
                raise ValueError(f"Project with ID {project} not found or is archived")
                
        elif isinstance(project, str):
            # It's a project name, resolve to ID
            try:
                project_obj = ProjectService.get_project_by_name(project, session)
                project_ids.append(project_obj.id)
            except ValueError:
                raise ValueError(f"Project with name '{project}' not found")
                
        else:
            raise ValueError(f"Invalid project identifier: {project}. Must be int (ID) or str (name)")
    
    return project_ids


def main():
    """Main script entry point."""
    parser = argparse.ArgumentParser(
        description="Export ground truth data from projects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 1 2 3 --format json --output export.json
  %(prog)s "Project Alpha" "Project Beta" --format excel --output results.xlsx
  %(prog)s 1 "Project Beta" 3 --format json --output mixed.json

The script will validate that reusable question groups have consistent answers
across projects before exporting. If inconsistencies are found, the export will
fail with detailed error information.

You can specify projects by ID (integer) or by name (string). Names with spaces
should be quoted.
        """
    )
    
    parser.add_argument(
        "projects", 
        nargs="+", 
        help="Project IDs (integers) or project names (strings) to export ground truth data from"
    )
    
    parser.add_argument(
        "--format", 
        choices=["json", "excel"], 
        default="json", 
        help="Export format (default: json)"
    )
    
    parser.add_argument(
        "--output", "-o", 
        required=True, 
        help="Output file path"
    )
    
    parser.add_argument(
        "--database-url-name",
        default="DBURL",
        help="Environment variable name for database URL (default: DBURL)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    

    load_dotenv(".env")  # loads DBURL
    if args.verbose:
        print("Loading database URL from .env file")

    # Initialize database
    try:
        init_database(args.database_url_name)
        if args.verbose:
            print(f"Database initialized using {args.database_url_name}")
        from label_pizza.db import SessionLocal
    except Exception as e:
        print(f"Error initializing database: {e}")
        return 1
    
    # Validate and prepare output path
    output_path = Path(args.output)
    if args.format == "json" and not output_path.suffix.lower() == ".json":
        output_path = output_path.with_suffix(".json")
        if args.verbose:
            print(f"Auto-corrected output path to: {output_path}")
    elif args.format == "excel" and not output_path.suffix.lower() in [".xlsx", ".xls"]:
        output_path = output_path.with_suffix(".xlsx")
        if args.verbose:
            print(f"Auto-corrected output path to: {output_path}")
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    
    try:
        with SessionLocal() as session:
            # Parse projects - can be IDs (integers) or names (strings)
            projects = []
            for proj in args.projects:
                try:
                    # Try to parse as integer (project ID)
                    projects.append(int(proj))
                except ValueError:
                    # If it fails, treat as string (project name)
                    projects.append(proj)
            
            if args.verbose:
                project_display = []
                for proj in projects:
                    if isinstance(proj, int):
                        project_display.append(f"ID:{proj}")
                    else:
                        project_display.append(f"'{proj}'")
                print(f"Exporting ground truth data from projects: {', '.join(project_display)}")
            
            # Resolve project names to IDs
            try:
                project_ids = resolve_projects_to_ids(projects, session)
                if args.verbose:
                    print(f"Resolved to project IDs: {project_ids}")
            except ValueError as e:
                print(f"✗ Error resolving projects: {e}")
                return 1
            
            # Export data with validation using existing service
            export_data = GroundTruthExportService.export_ground_truth_data(project_ids, session)
            
            if args.verbose:
                print(f"Found {len(export_data)} videos with ground truth data")
                
                # Show summary of questions found
                all_questions = set()
                for video in export_data:
                    all_questions.update(video["answers"].keys())
                print(f"Total unique questions: {len(all_questions)}")
            
            # Save data in requested format using existing functions
            if args.format == "json":
                save_export_as_json(export_data, str(output_path))
                print(f"✓ Successfully exported to JSON: {output_path}")
            else:  # excel
                save_export_as_excel(export_data, str(output_path))
                print(f"✓ Successfully exported to Excel: {output_path}")
            
            # Show some statistics
            if export_data:
                total_answers = sum(len(video["answers"]) for video in export_data)
                print(f"Export summary:")
                print(f"  - Videos: {len(export_data)}")
                print(f"  - Total answers: {total_answers}")
                print(f"  - Average answers per video: {total_answers/len(export_data):.1f}")
            else:
                print("Warning: No ground truth data found for the specified projects")
    
    except ValueError as e:
        print(f"✗ Export failed: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n✗ Export cancelled by user")
        return 1
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


def validate_projects_only(projects, database_url_name="DBURL", verbose=False):
    """Utility function to only validate reusable question groups without exporting.
    
    Args:
        projects: List of project IDs (int) or project names (str) to validate
        database_url_name: Environment variable name for database URL (default: DBURL)
        verbose: Enable verbose output
        
    Returns:
        True if validation passes, False otherwise
    """
    load_dotenv(".env")
    try:
        init_database(database_url_name)
        if verbose:
            print(f"Database initialized using {database_url_name}")
        from label_pizza.db import SessionLocal
    except Exception as e:
        print(f"Error: Database initialization failed: {e}")
        return False
    
    try:
        with SessionLocal() as session:
            if verbose:
                project_display = []
                for proj in projects:
                    if isinstance(proj, int):
                        project_display.append(f"ID:{proj}")
                    else:
                        project_display.append(f"'{proj}'")
                print(f"Validating reusable question groups for projects: {', '.join(project_display)}")
            
            # Resolve projects to IDs first, then validate
            project_ids = resolve_projects_to_ids(projects, session)
            GroundTruthExportService._validate_reusable_question_groups(project_ids, session)
            
            if verbose:
                print("✓ Validation passed - no inconsistencies found")
            return True
            
    except ValueError as e:
        print(f"✗ Validation failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error during validation: {e}")
        return False


if __name__ == "__main__":
    # Support direct validation mode
    if len(sys.argv) > 1 and sys.argv[1] == "validate":
        # Usage: python label_pizza/export.py validate 1 2 "Project Name"
        try:
            projects = []
            for proj_str in sys.argv[2:]:
                try:
                    # Try to parse as integer (project ID)
                    projects.append(int(proj_str))
                except ValueError:
                    # If it fails, treat as string (project name)
                    projects.append(proj_str)
            
            if not projects:
                print("Usage: python label_pizza/export.py validate [project_ids_or_names...]")
                print("Examples:")
                print("  python label_pizza/export.py validate 1 2 3")
                print('  python label_pizza/export.py validate "Project Alpha" "Project Beta"')
                print('  python label_pizza/export.py validate 1 "Project Beta" 3')
                sys.exit(1)
            
            success = validate_projects_only(projects, verbose=True)
            sys.exit(0 if success else 1)
        except Exception as e:
            print(f"Error during validation: {e}")
            sys.exit(1)
    else:
        # Normal export mode
        sys.exit(main())