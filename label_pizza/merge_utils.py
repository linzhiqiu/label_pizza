import json
import os
from typing import Dict, List, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime


def merge_videos(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge videos.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing videos.json
        folder2_path (str): Path to second folder containing videos.json
        output_folder (str): Output folder path where videos.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "videos.json"
    file2_path = Path(folder2_path) / "videos.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "videos.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["video_uid"]: item for item in data1}
    dict2 = {item["video_uid"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "video_uid": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_users(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge users.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing users.json
        folder2_path (str): Path to second folder containing users.json
        output_folder (str): Output folder path where users.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "users.json"
    file2_path = Path(folder2_path) / "users.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "users.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["user_id"]: item for item in data1}
    dict2 = {item["user_id"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "user_id": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_schemas(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge schemas.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing schemas.json
        folder2_path (str): Path to second folder containing schemas.json
        output_folder (str): Output folder path where schemas.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "schemas.json"
    file2_path = Path(folder2_path) / "schemas.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "schemas.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["schema_name"]: item for item in data1}
    dict2 = {item["schema_name"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "schema_name": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_projects(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge projects.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing projects.json
        folder2_path (str): Path to second folder containing projects.json
        output_folder (str): Output folder path where projects.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "projects.json"
    file2_path = Path(folder2_path) / "projects.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "projects.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["project_name"]: item for item in data1}
    dict2 = {item["project_name"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "project_name": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_project_groups(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge project_groups.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing project_groups.json
        folder2_path (str): Path to second folder containing project_groups.json
        output_folder (str): Output folder path where project_groups.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "project_groups.json"
    file2_path = Path(folder2_path) / "project_groups.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "project_groups.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    dict1 = {item["project_group_name"]: item for item in data1}
    dict2 = {item["project_group_name"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "project_group_name": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_assignments(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge assignments.json files from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing assignments.json
        folder2_path (str): Path to second folder containing assignments.json
        output_folder (str): Output folder path where assignments.json will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    file1_path = Path(folder1_path) / "assignments.json"
    file2_path = Path(folder2_path) / "assignments.json"
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    output_path = Path(output_folder) / "assignments.json"
    
    data1 = _read_json_file(file1_path)
    data2 = _read_json_file(file2_path)
    
    def make_assignment_key(item):
        return f"{item['user_name']}|{item['project_name']}|{item['role']}"
    
    dict1 = {make_assignment_key(item): item for item in data1}
    dict2 = {make_assignment_key(item): item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "key": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Convert back to list and write
    merged_data = list(merged_dict.values())
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_data),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_question_groups(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge question_groups folders from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing question_groups folder
        folder2_path (str): Path to second folder containing question_groups folder
        output_folder (str): Output folder path where question_groups folder will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    qg_folder1 = Path(folder1_path) / "question_groups"
    qg_folder2 = Path(folder2_path) / "question_groups"
    
    # Create output folder for question groups
    qg_output_folder = Path(output_folder) / "question_groups"
    os.makedirs(qg_output_folder, exist_ok=True)
    
    # Load all question groups from both folders
    data1 = _load_all_files_in_folder(qg_folder1)
    data2 = _load_all_files_in_folder(qg_folder2)
    
    dict1 = {item["title"]: item for item in data1}
    dict2 = {item["title"]: item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "title": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Write each question group to separate file
    for title, qg_data in merged_dict.items():
        filename = f"{title.replace(' ', '_').replace('/', '_')}.json"
        filepath = qg_output_folder / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(qg_data, f, indent=4, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_dict),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_annotations(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge annotations folders from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing annotations folder
        folder2_path (str): Path to second folder containing annotations folder
        output_folder (str): Output folder path where annotations folder will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    ann_folder1 = Path(folder1_path) / "annotations"
    ann_folder2 = Path(folder2_path) / "annotations"
    
    # Create output folder for annotations
    ann_output_folder = Path(output_folder) / "annotations"
    os.makedirs(ann_output_folder, exist_ok=True)
    
    # Load all annotations from both folders
    data1 = _load_all_files_in_folder(ann_folder1)
    data2 = _load_all_files_in_folder(ann_folder2)
    
    def make_annotation_key(item):
        return f"{item['video_uid']}|{item['project_name']}|{item['question_group_title']}|{item['user_name']}"
    
    dict1 = {make_annotation_key(item): item for item in data1}
    dict2 = {make_annotation_key(item): item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "key": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Group by question_group_title and write to separate files
    grouped_annotations = {}
    for annotation in merged_dict.values():
        qg_title = annotation['question_group_title']
        if qg_title not in grouped_annotations:
            grouped_annotations[qg_title] = []
        grouped_annotations[qg_title].append(annotation)
    
    # Write each question group's annotations to separate file
    for qg_title, annotations in grouped_annotations.items():
        filename = f"{qg_title.replace(' ', '_').replace('/', '_')}_annotations.json"
        filepath = ann_output_folder / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(annotations, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_dict),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_ground_truths(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge ground_truths folders from two folders.
    
    Args:
        folder1_path (str): Path to first folder containing ground_truths folder
        folder2_path (str): Path to second folder containing ground_truths folder
        output_folder (str): Output folder path where ground_truths folder will be created
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Merge report with statistics
    """
    gt_folder1 = Path(folder1_path) / "ground_truths"
    gt_folder2 = Path(folder2_path) / "ground_truths"
    
    # Create output folder for ground truths
    gt_output_folder = Path(output_folder) / "ground_truths"
    os.makedirs(gt_output_folder, exist_ok=True)
    
    # Load all ground truths from both folders
    data1 = _load_all_files_in_folder(gt_folder1)
    data2 = _load_all_files_in_folder(gt_folder2)
    
    def make_ground_truth_key(item):
        return f"{item['video_uid']}|{item['project_name']}|{item['question_group_title']}|{item['user_name']}"
    
    dict1 = {make_ground_truth_key(item): item for item in data1}
    dict2 = {make_ground_truth_key(item): item for item in data2}
    
    merged_dict = {}
    conflicts = []
    
    # Add all items from folder1
    for key, item in dict1.items():
        merged_dict[key] = item
    
    # Add items from folder2, handle conflicts
    for key, item in dict2.items():
        if key in merged_dict:
            if merged_dict[key] != item:
                conflicts.append({
                    "key": key,
                    "folder1_data": merged_dict[key],
                    "folder2_data": item
                })
                # Use folder2 data if use_first_folder_on_conflict is False
                if not use_first_folder_on_conflict:
                    merged_dict[key] = item
        else:
            merged_dict[key] = item
    
    # Group by question_group_title and write to separate files
    grouped_gts = {}
    for gt in merged_dict.values():
        qg_title = gt['question_group_title']
        if qg_title not in grouped_gts:
            grouped_gts[qg_title] = []
        grouped_gts[qg_title].append(gt)
    
    # Write each question group's ground truths to separate file
    for qg_title, gts in grouped_gts.items():
        filename = f"{qg_title.replace(' ', '_').replace('/', '_')}_ground_truths.json"
        filepath = gt_output_folder / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(gts, f, indent=2, ensure_ascii=False)
    
    return {
        "merged_count": len(merged_dict),
        "folder1_count": len(data1),
        "folder2_count": len(data2),
        "conflicts_resolved": len(conflicts),
        "used_folder1_on_conflict": use_first_folder_on_conflict
    }


def merge_workspace(folder1_path: str, folder2_path: str, output_folder: str, use_first_folder_on_conflict: bool = True) -> Dict:
    """
    Merge entire workspace between two folders.
    
    Args:
        folder1_path (str): Path to first workspace folder
        folder2_path (str): Path to second workspace folder
        output_folder (str): Output folder path for merged workspace
        use_first_folder_on_conflict (bool): If True, use folder1 data on conflicts; if False, use folder2 data
        
    Returns:
        dict: Complete merge report with statistics for all components
    """
    # Create output folder structure
    os.makedirs(output_folder, exist_ok=True)
    
    merge_report = {}
    
    # Merge all components
    merge_report["videos"] = merge_videos(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["users"] = merge_users(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["schemas"] = merge_schemas(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["projects"] = merge_projects(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["project_groups"] = merge_project_groups(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["assignments"] = merge_assignments(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["question_groups"] = merge_question_groups(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["annotations"] = merge_annotations(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    merge_report["ground_truths"] = merge_ground_truths(folder1_path, folder2_path, output_folder, use_first_folder_on_conflict)
    
    # Calculate overall statistics
    total_conflicts = sum(report["conflicts_resolved"] for report in merge_report.values())
    merge_report["summary"] = {
        "total_conflicts_resolved": total_conflicts,
        "used_folder1_on_conflict": use_first_folder_on_conflict,
        "merge_completed": True
    }
    
    return merge_report


def _load_all_files_in_folder(folder_path: Path) -> List[Dict]:
    """Load all JSON files in a folder and return combined list of items."""
    all_items = []
    
    if not folder_path.exists():
        return all_items
    
    for json_file in folder_path.glob("*.json"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both single dict and list of dicts
            if isinstance(data, dict):
                all_items.append(data)
            elif isinstance(data, list):
                all_items.extend(data)
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
    
    return all_items


def _read_json_file(file_path: Path) -> List[Dict]:
    """Read and parse JSON file."""
    if not file_path.exists():
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        raise ValueError(f"JSON file {file_path} does not contain a list")
    
    return data