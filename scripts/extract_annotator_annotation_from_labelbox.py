import json
from collections import defaultdict
from typing import List, Dict, Any
import os


major_lioght_source = {
    "Sunlight": "Is sunlight the major light source?",
    "Moonlight / Starlight": "Is moonlight / starlight the major light source?",
    "Firelight": "Is firelight the major light source?",
    "Artificial Lighting (Practical/Visible)": "Is a practical / visible artificial light the major source?",
    "Non-Visible Light Sources": "Is a non-visible light source the major source?",
    "N/A (abstract)": "Is the lighting abstract / N/A?",
    "Complex (others)": "Is the major light source complex / other type?"
}

light_direction = {
    "Front light": "Is there front light on the subject?",
    "Back light": "Is there back light on the subject?",
    "Right-side light": "Is there right-side light on the subject?",
    "Left-side light": "Is there left-side light on the subject?",
    "Top light": "Is there top light on the subject?",
    "Bottom light": "Is there bottom light on the subject?",
    "No dominant direction": "Is lighting direction ambient / no dominant side?"
}
special_lighting_subject = {
    "Rembrandt Lighting": "Is Rembrandt lighting used on the subject?",
    "Silhouette Lighting (Subject Not Always Required)": "Is the subject lit as a silhouette?",
    "Rim Light (Subject Not Always Required)": "Is rim lighting present on the subject?"
}

def extract_annotator_annotations(input_file_path: str, output_file_path: str) -> None:
    """
    Extract annotator annotations from Labelbox NDJSON file and save to JSON
    
    Args:
        input_file_path: Path to the input NDJSON file
        output_file_path: Path to save the output JSON file
    """
    # Read NDJSON data
    with open(input_file_path, 'r') as f:
        data = [json.loads(line) for line in f]

    # Build structure: external_id -> email -> {question: answer}
    result_dict = defaultdict(lambda: defaultdict(dict))

    for item in data:
        # Get external_id from data_row
        external_id = item.get("data_row", {}).get("external_id")
        if not external_id:
            continue

        # Process each project
        projects = item.get("projects", {})
        for project_id, project_data in projects.items():
            # Process each label in the project
            for label in project_data.get("labels", []):
                # Get annotator email from label_details
                annotator_email = label.get("label_details", {}).get("created_by")
                if not annotator_email:
                    continue

                # Process classifications
                annotations = label.get("annotations", {}).get("classifications", [])
                for ann in annotations:
                    question = ann.get("name")
                    if not question:
                        continue

                    # 处理三类特殊问题
                    if question == "Special Lighting on Subject(s) is:" and "checklist_answers" in ann:
                        # 先全部设为no
                        for k, v in special_lighting_subject.items():
                            result_dict[external_id][annotator_email][v] = "No"
                        # 再把选中的设为yes
                        for ans in ann["checklist_answers"]:
                            name = ans.get("name")
                            if name in special_lighting_subject:
                                result_dict[external_id][annotator_email][special_lighting_subject[name]] = "Yes"
                        continue

                    if question == "Select light directions:" and "checklist_answers" in ann:
                        for k, v in light_direction.items():
                            result_dict[external_id][annotator_email][v] = "No"
                        for ans in ann["checklist_answers"]:
                            name = ans.get("name")
                            if name in light_direction:
                                result_dict[external_id][annotator_email][light_direction[name]] = "Yes"
                        continue

                    if question == "What is the major light source?" and "checklist_answers" in ann:
                        for k, v in major_lioght_source.items():
                            result_dict[external_id][annotator_email][v] = "No"
                        for ans in ann["checklist_answers"]:
                            name = ans.get("name")
                            if name in major_lioght_source:
                                result_dict[external_id][annotator_email][major_lioght_source[name]] = "Yes"
                        continue

                    # 其它问题保持原有逻辑
                    if "radio_answer" in ann:
                        answer = ann["radio_answer"].get("name")
                    elif "checklist_answers" in ann:
                        answer = [ans.get("name") for ans in ann["checklist_answers"]]
                    elif "text_answer" in ann:
                        answer = ann["text_answer"].get("content")
                    else:
                        continue

                    if answer:
                        result_dict[external_id][annotator_email][question] = answer

    # Convert to desired format
    output = []
    for ext_id, annotators in result_dict.items():
        entry = {
            ext_id: [
                {email: answers} for email, answers in annotators.items()
            ]
        }
        output.append(entry)

    # Write to JSON file
    with open(output_file_path, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"✅ Done.")

if __name__ == "__main__":
    # Example usage
    # input_path = "/Users/censiyuan/Desktop/2025CVPR_VideoAnnotation/temp_branch/video_annotation/exports_pretests/lighting/ndjson/Lightingtest_0_subject_light_cmbe2wl1d0hmh07yxhg91caea_20250614_034417.ndjson"
    # output_path = "/Users/censiyuan/Desktop/2025CVPR_VideoAnnotation/temp_branch/video_annotation/exports_pretests/lighting/json/" + os.path.splitext(os.path.basename(input_path))[0] + ".json"
    import glob
    input_paths = glob.glob("/Users/censiyuan/Desktop/2025CVPR_VideoAnnotation/temp_branch/video_annotation/exports_pretests/lighting/ndjson/*.ndjson")
    for input_path in input_paths:
        output_path = "/Users/censiyuan/Desktop/2025CVPR_VideoAnnotation/temp_branch/video_annotation/exports_pretests/lighting/json/" + os.path.splitext(os.path.basename(input_path))[0] + ".json"
        extract_annotator_annotations(input_path, output_path)