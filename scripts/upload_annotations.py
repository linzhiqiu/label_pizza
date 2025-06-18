import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from label_pizza.services import VideoService, ProjectService, QuestionService, QuestionGroupService, AnnotatorService, AuthService, SchemaService
from label_pizza.db import SessionLocal

def parse_annotation_json(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    result = {}
    for item in data:
        for video_name, annotator_list in item.items():
            result.setdefault(video_name, {})
            for annotator_dict in annotator_list:
                for email, answers in annotator_dict.items():
                    result[video_name][email] = answers
    return result

def upload_annotations(json_path, project_name):
    with SessionLocal() as session:
        # 1. Get project ID
        project = ProjectService.get_project_by_name(project_name, session)
        project_id = project.id
        # 2. Get video name to ID mapping
        video_map = {v['uid']: v['id'] for v in VideoService.get_project_videos(project_id, session)}
        # 3. Get all question groups and their questions
        # Get all question groups in the schema
        schema_id = project.schema_id
        group_df = SchemaService.get_schema_question_groups(schema_id, session)
        group_id_to_questions = {}
        question_text_to_group_id = {}
        question_text_to_id = {}
        for _, row in group_df.iterrows():
            group_id = row['ID']
            questions = SchemaService.get_questions_by_group_id(group_id, session)
            group_id_to_questions[group_id] = {q['text']: q['id'] for q in questions}
            for q in questions:
                question_text_to_group_id[q['text']] = group_id
                question_text_to_id[q['text']] = q['id']
        # 4. Parse json
        annotation_data = parse_annotation_json(json_path)
        # 5. Upload
        for video_name, annotators in annotation_data.items():
            if video_name not in video_map:
                print(f"Video {video_name} is not in the project, skipping")
                continue
            video_id = video_map[video_name]
            for email, answers in annotators.items():
                try:
                    annotator = AuthService.get_user_by_email(email, session)
                    annotator_id = annotator.id
                except Exception as e:
                    print(f"Annotator {email} does not exist, skipping")
                    continue
                # Group answers by question group
                group_answers = {}
                for q_text, ans in answers.items():
                    if q_text not in question_text_to_group_id:
                        print(f"Question {q_text} is not in the project, skipping")
                        continue
                    group_id = question_text_to_group_id[q_text]
                    if group_id not in group_answers:
                        group_answers[group_id] = {}
                    group_answers[group_id][q_text] = ans
                for group_id, answer_dict in group_answers.items():
                    try:
                        AnnotatorService.submit_answer_to_question_group(
                            video_id=video_id,
                            project_id=project_id,
                            user_id=annotator_id,
                            question_group_id=group_id,
                            answers=answer_dict,
                            session=session
                        )
                        print(f"Upload succeeded: {video_name} - {email} - group {group_id}")
                    except Exception as e:
                        print(f"Upload failed: {video_name} - {email} - group {group_id}, reason: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_path", type=str, required=True, help="Path to annotation json")
    parser.add_argument("--project_name", type=str, required=True, help="Project name")
    args = parser.parse_args()
    upload_annotations(args.json_path, args.project_name)