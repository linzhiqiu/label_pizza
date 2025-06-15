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
        # 1. 获取项目ID
        project = ProjectService.get_project_by_name(project_name, session)
        project_id = project.id
        # 2. 获取视频名到ID映射
        video_map = {v['uid']: v['id'] for v in VideoService.get_project_videos(project_id, session)}
        # 3. 获取所有问题组及其问题
        # 获取schema下所有问题组
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
        # 4. 解析json
        annotation_data = parse_annotation_json(json_path)
        # 5. 上传
        for video_name, annotators in annotation_data.items():
            if video_name not in video_map:
                print(f"视频 {video_name} 不在项目中，跳过")
                continue
            video_id = video_map[video_name]
            for email, answers in annotators.items():
                try:
                    annotator = AuthService.get_user_by_email(email, session)
                    annotator_id = annotator.id
                except Exception as e:
                    print(f"标注者 {email} 不存在，跳过")
                    continue
                # 按问题组分组上传
                group_answers = {}
                for q_text, ans in answers.items():
                    if q_text not in question_text_to_group_id:
                        print(f"问题 {q_text} 不在项目中，跳过")
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
                        print(f"上传成功: {video_name} - {email} - group {group_id}")
                    except Exception as e:
                        print(f"上传失败: {video_name} - {email} - group {group_id}，原因：{e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_path", type=str, required=True, help="标注json路径")
    parser.add_argument("--project_name", type=str, required=True, help="项目名称")
    args = parser.parse_args()
    upload_annotations(args.json_path, args.project_name)