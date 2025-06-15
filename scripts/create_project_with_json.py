import json
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from label_pizza.services import VideoService, ProjectService, SchemaService
from label_pizza.db import SessionLocal

def extract_video_names_from_annotation_json(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    video_names = []
    for item in data:
        video_names.extend(item.keys())
    return video_names

def create_project_from_annotation_json(json_path, project_name, schema_name, batch_size=200):
    # 1. 提取视频名
    video_names = extract_video_names_from_annotation_json(json_path)
    print(f"共提取到 {len(video_names)} 个视频名。")
    # 2. 连接数据库
    session = SessionLocal()
    try:
        # 3. 获取schema id
        schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
        print(f"找到schema '{schema_name}', ID: {schema_id}")
        # 4. 获取所有视频
        all_videos_df = VideoService.get_all_videos(session)
        existing_video_uids = set(all_videos_df['Video UID'])
        # 5. 检查缺失
        missing_videos = [name for name in video_names if name not in existing_video_uids]
        if missing_videos:
            print(f"有 {len(missing_videos)} 个视频不在数据库中，无法创建项目：")
            for mv in missing_videos:
                print(mv)
            return
        # 6. 获取视频ID
        video_ids = ProjectService.get_video_ids_by_uids(video_names, session)
        # 7. 分批创建项目
        total_videos = len(video_ids)
        for i in range(0, total_videos, batch_size):
            batch_video_ids = video_ids[i:i + batch_size]
            project_name_with_batch = f"{project_name}-{i//batch_size + 1}"
            print(f"正在创建项目 {project_name_with_batch}...")
            try:
                ProjectService.create_project(
                    name=project_name_with_batch,
                    schema_id=schema_id,
                    video_ids=batch_video_ids,
                    session=session
                )
                print(f"项目 {project_name_with_batch} 创建成功!")
            except ValueError as e:
                if "already exists" in str(e):
                    print(f"项目 {project_name_with_batch} 已存在,跳过...")
                else:
                    raise e
    finally:
        session.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_path", type=str, required=True, help="标注json路径")
    parser.add_argument("--project_name", type=str, required=True, help="项目名称")
    parser.add_argument("--schema_name", type=str, required=True, help="schema名称")
    parser.add_argument("--batch_size", type=int, default=15, help="每个项目的视频数量")
    args = parser.parse_args()
    create_project_from_annotation_json(args.json_path, args.project_name, args.schema_name, args.batch_size)