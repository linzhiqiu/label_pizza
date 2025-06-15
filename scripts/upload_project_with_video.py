import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
from label_pizza.services import VideoService, ProjectService, SchemaService
from dotenv import load_dotenv
from tqdm import tqdm

from label_pizza.models import Base  
from label_pizza.services import AuthService
from label_pizza.db import SessionLocal

load_dotenv()

engine = create_engine(os.environ["DBURL"])

def upload_videos_and_create_project(json_file_path: str, project_name: str, schema_name: str, batch_size: int = 200):
    """
    从JSON文件创建项目,所有视频必须已存在于数据库中
    
    Args:
        json_file_path: JSON文件路径
        project_name: 项目名称
        schema_name: schema名称
        batch_size: 每个项目的视频数量
        
    Raises:
        ValueError: 如果存在新视频或schema不存在
    """
    # 读取JSON文件
    with open(json_file_path, 'r') as f:
        videos_data = json.load(f)
    
    # 创建数据库会话
    session = SessionLocal()
    SchemaService.unarchive_schema(17, session)
    try:
        # 获取schema ID
        try:
            schema_id = SchemaService.get_schema_id_by_name(schema_name, session)
            print(f"找到schema '{schema_name}', ID: {schema_id}")
        except ValueError as e:
            print(f"错误: {str(e)}")
            return
        
        # 获取所有视频UID
        video_uids = [video['video_uid'] for video in videos_data]

        # 一次性获取所有视频
        all_videos_df = VideoService.get_all_videos(session)
        existing_video_uids = set(all_videos_df['Video UID'])

        # 检查缺失的视频
        missing_videos = [uid for uid in video_uids if uid not in existing_video_uids]

        # 如果有缺失的视频,报错退出
        if missing_videos:
            error_msg = f"发现 {len(missing_videos)} 个视频不在数据库中:\n"
            raise ValueError(error_msg)
        
        # 获取所有视频ID
        video_ids = ProjectService.get_video_ids_by_uids(video_uids, session)
        
        # 按batch_size分组创建项目
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
    # 示例使用
    json_file_path = "../new_video_metadata.json"  # 替换为您的JSON文件路径
    project_base_name = "CamLight-test"  # 基础项目名称
    schema_name = "CameraLight"
    try:
        upload_videos_and_create_project(
            json_file_path=json_file_path,
            project_name=project_base_name,
            schema_name=schema_name,
            batch_size=50  # 每个项目200个视频
        )
    except ValueError as e:
        print(f"错误: {str(e)}")
        sys.exit(1)