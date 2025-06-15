import sys
import os

from label_pizza.services import VideoService, QuestionService
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
from dotenv import load_dotenv
from tqdm import tqdm

from label_pizza.models import Base  

from label_pizza.services import AuthService
from label_pizza.db import SessionLocal


load_dotenv()

engine = create_engine(os.environ["DBURL"])


import pandas as pd

def view_all_videos():
    with SessionLocal() as session:
        # 使用 VideoService 获取所有视频
        videos_df = VideoService.get_all_videos(session)
        
        # 打印基本信息
        print(f"总共找到 {len(videos_df)} 个视频")
        
        # 保存到 CSV 文件
        videos_df.to_csv('all_videos.csv', index=False)
        print("数据已保存到 all_videos.csv")
        
        # 打印前几行数据
        print("\n前5行数据预览：")
        print(videos_df.head())
        
        return videos_df

def create_database_tables():
    # 创建数据库连接
    engine = create_engine(os.environ["DBURL"])
    
    try:
        # 创建所有表
        Base.metadata.create_all(engine)
        print("数据库表创建成功！")
    except Exception as e:
        print(f"创建数据库表时出错: {str(e)}")


def create_user(user_id: str, email: str, password: str, user_type: str):
    with SessionLocal() as session:
        try:
            AuthService.create_user(
                user_id=user_id,  # 用户ID，例如：'user123'
                email=email,      # 邮箱，例如：'user@example.com'
                password_hash=password,  # 密码
                user_type=user_type,  # 用户类型：'admin', 'reviewer', 'annotator'
                session=session
            )
            print(f"用户 {email} 创建成功！")
        except Exception as e:
            print(f"创建用户时出错: {str(e)}")


def update_or_add_videos():
    # 读取 JSON 文件
    with open('../new_video_metadata.json', 'r') as f:
        videos_data = json.load(f)
    
    # 创建数据库会话
    with Session(engine) as session:
        for video_data in tqdm(videos_data, desc="处理视频", unit="个"):
            try:
                # 首先检查视频是否已存在
                existing_video = VideoService.get_video_by_uid(video_data['video_uid'], session)

                if existing_video:
                    # 如果视频存在，更新它
                    print('hi')
                    VideoService.update_video(
                        video_uid=video_data['video_uid'],
                        new_url=video_data['url'],
                        new_metadata=video_data['metadata'],  # 可以添加或更新元数据
                        session=session
                    )
                    print(f"Successfully updated video: {video_data['video_uid']}")
                else:
                    # 如果视频不存在，添加新视频
                    VideoService.add_video(
                        url=video_data['url'],
                        session=session,
                        metadata=video_data['metadata']  # 可以添加元数据
                    )
                    print(f"Successfully added new video: {video_data['video_uid']}")
                    
            except Exception as e:
                print(f"Error processing video {video_data['video_uid']}: {str(e)}")
                session.rollback()
                continue
        
        # 提交所有更改
        try:
            session.commit()
            print("All videos have been successfully processed!")
        except Exception as e:
            print(f"Error committing changes: {str(e)}")
            session.rollback()

def view_all_questions():
    with SessionLocal() as session:
        try:
            # 获取所有问题
            questions_df = QuestionService.get_all_questions(session)
            
            # 打印问题总数
            print(f"\n总共有 {len(questions_df)} 个问题")
            
            # 打印列名，用于调试
            print("\n可用的列名:", questions_df.columns.tolist())
            
            # 打印每个问题的详细信息
            for _, row in questions_df.iterrows():
                print("\n" + "="*50)
                print(f"问题ID: {row['ID']}")
                print(f"问题文本: {row['Text']}")
                print(f"显示文本: {row['Display Text']}")
                print(f"问题类型: {row['Type']}")
                print(f"所属组: {row['Group']}")
                print(f"选项: {row['Options']}")
                print(f"默认选项: {row['Default']}")
                print(f"是否归档: {row['Archived']}")
                print("="*50)
            
            # # 保存到 CSV 文件
            # questions_df.to_csv('all_questions.csv', index=False)
            # print("\n问题数据已保存到 all_questions.csv")
            
        except Exception as e:
            print(f"获取问题失败: {str(e)}")
            # 打印更详细的错误信息
            import traceback
            print(traceback.format_exc())


import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from label_pizza.services import QuestionService
from label_pizza.db import SessionLocal

def check_question_types():
    with SessionLocal() as session:
        # 要检查的问题列表
        questions_to_check = [
            "special_lighting_effects_description",
            "volumetric_lighting_description",
            "dynamic_lighting_effects_description"
        ]
        
        print("\n检查问题类型:")
        print("-" * 50)
        
        for question_text in questions_to_check:
            try:
                # 获取数据库中的问题
                existing_question = QuestionService.get_question_by_text(question_text, session)
                print(f"\n问题: {question_text}")
                print(f"数据库中的类型: {existing_question.type}")
                print(f"数据库中的选项: {existing_question.options}")
                print(f"数据库中的默认值: {existing_question.default_option}")
                print(f"数据库中的显示文本: {existing_question.display_text}")
            except Exception as e:
                print(f"\n问题: {question_text}")
                print(f"获取问题失败: {str(e)}")
            print("-" * 50)


if __name__ == "__main__":
    update_or_add_videos()
    # update_or_add_videos()
    # view_all_videos()
    # create_user(
    #     user_id='syCen',
    #     email='siyuancen096@gmail.com',
    #     password='siyuan',
    #     user_type='admin'  # 'annotator' 或 'reviewer' 或 'admin'
    # )