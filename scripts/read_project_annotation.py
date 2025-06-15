import json
import os
from typing import Dict, List, Tuple, Any

import random
import string
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from label_pizza.services import VideoService, ProjectService, SchemaService, AnnotatorService, QuestionService
from dotenv import load_dotenv
from tqdm import tqdm

from label_pizza.models import Base  
from label_pizza.services import AuthService
from label_pizza.db import SessionLocal

import logging

def get_project_annotators_info(project_id: int):
    """
    获取项目中所有标注者的信息
    
    Args:
        project_id: 项目ID
    """
    with SessionLocal() as session:
        try:
            # 获取项目所有标注者信息
            annotators = ProjectService.get_project_annotators(project_id, session)
            
            # 打印标注者信息
            logging.info(f"Project {project_id} has {len(annotators)} annotators:")
            for user_id, info in annotators.items():
                logging.info(f"Annotator ID: {user_id}")
                logging.info(f"  - Email: {info['email']}")
                logging.info(f"  - User ID: {info['user_id']}")
                logging.info(f"  - Weight: {info.get('weight', 'N/A')}")
                logging.info("  ---")
                
            return annotators
            
        except Exception as e:
            logging.error(f"Error getting project annotators: {str(e)}")
            return None

# 使用示例
if __name__ == "__main__":
    project_id = 2  # 替换为你的项目ID
    annotators = get_project_annotators_info(project_id)