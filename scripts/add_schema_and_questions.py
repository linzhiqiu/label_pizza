import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
from label_pizza.services import QuestionService, QuestionGroupService, SchemaService
from dotenv import load_dotenv
from tqdm import tqdm

import label_pizza
from label_pizza.models import Base
from label_pizza.db import SessionLocal
import pdb

print("label_pizza loaded from:", label_pizza.__file__)
print("label_pizza.service loaded from:", label_pizza.services.__file__)
print("sys.path:")


def print_object_attributes(obj, obj_name="Object"):
    """
    打印对象的所有属性和方法
    
    Args:
        obj: 要检查的对象
        obj_name: 对象的名称（用于打印）
    """
    print(f"\n{obj_name} 的所有属性:")
    print("-" * 50)
    
    # 打印所有属性
    print("属性:")
    for attr in dir(obj):
        if not attr.startswith('__'):  # 排除内置属性
            try:
                value = getattr(obj, attr)
                if not callable(value):  # 只打印非方法属性
                    print(f"{attr}: {value}")
            except Exception as e:
                print(f"{attr}: [无法获取值 - {str(e)}]")
    
    print("\n方法:")
    for attr in dir(obj):
        if not attr.startswith('__'):  # 排除内置方法
            try:
                value = getattr(obj, attr)
                if callable(value):  # 只打印方法
                    print(f"{attr}()")
            except Exception as e:
                print(f"{attr}(): [无法获取值 - {str(e)}]")
    
    print("-" * 50)

# # 在问题处理部分添加以下代码
#     try:
#         existing_question = QuestionService.get_question_by_text(question_data['text'], session)
#         print_object_attributes(existing_question, f"Question: {question_data['text']}")
#     except Exception as e:
#         print(f"获取问题失败: {str(e)}")


def import_all_schemas():
    # 读取 JSON 文件
    with open('../lighting_schema_questions.json', 'r') as f:
        data = json.load(f)
    
    with SessionLocal() as session:
        # 遍历每个 schema
        for schema_name, question_groups in tqdm(data.items(), desc="处理 Schemas"):
            print(f"\n处理 Schema: {schema_name}")
            
            # 检查 schema 是否存在
            try:
                existing_schema = SchemaService.get_schema_by_name(schema_name, session)
                print(f"找到已存在的 Schema: {schema_name}")
            except:
                existing_schema = None
                print(f"创建新的 Schema: {schema_name}")
            
            # 存储这个 schema 下所有 question group 的 ID
            schema_question_group_ids = []
            
            # 遍历每个 question group
            for group_data in tqdm(question_groups, desc="处理 Question Groups"):
                print(f"\n处理 Question Group: {group_data['title']}")
                
                # 检查 question group 是否存在
                try:
                    existing_group = QuestionGroupService.get_group_by_name(group_data['title'], session)
                    print(f"找到已存在的 Question Group: {group_data['title']}")
                except:
                    existing_group = None
                    print(f"创建新的 Question Group: {group_data['title']}")
                
                # 1. 创建或更新这个 group 下的所有问题
                question_ids = []
                for question_data in tqdm(group_data['questions'], desc="Processing questions"):
                    try:
                        # Check if the question exists
                        try:
                            import inspect
                            # print(inspect.getsource(QuestionService.get_question_by_text))
                            # print(inspect.getfile(QuestionService.get_question_by_text))
                            # # return
                            existing_question = QuestionService.get_question_by_text(question_data['text'], session)
                            # print(type(existing_question))
                            # return
                            question_exists = True
                        except:
                            existing_question = None
                            question_exists = False
                        
                        if question_exists:
                            print(f"Update existing question: {question_data['text']}")
                            # pdb.set_trace()
                            # raise
                            # Update question
                            if existing_question['type'] == 'description':
                                print('yes')
                                # For description type, only update display_text
                                QuestionService.edit_question(
                                    question_id=existing_question['id'],
                                    new_display_text=question_data['display_text'],
                                    new_opts=None,
                                    new_default=None,
                                    new_display_values=None,
                                    session=session
                                )
                            else:
                                print('no')
                                # 对于 single 类型，更新所有字段
                                QuestionService.edit_question(
                                    question_id=existing_question['id'],
                                    new_display_text=question_data['display_text'],
                                    new_opts=question_data['options'],
                                    new_default=question_data.get('default_option'),
                                    new_display_values=question_data['display_values'],
                                    session=session
                                )
                            question_ids.append(existing_question['id'])
                        else:
                            print(f"创建新问题: {question_data['text']}")
                            # 创建新问题
                            if question_data['qtype'] == 'single':
                                question = QuestionService.add_question(
                                    text=question_data['text'],
                                    qtype=question_data['qtype'],
                                    options=question_data['options'],
                                    display_values=question_data['display_values'],
                                    display_text=question_data['display_text'],
                                    default=question_data.get('default_option'),
                                    session=session
                                )
                            elif question_data['qtype'] == 'description':
                                question = QuestionService.add_question(
                                    text=question_data['text'],
                                    qtype=question_data['qtype'],
                                    options=[],
                                    default=None,
                                    display_values=[],
                                    display_text=question_data['display_text'],
                                    session=session
                                )
                            question_ids.append(question.id)
                    except Exception as e:
                        print(f"✗ 处理问题失败 {question_data['text']}: {str(e)}")
                
                # 2. 创建或更新问题组
                try:
                    if existing_group:
                        # 更新问题组
                        QuestionGroupService.edit_group(
                            group_id=existing_group.id,
                            new_title=group_data['title'],
                            new_description=group_data['description'],
                            is_reusable=group_data['is_reusable'],
                            verification_function="",
                            session=session
                        )
                        schema_question_group_ids.append(existing_group.id)
                        print(f"✓ 更新问题组: {group_data['title']}")
                    else:
                        # 创建新问题组
                        question_group = QuestionGroupService.create_group(
                            title=group_data['title'],
                            description=group_data['description'],
                            is_reusable=group_data['is_reusable'],
                            verification_function="",
                            question_ids=question_ids,
                            is_auto_submit=group_data['is_auto_submit'],
                            session=session
                        )
                        schema_question_group_ids.append(question_group.id)
                        print(f"✓ 创建问题组: {group_data['title']}")
                except Exception as e:
                    print(f"✗ 处理问题组失败: {str(e)}")
            
            # 3. 创建或更新 schema
            try:
                if existing_schema:
                    # 先归档旧的 schema
                    print(f"\n归档旧 Schema: {schema_name}")
                    SchemaService.archive_schema(existing_schema.id, session)
                
                # 创建新的 schema
                schema = SchemaService.create_schema(
                    name=schema_name,
                    question_group_ids=schema_question_group_ids,
                    session=session
                )
                print(f"\n✓ 创建 Schema: {schema.name}")
            except Exception as e:
                print(f"\n✗ 处理 Schema 失败: {str(e)}")


if __name__ == "__main__":
    import_all_schemas()