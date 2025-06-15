import json
import sys
import os
import hashlib
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from label_pizza.services import AuthService
from label_pizza.db import SessionLocal

def extract_emails_from_json(json_path):
    """
    从标注json文件中提取所有出现过的邮箱
    """
    with open(json_path, 'r') as f:
        data = json.load(f)
    emails = set()
    for item in data:
        for video_name, annotator_list in item.items():
            for annotator_dict in annotator_list:
                for email in annotator_dict.keys():
                    emails.add(email)
    return emails

def create_users_from_emails(json_path):
    emails = extract_emails_from_json(json_path)
    print(f"共发现 {len(emails)} 个邮箱：")
    for email in emails:
        print(email)
    with SessionLocal() as session:
        # 获取已存在的用户
        existing_users = AuthService.get_all_users(session)
        existing_emails = set(existing_users['Email'].tolist())
        for email in emails:
            if email in existing_emails:
                print(f"用户 {email} 已存在，跳过")
                continue
            user_id = email.split('@')[0]
            password = user_id  # 你可以自定义密码策略
            try:
                # 用sha256生成密码hash
                password_hash = password
                AuthService.create_user(
                    user_id=user_id,
                    email=email,
                    password_hash=password_hash,
                    user_type='human',
                    session=session
                )
                print(f"成功创建用户 {email}")
            except Exception as e:
                print(f"创建用户 {email} 失败：{e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_path", type=str, required=True, help="标注json路径")
    args = parser.parse_args()
    create_users_from_emails(args.json_path)