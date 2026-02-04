import boto3
import os
from dotenv import load_dotenv

load_dotenv()

iam = boto3.client(
    "iam",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

print("Listing IAM Users...")
try:
    paginator = iam.get_paginator('list_users')
    for page in paginator.paginate():
        for user in page['Users']:
            print(f"- Name: {user['UserName']}, ARN: {user['Arn']}, UserId: {user['UserId']}")
except Exception as e:
    print(f"Error listing IAM users: {e}")
