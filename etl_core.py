import json

import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('REGION_NAME')
)
comprehend_client = boto3.client(
    'comprehend',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('REGION_NAME')
)


def process_data(file_key, bucket_raw, bucket_processed):
    print(f"--- Bắt đầu xử lý file: {file_key} ---")

    # 1. E (Extract): Tải file từ S3 [cite: 71]
    try:
        response = s3_client.get_object(Bucket=bucket_raw, Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return False

    # 2. T (Transform): Ví dụ xóa các dòng trống [cite: 72]
    df.dropna(inplace=True)

    # 3. T (AI-Transform): Gọi Amazon Comprehend [cite: 73]
    # Giả sử file CSV có cột 'comments' cần phân tích
    def get_sentiment(text):
        try:
            # Gọi API AWS
            response = comprehend_client.detect_sentiment(Text=text[:4900], LanguageCode='en')
            return response['Sentiment']  # POSITIVE, NEGATIVE, NEUTRAL...
        except Exception as e:
            return "ERROR"

    print("Đang gọi AI để phân tích...")
    # Tạo cột mới 'sentiment_result'
    if 'comments' in df.columns:
        df['sentiment_result'] = df['comments'].apply(get_sentiment)

    # 4. L (Load): Lưu file đã xử lý lên S3 [cite: 74]
    try:
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        processed_key = f"processed_{file_key}"

        s3_client.put_object(
            Bucket=bucket_processed,
            Key=processed_key,
            Body=csv_buffer.getvalue()
        )
        print(f"Đã lưu file kết quả: {processed_key}")
        return True
    except Exception as e:
        print(f"Lỗi lưu file: {e}")
        return False


def lambda_handler(event, context):
    file_key = event.get('file_key')

    bucket_raw = 'yourname-etl-raw-data'
    bucket_processed = 'yourname-etl-processed-data'

    print(f"Processing {file_key}...")

    return {
        'statusCode': 200,
        'body': json.dumps('ETL Process Completed!')
    }