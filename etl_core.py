import json
import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# Sửa lỗi kết nối: Ưu tiên dùng quyền tự động từ LabInstanceProfile trên EC2
# Nếu không có Key trong môi trường, Boto3 sẽ tự dùng Role của máy
# Cách này giúp Boto3 tự động dùng Role của máy EC2
s3_client = boto3.client('s3', region_name='us-east-1')
comprehend_client = boto3.client('comprehend', region_name='us-east-1')


def process_data(file_key, bucket_raw, bucket_processed):
    print(f"--- Bắt đầu xử lý file: {file_key} ---")

    # 1. E (Extract): Tải file từ S3
    try:
        response = s3_client.get_object(
            Bucket=bucket_raw,
            Key=file_key
        )
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return False

    # 2. T (Transform): Xóa các dòng trống
    df.dropna(inplace=True)

    # 3. T (AI-Transform): Gọi Amazon Comprehend
    def get_sentiment(text):
        try:
            if pd.isna(text) or str(text).strip() == "":
                return "NEUTRAL"

            # Giới hạn 4900 ký tự theo quy định của AWS Comprehend
            response = comprehend_client.detect_sentiment(
                Text=str(text)[:4900],
                LanguageCode='en'
            )
            return response['Sentiment']
        except Exception as e:
            print(f"Lỗi AI: {e}")
            return "ERROR"

    print("Đang gọi AI để phân tích...")

    # CẬP NHẬT QUAN TRỌNG: Kiểm tra cột 'review' từ file data2.csv của bạn
    # Nếu có cột 'review', ta dùng nó để phân tích AI,
    # vì cột 'comments' của bạn chỉ chứa nhãn
    target_column = 'review' if 'review' in df.columns else 'comments'

    if target_column in df.columns:
        print(f"Dữ liệu phân tích lấy từ cột: {target_column}")
        df['sentiment_result'] = df[target_column].apply(get_sentiment)
    else:
        print("Không tìm thấy cột dữ liệu phù hợp để phân tích AI!")
        return False

    # 4. L (Load): Lưu file đã xử lý lên S3
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
        return True  # Trả về True để app.py cập nhật trạng thái 'Completed'
    except Exception as e:
        print(f"Lỗi lưu file: {e}")
        return False
