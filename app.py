from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

from models import db, User, Job

import boto3
import os
import io
import pandas as pd
from dotenv import load_dotenv
from io import BytesIO

# =========================
# LOAD ENV
# =========================
load_dotenv()

app = Flask(__name__)
CORS(app)

# =========================
# AWS CONFIG
# =========================
REGION = os.getenv("REGION_NAME", "us-east-1")
BUCKET_NAME = "etl-data-btl-2025"
OUTPUT_KEY = "updates/data.csv"

s3_client = boto3.client("s3", region_name=REGION)

# =========================
# DATABASE CONFIG (POSTGRES RDS)
# =========================
app.config["SQLALCHEMY_DATABASE_URI"] = (
    "postgresql://postgres:postgres123@YOUR-RDS-ENDPOINT:5432/postgres"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

with app.app_context():
    db.create_all()

# =========================
# HELPER FUNCTIONS
# =========================
def read_csv_from_s3(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(BytesIO(obj["Body"].read()))
    return df


def save_csv_to_s3(df, bucket, key):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )

# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return render_template("index.html")

# =========================
# AUTH
# =========================
@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"message": "Vui lòng điền đủ thông tin"}), 400

    if User.query.filter_by(username=username).first():
        return jsonify({"message": "Tên đăng nhập đã tồn tại"}), 409

    user = User(
        username=username,
        password=generate_password_hash(password)
    )

    db.session.add(user)
    db.session.commit()

    return jsonify({"message": "Đăng ký thành công"}), 201


@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json
    user = User.query.filter_by(username=data.get("username")).first()

    if user and check_password_hash(user.password, data.get("password")):
        return jsonify({
            "message": "Đăng nhập thành công",
            "user_id": user.id,
            "username": user.username
        })

    return jsonify({"message": "Sai tài khoản hoặc mật khẩu"}), 401

# =========================
# UPLOAD FILE (PRESIGNED URL)
# =========================
@app.route("/api/jobs/upload_url", methods=["POST"])
def get_upload_url():
    data = request.get_json()

    if "file_name" not in data:
        return jsonify({"error": "Thiếu file_name"}), 400

    file_key = f"uploads/{data['file_name']}"

    url = s3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": file_key,
            "ContentType": "text/csv"
        },
        ExpiresIn=3600
    )

    return jsonify({
        "url": url,
        "file_key": file_key
    })

# =========================
# API 2: START ETL JOB
# =========================
@app.route("/api/jobs/start_etl", methods=["POST"])
def start_etl_job():
    try:
        data = request.get_json()
        if not data or "file_key" not in data:
            return jsonify({"message": "Thiếu file_key"}), 400

        file_key = data["file_key"]

        # lấy user demo
        user = User.query.first()
        if not user:
            return jsonify({"message": "Chưa có User nào trong DB"}), 400

        # 1. Tạo Job
        new_job = Job(
            filename=file_key,
            status="Processing",
            user_id=user.id
        )
        db.session.add(new_job)
        db.session.commit()

        # 2. Đọc CSV từ S3
        df = read_csv_from_s3(BUCKET_NAME, file_key)

        if "comments" not in df.columns:
            raise Exception("CSV thiếu cột 'comments'")

        # 3. Xử lý dữ liệu (DEMO)
        df["comment_length"] = df["comments"].astype(str).apply(len)

        # 4. Ghi kết quả lên S3
        save_csv_to_s3(df, BUCKET_NAME, OUTPUT_KEY)

        # 5. Update DB
        new_job.status = "Completed"
        new_job.result_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{OUTPUT_KEY}"
        db.session.commit()

        return jsonify({
            "message": "ETL xử lý thành công",
            "job_id": new_job.id,
            "result_url": new_job.result_url
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

# =========================
# GET ALL JOBS
# =========================
@app.route("/api/jobs", methods=["GET"])
def get_jobs():
    jobs = Job.query.order_by(Job.upload_time.desc()).all()
    result = []

    for j in jobs:
        result.append({
            "id": j.id,
            "filename": j.filename,
            "status": j.status,
            "result_url": j.result_url,
            "upload_time": j.upload_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    return jsonify(result)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
