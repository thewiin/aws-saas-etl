from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from models import db, User, Job

import boto3
import os
import io
import pandas as pd
from io import BytesIO, StringIO
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)

REGION = os.getenv("REGION_NAME", "us-east-1")
BUCKET_NAME = "etl-data-btl-2025"
UPLOAD_PREFIX = "uploads/"
UPDATE_PREFIX = "updates/"

app.config["SQLALCHEMY_DATABASE_URI"] = (
    "postgresql://postgres:postgres123@etl-postgresbtl-db.ccgxenjaxk1i.us-east-1.rds.amazonaws.com:5432/postgres"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)

with app.app_context():
    db.create_all()

s3_client = boto3.client("s3", region_name=REGION)

def read_csv_from_s3(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))

def save_csv_to_s3(df, bucket, key):
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )

def sentiment_rule(text):
    if pd.isna(text):
        return "negative"

    text = str(text).lower()

    positive_words = [
        "good", "great", "excellent", "amazing", "love",
        "awesome", "perfect", "nice", "satisfied", "happy", "delightful", "smooth","efficient","valuable",
        "friendly", "helpful","convenient","stable","fast","responsive","clear","simple","intuitive",
        "trustworthy","high-quality","effective","comfortable","polished","consistent","professional"
    ]

    negative_words = [
        "bad", "terrible", "awful", "hate", "poor",
        "slow", "worst", "disappointed", "problem",
        "confusing","unstable","crash","lag","freeze",
        "broken","misleading","frustrating","inconvenient",
        "complex","unclear","inaccurate","unreliable",
        "overpriced","waste", "glitch","timeout","disconnect",
        "incomplete","messy"
    ]

    pos = sum(word in text for word in positive_words)
    neg = sum(word in text for word in negative_words)

    return "positive" if pos >= neg else "negative"

@app.route("/")
def home():
    return render_template("index.html")

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    if not data.get("username") or not data.get("password"):
        return jsonify({"message": "Thiếu thông tin"}), 400

    if User.query.filter_by(username=data["username"]).first():
        return jsonify({"message": "Username đã tồn tại"}), 409

    user = User(
        username=data["username"],
        password=generate_password_hash(data["password"])
    )
    db.session.add(user)
    db.session.commit()
    return jsonify({"message": "Đăng ký thành công"}), 201

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.json
    user = User.query.filter_by(username=data.get("username")).first()
    if user and check_password_hash(user.password, data.get("password")):
        return jsonify({"user_id": user.id, "username": user.username})
    return jsonify({"message": "Sai tài khoản hoặc mật khẩu"}), 401

@app.route("/api/jobs/upload_url", methods=["POST"])
def get_upload_url():
    file_name = request.json.get("file_name")
    if not file_name:
        return jsonify({"error": "Thiếu file_name"}), 400

    file_key = f"{UPLOAD_PREFIX}{file_name}"
    url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": BUCKET_NAME, "Key": file_key, "ContentType": "text/csv"},
        ExpiresIn=3600
    )
    return jsonify({"url": url, "file_key": file_key})

@app.route("/api/jobs/start_etl", methods=["POST"])
def start_etl_job():
    file_key = request.json.get("file_key")
    if not file_key:
        return jsonify({"message": "Thiếu file_key"}), 400

    user = User.query.first()
    job = Job(
        filename=file_key,
        status="Processing",
        user_id=user.id,
        upload_time=datetime.utcnow()
    )
    db.session.add(job)
    db.session.commit()

    try:
        df = read_csv_from_s3(BUCKET_NAME, file_key)

        comment_col = None
        for col in df.columns:
            if col.lower() in ["comments", "comment", "review", "text"]:
                comment_col = col
                break

        if not comment_col:
            raise Exception("CSV không có cột comment")

        df["sentiment_result"] = df[comment_col].apply(sentiment_rule)

        output_key = f"{UPDATE_PREFIX}processed_{os.path.basename(file_key)}"
        save_csv_to_s3(df, BUCKET_NAME, output_key)

        job.status = "Completed"
        job.result_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{output_key}"
        db.session.commit()

        return jsonify({
            "message": "ETL xử lý thành công",
            "result_url": job.result_url
        })

    except Exception as e:
        job.status = "Failed"
        db.session.commit()
        return jsonify({"error": str(e)}), 500

@app.route("/api/jobs", methods=["GET"])
def get_jobs():
    jobs = Job.query.order_by(Job.upload_time.desc()).all()
    return jsonify([
        {
            "filename": j.filename,
            "status": j.status,
            "upload_time": j.upload_time.strftime("%Y-%m-%d %H:%M:%S"),
            "result_url": j.result_url
        } for j in jobs
    ])

@app.route('/api/jobs/download', methods=['POST'])
def download_result():
    s3_key = request.json.get("s3_key")
    url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": s3_key},
        ExpiresIn=300
    )
    return jsonify({"url": url})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
