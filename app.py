from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

from models import db, User, Job
import boto3
import os
import json
import random
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

s3_client = boto3.client('s3', region_name=os.getenv('REGION_NAME'))
lambda_client = boto3.client('lambda', region_name=os.getenv('REGION_NAME'))

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:Nhaiben%401651652004@localhost:5432/etl_saas_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Cấu hình tên Bucket
RAW_BUCKET = 'yourname-etl-raw-data'

# Khởi tạo DB cùng với App
db.init_app(app)

# Tạo bảng tự động nếu chưa có
# with app.app_context():
#     db.create_all()

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'message': 'Vui lòng điền đủ thông tin!'}), 400

    if User.query.filter_by(username=username).first():
        return jsonify({'message': 'Tên đăng nhập đã tồn tại!'}), 409

    hashed_password = generate_password_hash(password)

    new_user = User(username=username, password=hashed_password)
    db.session.add(new_user)
    db.session.commit()

    return jsonify({'message': 'Tạo tài khoản thành công! Hãy đăng nhập.'}), 201


@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    user = User.query.filter_by(username=username).first()

    if user and check_password_hash(user.password, password):
        return jsonify({
            'message': 'Đăng nhập thành công',
            'user_id': user.id,
            'username': user.username
        }), 200
    else:
        return jsonify({'message': 'Sai tên đăng nhập hoặc mật khẩu'}), 401

# API 1: Lấy URL để upload file [cite: 65]
@app.route('/api/jobs/upload_url', methods=['POST'])
def get_presigned_url():
    file_name = request.json.get('file_name')

    # [REAL MODE] Nếu có AWS S3 thật:
    # try:
    #     url = s3_client.generate_presigned_url(...)
    #     return jsonify({'url': url, 'file_key': file_name})
    # except: ...

    # [DEMO MODE] Trả về thành công giả định để Frontend chạy tiếp
    return jsonify({
        'url': 'https://fake-s3-url.com/upload',
        'file_key': file_name
    })

# API 2: Kích hoạt ETL Job [cite: 66, 75]
@app.route('/api/jobs/start_etl', methods=['POST'])
def start_etl_job():
    data = request.json
    file_key = data.get('file_key')
    options = data.get('options', {})

    # Lấy user hiện tại (Demo thì lấy user đầu tiên trong DB)
    user = User.query.first()
    if not user:
        return jsonify({'message': 'Chưa có User nào trong DB!'}), 400

    # 1. Tạo Job mới trong Database với trạng thái 'Processing'
    new_job = Job(
        filename=file_key,
        status='Processing',
        user_id=user.id
    )
    db.session.add(new_job)
    db.session.commit()

    # [DEMO MODE] Giả lập AWS Lambda xử lý xong sau 1 giây
    # Trong thực tế: Lambda sẽ tự update DB, còn ở đây ta tự update luôn để demo
    try:
        # Giả vờ xử lý...
        status = 'Completed'
        # Nếu chọn AI, giả lập link kết quả có thêm hậu tố
        result_link = f"https://s3-bucket.aws.com/processed_{file_key}"

        # Cập nhật trạng thái Job thành công
        new_job.status = status
        new_job.result_url = result_link
        db.session.commit()

        return jsonify({
            'message': 'Job ETL & AI đã hoàn tất!',
            'job_id': new_job.id
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    jobs = Job.query.order_by(Job.upload_time.desc()).all()

    job_list = []
    for j in jobs:
        job_list.append({
            'id': j.id,
            'filename': j.filename,
            'status': j.status,
            'result_url': j.result_url,
            'upload_time': j.upload_time.strftime("%Y-%m-%d %H:%M:%S")
        })
    return jsonify(job_list)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)