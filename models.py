from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


# Bảng 1: User
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)  # Lưu password đã mã hóa
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    # Quan hệ: Một user có nhiều job
    jobs = db.relationship('Job', backref='owner', lazy=True)

    def __repr__(self):
        return f'<User {self.username}>'


# Bảng 2: Job (Lịch sử ETL)
class Job(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), default='Pending')
    result_url = db.Column(db.String(200))  # Link file kết quả trên S3
    upload_time = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)

    def __repr__(self):
        return f'<Job {self.filename} - {self.status}>'