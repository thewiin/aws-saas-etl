from flask import Flask
from models import db, User, Job

# Cấu hình App giả lập
app = Flask(__name__)

# --- CẤU HÌNH KẾT NỐI DATABASE ---
# 1. Để test nhanh (dùng SQLite – tạo file db.sqlite ngay tại đây):
# app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///local_test.db'

# 2. Để test với PostgreSQL (Nếu bạn cài Postgres trên máy hoặc kết nối tới AWS RDS):
app.config['SQLALCHEMY_DATABASE_URI'] = (
    'postgresql://postgres:postgres123@etl-postgresbtl-db.cggxenjaxkli.us-east-1.rds.amazonaws.com:5432/postgres'
)

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)


def run_demo():
    print("--- BẮT ĐẦU DEMO DATABASE ---")

    with app.app_context():
        # 1. Tạo bảng (Create Tables)
        print("1. Đang tạo bảng dữ liệu...")
        db.create_all()
        print("   -> Đã tạo xong bảng 'users' và 'jobs'.")

        # 2. Thêm User giả (Insert Data)
        print("2. Đang thêm User mẫu...")
        # Kiểm tra xem user tồn tại chưa để tránh lỗi trùng lặp khi chạy lại
        if not User.query.filter_by(username='demo_admin').first():
            new_user = User(
                username='demo_admin',
                password='hashed_secret_password'
            )
            db.session.add(new_user)
            db.session.commit()
            print("   -> Đã thêm User: demo_admin")
        else:
            print("   -> User 'demo_admin' đã tồn tại.")

        # 3. Giả lập tạo Job ETL (Insert Data)
        print("3. Đang tạo lịch sử Job ETL...")
        admin = User.query.filter_by(username='demo_admin').first()
        new_job = Job(
            filename='sales_data_2024.csv',
            status='Completed',
            result_url='s3://bucket/processed_sales_data_2024.csv',
            owner=admin
        )
        db.session.add(new_job)
        db.session.commit()
        print("   -> Đã lưu Job mới vào DB.")

        # 4. Truy vấn dữ liệu (Query Data)
        print("4. Kiểm tra dữ liệu đang có trong DB:")
        all_users = User.query.all()
        print(f"   - Danh sách Users: {all_users}")

        user_jobs = Job.query.filter_by(user_id=admin.id).all()
        print(f"   - Lịch sử Job của {admin.username}:")
        for job in user_jobs:
            print(
                f"     + File: {job.filename} | "
                f"Trạng thái: {job.status} | "
                f"Thời gian: {job.upload_time}"
            )

        print("\n--- KẾT THÚC DEMO: DATABASE HOẠT ĐỘNG TỐT! ---")


if __name__ == '__main__':
    run_demo()
