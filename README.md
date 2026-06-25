# AWS SaaS ETL Project

Đây là một ứng dụng Web SaaS (Software as a Service) cung cấp chức năng ETL (Extract, Transform, Load) xử lý dữ liệu tự động, được xây dựng bằng **Flask** (Python) và tích hợp mạnh mẽ với các dịch vụ của **AWS** (S3, Comprehend, RDS).

## Tổng quan kiến trúc
- **Backend:** Flask (Python) cung cấp RESTful APIs.
- **Cơ sở dữ liệu:** PostgreSQL (lưu trữ trên AWS RDS) quản lý người dùng và trạng thái của các tiến trình xử lý dữ liệu (Job).
- **Lưu trữ:** Amazon S3 được dùng để lưu trữ file dữ liệu đầu vào (Raw Data) và file kết quả (Processed Data).
- **AI/Xử lý ngôn ngữ tự nhiên:** Amazon Comprehend được dùng để phân tích cảm xúc (Sentiment Analysis) văn bản.

## Workflow hoạt động

1. **Xác thực & Ủy quyền (Authentication)**
   - Người dùng đăng ký tài khoản và đăng nhập qua các API (`/api/auth/register`, `/api/auth/login`).
   - Thông tin người dùng (User) được lưu trữ và quản lý trong cơ sở dữ liệu PostgreSQL.

2. **Upload dữ liệu (Tải lên S3)**
   - Người dùng yêu cầu tải file dữ liệu CSV lên. Hệ thống sẽ cấp một **Presigned URL** (`/api/jobs/upload_url`) của Amazon S3.
   - Nhờ Presigned URL, client có thể upload file trực tiếp lên S3 bucket (thư mục `uploads/`) một cách an toàn mà không cần qua server backend, giảm tải cho server.

3. **Tiến trình ETL (Xử lý dữ liệu)**
   - Người dùng kích hoạt API xử lý dữ liệu (`/api/jobs/start_etl`). Một `Job` mới được tạo trong database với trạng thái `Processing`.
   - **Extract (Trích xuất):** Đọc nội dung file CSV từ Amazon S3 vào bộ nhớ dưới dạng Pandas DataFrame.
   - **Transform (Biến đổi):** 
     - *Demo cơ bản (`app.py`):* Tính độ dài chuỗi của một cột dữ liệu.
     - *Nâng cao (`etl_core.py`):* Làm sạch dữ liệu (xóa các dòng rỗng). Tiếp đến, gọi dịch vụ AI **Amazon Comprehend** để thực hiện phân tích cảm xúc (Sentiment Analysis) trên cột chứa văn bản review. Hệ thống sẽ tự động phân loại cảm xúc thành POSITIVE, NEGATIVE, NEUTRAL, v.v.
   - **Load (Tải lên):** Lưu toàn bộ DataFrame đã xử lý thành định dạng CSV và đẩy ngược lên một thư mục đích trên S3 (ví dụ: `updates/data.csv` hoặc prefix `processed_`).
   
4. **Cập nhật & Truy xuất kết quả**
   - Sau khi file kết quả được tải lên S3 thành công, backend sẽ cập nhật trạng thái `Job` trong database thành `Completed` cùng với URL tải về của file kết quả (`result_url`).
   - Người dùng có thể xem lại lịch sử các Job và tải file đã được xử lý về.

## Cấu trúc CSDL (Models)
- `User`: Lưu trữ thông tin người dùng (tên đăng nhập, mật khẩu đã mã hóa).
- `Job`: Ghi nhận lịch sử các file đã upload và trạng thái xử lý (Pending, Processing, Completed), liên kết với `User` và lưu đường dẫn file kết quả trên S3.
