# Bài tập lớn Cơ sở dữ liệu phân tán

## Mô tả
Dự án này triển khai các phương pháp phân mảnh dữ liệu trong cơ sở dữ liệu quan hệ, cụ thể là PostgreSQL. Chương trình tải dữ liệu đánh giá phim từ trang web MovieLens, phân mảnh theo các phương pháp khác nhau và quản lý việc chèn dữ liệu mới vào các phân mảnh phù hợp.

## Cài đặt

### Yêu cầu hệ thống
- Python 3.12.x
- PostgreSQL
- Thư viện psycopg2

### Các bước cài đặt
1. Clone repository này về máy:
   ```
   git clone <URL repository>
   ```

2. Cài đặt các thư viện cần thiết:
   ```
   pip install psycopg2
   ```

3. Cấu hình kết nối PostgreSQL:
   Mở file `db_connection.py` và chỉnh sửa các thông số kết nối (tên người dùng, mật khẩu, tên cơ sở dữ liệu...) phù hợp với môi trường của bạn.

4. Tải dataset MovieLens:
   ```
   http://files.grouplens.org/datasets/movielens/ml-10m.zip
   ```
   Giải nén và lưu file ratings.dat.

## Sử dụng
Dự án bao gồm các chức năng chính sau:

1. **LoadRatings(path_to_ratings)**
   - Tải dữ liệu từ file ratings.dat vào bảng Ratings

2. **Range_Partition(conn, n_partitions)**
   - Phân mảnh bảng Ratings thành n phân mảnh dựa trên khoảng giá trị của Rating

3. **RoundRobin_Partition(conn, n_partitions)**
   - Phân mảnh bảng Ratings thành n phân mảnh theo phương pháp Round-Robin

4. **Range_Insert(conn, user_id, movie_id, rating)**
   - Chèn dữ liệu mới vào bảng Ratings và vào đúng phân mảnh theo khoảng giá trị

5. **RoundRobin_Insert(conn, user_id, movie_id, rating)**
   - Chèn dữ liệu mới vào bảng Ratings và vào đúng phân mảnh theo phương pháp Round-Robin

### Ví dụ sử dụng:
```python
from loadRatings import loadRatings
from rangePartition import Range_Partition, Range_Insert
from roundRobinPartition import RoundRobin_Partition, RoundRobin_Insert
from db_connection import get_connection, fetch_all, createdb

# Kết nối đến cơ sở dữ liệu
conn = get_connection()

# Tải dữ liệu
loadRatings('/path/to/ratings.dat')

# Phân mảnh theo Range với 5 phân mảnh
Range_Partition(conn, 5)

# Phân mảnh theo Round-Robin với 3 phân mảnh
RoundRobin_Partition(conn, 3)
```

## Kiểm thử
Có thể chạy file `tester.py` để kiểm tra các chức năng:
```
python tester.py
```

Lưu ý: Hãy chỉnh sửa đường dẫn đến file ratings.dat trong `tester.py` trước khi chạy.

## Các lưu ý quan trọng
1. Số phân mảnh bắt đầu từ 0. Ví dụ khi có 3 phân mảnh, tên các bảng sẽ là range_part0, range_part1, range_part2
2. Không thay đổi tiền tố tên bảng phân mảnh
3. Không mã hóa cứng tên tệp đầu vào, tên cơ sở dữ liệu
4. Không đóng kết nối bên trong các hàm đã triển khai
