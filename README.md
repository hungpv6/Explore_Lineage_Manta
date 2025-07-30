# Explore Lineage Manta

Explore Lineage Manta là bộ công cụ Python hỗ trợ xử lý, khai thác và tối ưu hóa dữ liệu truy vết dòng chảy (data lineage) trong hệ thống dữ liệu. Bộ công cụ cung cấp các hàm, class và thuật toán giúp xây dựng, mô hình hóa, và tối ưu hóa biểu đồ lineage một cách hiệu quả, đặc biệt phù hợp cho các tập dữ liệu lớn trong các hệ thống thực tế.

## Tính năng chính

- **Xây dựng và mô tả quan hệ lineage**: 
  - Tạo, mô tả, và phân tích các bảng nguồn và đích, xác định số lượng quan hệ, giá trị liên quan giữa các node trong biểu đồ dòng chảy.
- **Tối ưu hóa hiệu suất xử lý lineage**:
  - Sử dụng cấu trúc dữ liệu HashTable và Linked List tự cài đặt để tăng tốc độ truy vấn, lưu trữ và xây dựng danh sách kề của đồ thị.
- **Khai thác adjacency list tối ưu**:
  - Tạo danh sách kề (adjacency list) từ bảng băm, hỗ trợ tìm kiếm mối quan hệ giữa các node nhanh chóng.
- **Biến đổi, lọc và trực quan hóa dữ liệu lineage**:
  - Hỗ trợ chuyển đổi dataframe, lọc các đối tượng theo loại (Table, View, PLSQL), và chuẩn hóa dữ liệu phục vụ các mục đích phân tích khác nhau.
- **Ghi log chi tiết**:
  - Module logger riêng để theo dõi, ghi nhận lỗi và hiệu suất xử lý.

## Cấu trúc thư mục

- `model/lineage/lineage_exploit_data/`: Các class chính xử lý lineage, tối ưu hóa khai thác, xây dựng dictionary và adjacency list.
- `model/lineage/lineage_manta_deploy/`: Triển khai các class phục vụ transform, lọc và triển khai biểu đồ lineage.
- `model/optimize_algo/hash_and_linklist/`: Thuật toán, cấu trúc HashTable và LinkedList phục vụ tối ưu hiệu suất.
- `logger_module/`: Module ghi log hệ thống.
- `check.txt`: Ví dụ mã xử lý biến đổi dữ liệu.

## Yêu cầu

- Python 3.x
- pandas

## Ví dụ sử dụng

```python
from model.lineage.lineage_exploit_data.lineage_exploit import LineageMantaOptimize

# Khởi tạo object
lineage = LineageMantaOptimize()

# Tạo từ điển ánh xạ node path
dictionary = lineage.mapping_dict_nodepath_optimize(df, source_col="Source", target_col="Target")

# Tạo adjacency list tối ưu
adjacency_list = lineage.create_adjacency_list_optimized(dictionary)

# Mô tả bảng quan hệ optimized
table = lineage.describe_table_optimized(df, source_col="Source", target_col="Target")
```

## Đóng góp

Mọi đóng góp, ý kiến hoặc pull request đều được hoan nghênh!

## License

**Hiện tại chưa có license.**

---

Tác giả: [hungpv6](https://github.com/hungpv6)
