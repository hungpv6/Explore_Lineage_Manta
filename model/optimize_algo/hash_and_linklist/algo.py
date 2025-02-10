import os
import pandas as pd
import os
import sys
from pathlib import Path
sys.path.append(str(Path(os.path.abspath(__file__)).parents[4]))
from logger_module import setup_logger_global
class Node:
    """Định nghĩa node trong linked list để xử lý va chạm."""
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.next = None

def setup_logger_global(name, log_file):
    """Fake setup_logger để tránh lỗi nếu chưa có."""
    import logging
    logger = logging.getLogger(name)
    logger.setLevel(logging.ERROR)
    return logger

class HashTable:
    def __init__(self, size):
        """
        Khởi tạo bảng băm với kích thước nhất định.
        - size: Kích thước của bảng hash (tùy thuộc vào dữ liệu)
        """
        self.table_size = size
        self.count = 0
        self.table = [None] * self.table_size  # Mảng chứa linked list
        self.binary_map = [0] * self.table_size  # Lưu trữ dạng nhị phân 0-1
        self.hash_table_logger = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')

    def _hash(self, key):
        """
        FNV-1a Hash Function để phân phối giá trị đồng đều.
        - key: Giá trị cần băm (string hoặc số)
        """
        try:
            FNV_OFFSET_BASIS = 14695981039346656037
            FNV_PRIME = 1099511628211
            hash_value = FNV_OFFSET_BASIS
            key_str = str(key).encode('utf-8')

            for byte in key_str:
                hash_value ^= byte
                hash_value *= FNV_PRIME
                hash_value &= 0xFFFFFFFFFFFFFFFF  # Giới hạn trong 64-bit

            return hash_value % self.table_size  # Chỉ số trong bảng băm
        except Exception as e:
            self.hash_table_logger.error(f"Error occurred in _hash function: {e}")

    def insert(self, key, value):
        """
        Chèn giá trị vào bảng hash (có xử lý va chạm bằng Linked List)
        """
        try:
            index = self._hash(key)
            new_node = Node(key, value)

            if self.table[index] is None:
                self.table[index] = new_node
                self.count += 1  # Tăng số lượng phần tử khi thêm mới
            else:
                current = self.table[index]
                while current:
                    if current.key == key:
                        current.value = value  # Cập nhật nếu key đã tồn tại
                        return  # Không tăng count nếu chỉ cập nhật
                    if current.next is None:
                        break
                    current = current.next
                current.next = new_node
                self.count += 1  # Chỉ tăng khi chèn một node mới

            self.binary_map[index] = 1  # Đánh dấu vị trí đã lưu bằng nhị phân 1
        except Exception as e:
            self.hash_table_logger.error(f"Error occurred in insert function: {e}")

    def get(self, key, default=None):
        """
        Lấy giá trị từ bảng hash dựa vào key, hỗ trợ giá trị mặc định.
        """
        try:
            index = self._hash(key)
            current = self.table[index]

            while current:
                if current.key == key:
                    return current.value
                current = current.next  # Duyệt tiếp trong Linked List

            return default  # Trả về giá trị mặc định nếu không tìm thấy
        except Exception as e:
            self.hash_table_logger.error(f"Error occurred in get function: {e}")

    def delete(self, key):
        """
        Xóa giá trị khỏi bảng hash
        """
        try:
            index = self._hash(key)
            current = self.table[index]
            prev = None

            while current:
                if current.key == key:
                    if prev is None:
                        self.table[index] = current.next  # Xóa node đầu tiên
                    else:
                        prev.next = current.next  # Bỏ qua node hiện tại

                    self.count -= 1  # Giảm số lượng phần tử khi xóa thành công

                    # Kiểm tra xem có còn node nào ở vị trí này không
                    if self.table[index] is None:
                        self.binary_map[index] = 0  # Cập nhật nhị phân về 0

                    return True  # Xóa thành công

                prev = current
                current = current.next

            return False  # Key không tồn tại
        except Exception as e:
            self.hash_table_logger.error(f"Error occurred in delete function: {e}")

    def __len__(self):
        """Trả về số lượng phần tử hiện có trong bảng hash."""
        return self.count

    def keys(self):
        """Trả về danh sách tất cả các key trong bảng hash."""
        all_keys = []
        for i in range(self.table_size):
            current = self.table[i]
            while current:
                all_keys.append(current.key)
                current = current.next
        return all_keys

    def values(self):
        """Trả về danh sách tất cả các giá trị trong bảng hash."""
        all_values = []
        for i in range(self.table_size):
            current = self.table[i]
            while current:
                all_values.append(current.value)
                current = current.next
        return all_values

    def items(self):
        """Trả về danh sách (key, value) trong bảng hash."""
        all_items = []
        for i in range(self.table_size):
            current = self.table[i]
            while current:
                all_items.append((current.key, current.value))
                current = current.next
        return all_items



class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, path):
        node = self.root
        for step in path:
            if step not in node.children:
                node.children[step] = TrieNode()
            node = node.children[step]
        node.is_end = True

    def is_subset(self, path):
        node = self.root
        for step in path:
            if step not in node.children:
                return False
            node = node.children[step]
        return node.is_end





    