class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, path):
        """ Thêm nguyên đường đi vào Trie """
        node = self.root
        for elem in path:
            if elem not in node.children:
                node.children[elem] = TrieNode()
            node = node.children[elem]
        node.is_end = True

    def is_superset(self, path):
        """ Kiểm tra xem có đường đi nào lớn hơn path đã tồn tại không """
        node = self.root
        for elem in path:
            if elem not in node.children:
                return False  # Nếu có phần tử không khớp, chắc chắn không phải tập cha
            node = node.children[elem]
        return True  # Nếu đã có đường đi lớn hơn thì bỏ qua

