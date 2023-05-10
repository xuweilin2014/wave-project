import os
import re
import threading
import traceback
from datetime import datetime, timedelta
from re import Pattern

from watcher.bricks import ObservedWatch


class FileContext:

    def __init__(self, absolute_path, file_type=None):
        # file 文件的绝对路径
        self._abs_path = absolute_path
        self.prev = None
        self.next = None
        # 文件的类型
        self.type = file_type

    def verify_file(self):
        return True

    def compare_key(self, key):
        raise NotImplemented("NotImplementedError: Please rewrite FileContext.compare_context()")

    @classmethod
    def new_file_context(cls, absolute_path):
        raise NotImplemented("NotImplementedError: Please rewrite FileContext.new_file_context()")

    @property
    def absolute_path(self):
        return self._abs_path

    @property
    def key(self):
        raise NotImplemented("NotImplementedError: Please rewrite FileContext.key()")


class FileContextMSD(FileContext):

    def __init__(self, absolute_path):
        super().__init__(absolute_path, file_type="msd")
        # msd 文件产生的时间戳
        self._time_stamp = None
        # 产生 msd 文件的设备编号
        self.section = None

    # 检验文件名称是否符合标准（可以提取出时间戳）、检验文件类型（必须为 MSD 文件）
    def verify_file(self):
        if self._abs_path is None or len(self._abs_path) == 0:
            return False

        ext = os.path.splitext(self._abs_path)[-1].replace(".", "")
        if ext.lower() != self.type:
            return False

        # 通过 split 函数获取文件的名称（不包含 / 或者 \\）
        filename = os.path.split(self._abs_path)[-1]
        if len(filename) == 0:
            return False
        # 通过 splitext 函数删除掉文件的扩展名称
        filename_noext = os.path.splitext(filename)[0]
        if len(filename_noext) == 0:
            return False

        # 标准格式有两种：
        # 1) TRG_103502_20230427_021000.msd
        # 2) TRG_103502_20230427_021000+0000.msd
        filename_noext = filename_noext if "+" not in filename_noext else filename_noext.split("+")[0]
        factors = filename_noext.split("_", maxsplit=2)
        if len(factors) < 3:
            return False

        try:
            # 将日期字符串转换成 datetime 类型，并且加上 8 个小时时差
            self._time_stamp = datetime.strptime(factors[2], "%Y%m%d_%H%M%S") + timedelta(hours=8)
            self.section = factors[1]
            return True
        except ValueError or TypeError:
            print(traceback.format_exc())
            return False

    def compare_key(self, other_key: datetime):
        if not isinstance(other_key, datetime):
            raise TypeError("the comparison type must be datetime")

        # 大于 other 的时间戳，返回 > 0
        # 小于 other 的时间戳，返回 < 0
        # 等于 other 的时间戳，返回 = 0
        return (self._time_stamp - other_key).total_seconds()

    @classmethod
    def new_file_context(cls, absolute_path):
        file_context = cls(absolute_path)
        # 校验文件的类型必须为 MSD，校验文件名称是否标准（可以提取时间戳）
        # 返回 False 则说明校验不通过
        if not file_context.verify_file():
            return None
        return file_context

    @property
    def time_stamp(self):
        return self._time_stamp

    @property
    def key(self):
        # 不同区域设备可能在相同时间产生 msd 文件，导致 key 相同，这不要紧，因为 FileContextMSD 在链表中插入时，
        # 只有碰到 timestamp 大于自己的节点，才会进行插入操作，相同 timestamp 的节点会继续向链表后方移动，查找
        # 可以插入的位置
        # 在 kv_store 中，只存储最开始的 timestamp -> node 结点映射，这样根据时间戳查询时，就不会遗漏结点
        return self._time_stamp.strftime('%Y-%m-%d %H:%M:%S')


class FileCache:

    def __init__(self, context_cls: FileContext.__class__, watch: ObservedWatch):
        self._context_cls = context_cls
        self._watch = watch

    def init_cache(self):
        # 用于初始构建文件缓存
        raise NotImplemented("NotImplementedError: Please rewrite FileCache._build_cache()")

    def clear_cache(self):
        # 用于清除文件缓存
        raise NotImplemented("NotImplementedError: Please rewrite FileCache._build_cache()")

    def insert(self, file_context):
        # 用于向文件缓存中插入元素
        raise NotImplemented("NotImplementedError: Please rewrite FileCache.insert()")

    def delete(self, file_context):
        # 从文件缓存中删除元素
        raise NotImplemented("NotImplementedError: Please rewrite FileCache.delete()")

    def replace(self, old_file_context, new_file_context):
        # 从文件缓存中替换旧元素为新元素
        raise NotImplemented("NotImplementedError: Please rewrite FileCache.replace()")

    def query_by_path(self, file_path=None, regex=False, re_pattern: Pattern = None):
        # 根据文件路径和正则表达式从文件缓存中查询
        raise NotImplemented("NotImplementedError: Please rewrite FileCache.query_by_path()")

    def query_by_key_range(self, from_key, to_key):
        # 根据子类自定义的 key 范围，从文件缓存中查找
        raise NotImplemented("NotImplementedError: Please rewrite FileCache.query_by_key_range()")


# 使用双向链表 + dict 实现文件在内存中的缓存
class FileCacheLinkedList(FileCache):

    def __init__(self, context_cls: FileContext.__class__, watch: ObservedWatch):
        super(FileCacheLinkedList, self).__init__(context_cls, watch)
        # 使用可重入锁来进行并发控制
        self._lock = threading.RLock()
        # kv_store 主要用来存储 K-V 缓存项，另外 V 为 Node 类型，也就是能通过 key 快速找到 Node 节点
        # 快速定位到该 Node 节点在双链表中的位置，而不用遍历双链表来找该 Node 节点
        self._kv_store = dict()
        # head 和 tail 均指向哑结点 dummy node
        # context_cls 表示 FileContext 的子类
        self._head = context_cls('', '')
        self._tail = context_cls('', '')
        self._head.next = self._tail
        self._tail.prev = self._head

    def init_cache(self):
        with self._lock:
            for root, _, filenames in os.walk(self._watch.path):
                for file_name in filenames:
                    # 拼接获取到文件的绝对路径
                    file_context = self._context_cls.new_file_context(os.path.join(root, file_name))
                    # 将此 file_context 结点保存到双向链表中（依据时间戳进行排序）
                    self.insert(file_context)

    def clear_cache(self):
        with self._lock:
            # 清空缓存，将缓存设置为只有 head，tail 两个结点的状态
            ptr = self._head.next
            while ptr is not self._tail:
                # 将 ptr 指向的 FileContext 结点从双向链表中删除，并且删除 _kv_store 中对应的 kv 映射
                self.delete(ptr)
                ptr = ptr.next

    def _kv_store_push_item(self, file_context):
        with self._lock:
            if file_context.key not in self._kv_store:
                self._kv_store[file_context.key] = file_context

    def _kv_store_pop_item(self, file_context):
        with self._lock:
            if file_context.key in self._kv_store:
                del self._kv_store[file_context.key]

    def insert(self, file_context):
        # 类型和参数合法性检查
        if file_context is None:
            raise ValueError("FileCache.insert: file_context should not be None.")
        if not isinstance(file_context, FileContext):
            raise TypeError(
                f"FileCache.insert: type of file_context should be FileContext, but is {type(file_context)}"
            )

        with self._lock:
            # 可能同时会有多个线程同时访问 FileCache 这个数据结构，因此需要加锁并发控制
            # 根据 file_context 中的 key 属性进行插入排序
            prev = self._head
            ptr = self._head.next
            while ptr is not self._tail:
                if file_context.compare_key(ptr.key) < 0:
                    break
                prev = ptr
                ptr = ptr.next

            file_context.prev = prev
            file_context.next = ptr
            prev.next = file_context
            ptr.prev = file_context

            # 将 KV 保存到 hash 表中，根据 key 可以快速在链表中找到 file_context 结点
            self._kv_store_push_item(file_context)

    def delete(self, file_context):
        # 类型和参数合法性检查
        if file_context is None:
            raise ValueError("FileCache.insert: file_context should not be None.")
        if not isinstance(file_context, FileContext):
            raise TypeError(f"FileCache.insert: type of file_context should be FileContext, but is {type(file_context)}")

        with self._lock:
            # 将 file_context 结点从双向链表中删除
            ptr = file_context
            ptr.prev.next = ptr.next
            ptr.next.prev = ptr.prev

            # for gc
            ptr.next = None
            ptr.prev = None

            # 将 key -> file_context 的映射从 kv 中删除
            self._kv_store_pop_item(file_context)

    def replace(self, old_file_context, new_file_context):
        # 类型和参数合法性检查
        if old_file_context is None or new_file_context is None:
            raise ValueError(f"FileCache.replace: args should not be None.")
        if not isinstance(old_file_context, FileContext) or not isinstance(new_file_context, FileContext):
            raise TypeError(f"FileCache.replace: args should be FileContext")

        # 在双向链表中，将 old_file_context 结点替换成 new_file_context
        with self._lock:
            ptr = self._head.next
            while ptr is not self._tail:
                # 找到 old_file_context
                if ptr == old_file_context:
                    # 更新 kv_store
                    self._kv_store_pop_item(old_file_context)
                    self._kv_store_push_item(new_file_context)

                    # 将旧结点替换成新结点，并删除旧结点
                    new_file_context.prev = ptr.prev
                    new_file_context.next = ptr.next
                    ptr.prev.next = new_file_context
                    ptr.next.prev = new_file_context

                    # for gc
                    ptr.next = None
                    ptr.prev = None

    def query_by_path(self, file_path=None, regex=False, re_pattern: Pattern = None) -> list:
        # 遍历双向链表，找到文件的绝对路径等于 file_path 的 Node 结点，如果没有找到则返回 None
        # 参数 file_path 必须为绝对路径，否则抛出异常
        if not os.path.isabs(file_path):
            raise ValueError(f"FileCache.query_by_path: arg file_path {file_path} is not absolute path.")
        if regex and re_pattern is None:
            raise ValueError(f"FileCache.query_by_path: arg re_pattern should not be None.")

        while self._lock:
            res = list()
            ptr = self._head.next
            while ptr is not self._tail:
                if regex:
                    # 如果开启了正则匹配，那么使用正则表达式来匹配文件的绝对路径
                    # windows 文件系统即使文件路径为 case insensitive，所以忽略大小写的区别
                    if re_pattern.match(ptr.absolute_path, re.IGNORECASE):
                        res.append(ptr)
                else:
                    # 如果没有开启正则匹配，那么使用完全匹配，路径必须完全相等
                    if ptr.absolute_path == file_path:
                        res.append(ptr)
                ptr = ptr.next

            return res

    def query_by_key_range(self, from_key, to_key=None) -> list:
        if from_key is None and to_key is None:
            raise ValueError("FileCacheLinkedList.query_by_key_range: from_key and to_key cannot be None at the same time.")

        # 到双向链表中查找 from_key <= Node <= to_key 的所有元素，包装在一个列表中返回，没有找到则返回空列表
        with self._lock:
            res = list()
            # 如果 from_key 为 None，则说明要查找 <= to_key 的所有元素
            if from_key is None:
                ptr = self._head.next
            # 如果 from_key 在 kv_store 中，那么就可以直接获取到 node 结点，开始遍历
            elif from_key in self._kv_store:
                ptr = self._kv_store[from_key]
            # 否则，需要从头开始遍历，找到 >= from_key 的 node 结点
            else:
                ptr = self._head.next
                while ptr is not self._tail:
                    if ptr.compare_key(from_key) >= 0:
                        break
                    ptr = ptr.next

            while ptr is not self._tail:
                if to_key is not None and ptr.compare_key(to_key) > 0:
                    break
                res.append(ptr)
                ptr = ptr.next

            return res

    def __str__(self):
        ptr = self._head.next
        info = list()
        while ptr is not self._tail:
            info.append(ptr.file_rpath + '\n')
            ptr = ptr.next
        return ''.join(info)

