import queue
import re

from watcher.bricks import BaseThread
from watcher.events import (
    FileEventHandler,
    FileSystemEvent,
    FileDeletedEvent,
    DirDeletedEvent,
    FileCreatedEvent,
    DirCreatedEvent,
    FileMovedEvent,
    DirMovedEvent,
    EVENT_TYPE_MOVED,
    EVENT_TYPE_DELETED,
    EVENT_TYPE_CREATED
)


# FileEventHandler 的异步子类，Observer 监控线程只会将事件保存到 event_queue 中，然后直接返回，
# 监控线程不执行具体业务代码，防止监控线程阻塞或者业务逻辑耗时太长，消费者线程自己从 event_queue 中
# 获取事件对象，并进行业务处理
class FileEventHandlerAsync(FileEventHandler):

    def __init__(self, event_queue: queue.Queue):
        super(FileEventHandlerAsync, self).__init__()
        self._event_queue = event_queue

    def dispatch(self, event):
        # 监控线程只调用 on_any_event，将事件保存到任务队列中，然后直接返回
        self.on_any_event(event)

    def on_any_event(self, event):
        # 1.If optional args block is true and timeout is None (the default), block if necessary until a free slot is available.
        # 2.Otherwise (block is false), put an item on the queue if a free slot is immediately available, else raise the Full
        #   exception (timeout is ignored in that case)
        self._event_queue.put(event, block=True)


class FileCacheManager(BaseThread):

    def __init__(self, file_cache, event_queue, context_cls):
        super(FileCacheManager, self).__init__()
        # 获取到文件缓存，缓存的内部数据结构由具体子类实现
        self._file_cache = file_cache
        # 创建事件队列
        self._event_queue = event_queue
        # 创建查询队列
        self._task_queue = queue.Queue()
        self._context_cls = context_cls

    def _process_created_event(self, event: FileSystemEvent):
        if not isinstance(event, FileCreatedEvent) and not isinstance(event, DirCreatedEvent):
            raise TypeError(f"MSDFileEventHandler.on_created: event should be FileCreatedEvent or "
                            f"DirCreatedEvent object, but get {type(event)}")

        # event 中的 src_path 是文件/目录的绝对路径
        create_path = event.src_path
        # 只处理文件的新增事件，即 FileCreatedEvent
        if not event.is_directory:
            # 由于 windows 函数 ReadDirectoryChangeW 的限制，同一个文件可能会产生多个 FileCreatedEvent 事件
            # 因此需要先查询是否已添加到缓存中
            res = self._file_cache.query_by_path(create_path)
            # 如果缓存中没有
            if len(res) == 0:
                # 创建并且校验 file_context 对象
                fcont = self._context_cls.new_file_context(create_path)
                # 将此 file_context 结点保存到双向链表中（依据时间戳进行排序）
                self._file_cache.insert(fcont)

    def _process_deleted_event(self, event: FileSystemEvent):
        if not isinstance(event, FileDeletedEvent) and not isinstance(event, DirDeletedEvent):
            raise TypeError(f"MSDFileEventHandler.on_deleted: event should be FileDeletedEvent or "
                            f"DirDeletedEvent object, but get {type(event)}")

        delete_path = event.src_path
        # 如为 DirDeletedEvent 事件，则目录下的各个文件也被删除，但是不会产生 FileDeletedEvent，因此必须手动移除
        if event.is_directory:
            # 查询以此目录路径开头的所有文件
            # query_by_path 返回符合路径要求的所有结点对象（通过生成器的方式）
            res = self._file_cache.query_by_path(regex=True, re_pattern=re.compile("^" + delete_path))

        # 如为 FileDeletedEvent 事件，直接删除
        else:
            # 查询和此被删除文件路径对应的文件节点（通过生成器的方式）
            res = self._file_cache.query_by_path(delete_path)

        for fcont in res:
            if fcont is not None:
                # 将 src_path 对应的 FileContext 结点从双向链表中删除
                self._file_cache.delete(fcont)

    def _process_moved_event(self, event: FileSystemEvent):
        if not isinstance(event, FileMovedEvent) and not isinstance(event, DirMovedEvent):
            raise TypeError(f"MSDFileEventHandler.on_moved: event arg should be FileMovedEvent or "
                            f"DirMovedEvent object, but get {type(event)}")

        src_path = event.src_path
        dest_path = event.dest_path
        if not event.is_directory:
            # 根据原始 src_path 获取到旧的结点
            res = self._file_cache.query_by_path(src_path)
            # 如果查找到旧结点
            if len(res) > 0:
                new_fcont = self._context_cls.new_file_context(dest_path)
                for old_fcont in res:
                    # 将旧的结点替换成新的结点
                    self._file_cache.replace(old_fcont, new_fcont)

    def query_file_cache(self, *args, **kwargs):
        # todo 将用户线程的查询封装成一个查询请求，保存到任务队列 task_queue 中
        # todo 对这个查询请求生成一个 Future 对象返回给用户，来获取返回结果
        pass

    def run(self) -> None:
        # 当 Manager 线程被启动时，初始化文件缓存
        # 遍历被监控目录下的文件，建立双向链表，双向链表主要用来维护 Node 结点顺序
        self._file_cache.init_cache()

        caller_dict = {
            EVENT_TYPE_CREATED: self._process_created_event,
            EVENT_TYPE_MOVED: self._process_moved_event,
            EVENT_TYPE_DELETED: self._process_deleted_event,
        }
        # 如果线程被停止，那么退出 run 方法
        while self.should_keep_running():
            # 每次从 event_queue 中取出一个事件，然后处理文件事件
            event = self._event_queue.get(block=True)
            caller_dict[event.event_type](event)
            # todo 从 task_queue 中获取到查询任务，执行查询操作
            # todo 将查询的结果设置到 Future 对象中，如果查询过程中出现异常，也将异常设置到 Future 对象中

        # todo 线程退出之后，将 task_queue 中每一个查询任务对应的 Future 设置线程停止异常
        # todo 线程退出之后，将 event_queue 清空

        self._event_queue.clear()
        # 当 Manager 线程被关闭时，清除文件缓存
        self._file_cache.clear_cache()


