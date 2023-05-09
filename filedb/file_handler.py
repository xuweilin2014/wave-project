import queue
import re

from file_cache import FileCacheManager, FileContext

from watcher.events import (
    FileEventHandlerAsync,
    FileSystemEvent,
    FileDeletedEvent,
    DirDeletedEvent,
    FileCreatedEvent,
    DirCreatedEvent,
    FileMovedEvent,
    DirMovedEvent
)


class MSDFileEventHandler(FileEventHandlerAsync):

    def __init__(self, cache_manager: FileCacheManager, context_cls: FileContext.__class__):
        # queue 的 maxsize 默认被设置为 0，表示队列长度为无穷大
        super(MSDFileEventHandler, self).__init__(queue.Queue())
        # 获取文件缓存
        self._cache_manager = cache_manager
        self._context_cls = context_cls
    
    def on_any_event(self, event):
        # 1.If optional args block is true and timeout is None (the default), block if necessary until a free slot is available.
        # 2.Otherwise (block is false), put an item on the queue if a free slot is immediately available, else raise the Full
        #   exception (timeout is ignored in that case)
        self._event_queue.put(event, block=True)

    def on_moved(self, event: FileSystemEvent):
        if not isinstance(event, FileMovedEvent) and not isinstance(event, DirMovedEvent):
            raise TypeError(f"MSDFileEventHandler.on_moved: event arg should be FileMovedEvent or "
                            f"DirMovedEvent object, but get {type(event)}")

        src_path = event.src_path
        dest_path = event.dest_path
        if not event.is_directory:
            generator = list(self._cache_manager.query_by_path(src_path))
            if len(list(generator)) > 0:
                old_fcont = self._context_cls.new_file_context(dest_path)
                for new_fcont in generator:
                    self._cache_manager.replace(old_fcont, new_fcont)

    def on_deleted(self, event: FileSystemEvent):
        if not isinstance(event, FileDeletedEvent) and not isinstance(event, DirDeletedEvent):
            raise TypeError(f"MSDFileEventHandler.on_deleted: event should be FileDeletedEvent or "
                            f"DirDeletedEvent object, but get {type(event)}")

        # event 中的 src_path 是文件/目录的绝对路径
        delete_path = event.src_path
        # 如为 DirDeletedEvent 事件，则目录下的各个文件也被删除，但是不会产生 FileDeletedEvent，因此必须手动移除
        if event.is_directory:
            # 查询以此目录路径开头的所有文件
            # query_by_path 返回符合路径要求的所有结点对象（通过生成器的方式）
            generator = self._cache_manager.query_by_path(regex=True, re_pattern=re.compile("^" + delete_path))

        # 如为 FileDeletedEvent 事件，直接删除
        else:
            # 查询和此被删除文件路径对应的文件节点（通过生成器的方式）
            generator = self._cache_manager.query_by_path(delete_path)

        for fcont in generator:
            if fcont is not None:
                # 将 src_path 对应的 FileContext 结点从双向链表中删除
                self._cache_manager.delete(fcont)

    def on_created(self, event: FileSystemEvent):
        if not isinstance(event, FileCreatedEvent) and not isinstance(event, DirCreatedEvent):
            raise TypeError(f"MSDFileEventHandler.on_created: event should be FileCreatedEvent or "
                            f"DirCreatedEvent object, but get {type(event)}")

        # event 中的 src_path 是文件/目录的绝对路径
        create_path = event.src_path
        # 只处理文件的新增事件，即 FileCreatedEvent
        if not event.is_directory:
            # 由于 windows 函数 ReadDirectoryChangeW 的限制，同一个文件可能会产生多个 FileCreatedEvent 事件
            # 因此需要先查询是否已添加到缓存中
            generator = self._cache_manager.query_by_path(create_path)
            # 如果缓存中没有
            if len(list(generator)) == 0:
                # 创建并且校验 file_context 对象
                fcont = self._context_cls.new_file_context(create_path)
                # 将此 file_context 结点保存到双向链表中（依据时间戳进行排序）
                self._cache_manager.insert(fcont)




