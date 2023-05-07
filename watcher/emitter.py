import os.path
import threading
import time

from .events import (
    DirCreatedEvent,
    DirDeletedEvent,
    DirModifiedEvent,
    DirMovedEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    generate_sub_created_events,
    generate_sub_moved_events,
)
from .bricks import BaseThread
from .directory_winapi import close_directory_handle, get_directory_handle, read_events

DEFAULT_EMITTER_TIMEOUT = 1  # in seconds.
WATCHDOG_TRAVERSE_MOVED_DIR_DELAY = 1  # seconds


# Observer classes
class EventEmitter(BaseThread):

    def __init__(self, event_queue, watch, timeout=DEFAULT_EMITTER_TIMEOUT):
        super().__init__()
        self._event_queue = event_queue
        self._watch = watch
        self._timeout = timeout

    @property
    def timeout(self):
        return self._timeout

    @property
    def watch(self):
        return self._watch

    def queue_event(self, event):
        # 将一个 event 和 watch（ObservedWatch）封装成一个元组，保存到 event_queue 中
        self._event_queue.put((event, self.watch))

    def queue_events(self, timeout):
        """
        Override this method to populate the event queue with events per interval period.
        """

    def run(self):
        # 当 EventEmitter 线程正在运行时，调用 queue_events 方法，根据 windows api 函数
        # ReadDirectoryChangesW 来获取监控路径下文件的变化情况，并封装成 event 事件保存到
        # event_queue 队列中
        # ReadDirectoryChangesW 同步调用时，会被阻塞，在每次监测到一次变化后，重新发起新的 ReadDirectoryChangesW 调用
        while self.should_keep_running():
            self.queue_events(self.timeout)


class WindowsApiEmitter(EventEmitter):
    """
    Windows API-based emitter that uses ReadDirectoryChangesW
    to detect file system changes for a watch.
    """

    def __init__(self, event_queue, watch, timeout=DEFAULT_EMITTER_TIMEOUT):
        super().__init__(event_queue, watch, timeout)
        self._lock = threading.Lock()
        self._handle = None

    def on_thread_start(self):
        # 调用 CreateFileW 获取被监控文件夹的句柄
        self._handle = get_directory_handle(self.watch.path)

    def start(self):
        # 获取被监控路径目录的句柄 handle，然后开启 run 方法，从 ReadDirectoryChangeW 中获取文件/目录的变化，
        # 生成事件对象，保存到 event_queue 中
        super().start()
        time.sleep(0.01)

    def on_thread_stop(self):
        # 关闭被监控路径目录的句柄 handle，设置停止事件 _stop_event
        if self._handle:
            close_directory_handle(self._handle)

    def _read_events(self):
        return read_events(self._handle, self.watch.path, self.watch.is_recursive)

    def queue_events(self, timeout):
        # 调用 _read_events 获取文件/目录变化事件对象 WinAPINativeEvent
        winapi_events = self._read_events()
        with self._lock:
            last_renamed_src_path = ""

            for winapi_event in winapi_events:
                # winapi_event.src_path 表示发生变化的文件相对于目录句柄的文件名
                src_path = os.path.join(self.watch.path, winapi_event.src_path)

                if winapi_event.is_renamed_old:
                    last_renamed_src_path = src_path
                elif winapi_event.is_renamed_new:
                    dest_path = src_path
                    src_path = last_renamed_src_path
                    if os.path.isdir(dest_path):
                        # 子目录移动事件
                        event = DirMovedEvent(src_path, dest_path)
                        if self.watch.is_recursive:
                            # HACK: We introduce a forced delay before traversing the moved directory.
                            # 阻塞等待一段时间，让子目录下的文件/目录 IO 完成（或者说移动完成）
                            time.sleep(WATCHDOG_TRAVERSE_MOVED_DIR_DELAY)
                            # The following block of code may not obtain moved events for the entire tree if
                            # the I/O is not completed within the above delay time. So, it's not guaranteed to work.
                            # 因为上面只产生了 DirMovedEvent 事件，没有产生子目录下的所有文件和目录移动事件，
                            # 因此为子目录（dest_path）下的所有文件和目录分别产生一个 DirMovedEvent/FileMovedEvent 事件
                            for sub_moved_event in generate_sub_moved_events(src_path, dest_path):
                                self.queue_event(sub_moved_event)
                        self.queue_event(event)
                    else:
                        self.queue_event(FileMovedEvent(src_path, dest_path))
                # 文件/目录的变更事件
                # DirModifiedEvent、FileModifiedEvent
                elif winapi_event.is_modified:
                    cls = (DirModifiedEvent if os.path.isdir(src_path) else FileModifiedEvent)
                    self.queue_event(cls(src_path))
                # 文件/目录的新增事件
                # DirCreatedEvent、FileCreatedEvent
                elif winapi_event.is_added:
                    isdir = os.path.isdir(src_path)
                    cls = DirCreatedEvent if isdir else FileCreatedEvent
                    self.queue_event(cls(src_path))
                    if isdir and self.watch.is_recursive:
                        # If a directory is moved from outside the watched folder to inside it,
                        # we only get a created directory event out of it, not any events for its children
                        # so use the same hack as for file moves to get the child events
                        # 阻塞等待一段时间，让子目录下的文件/目录 IO 完成
                        # 同时，也为子目录下的所有目录/文件都分别生成一个 DirCreatedEvent/FileCreatedEvent 事件
                        time.sleep(WATCHDOG_TRAVERSE_MOVED_DIR_DELAY)
                        sub_events = generate_sub_created_events(src_path)
                        for sub_created_event in sub_events:
                            self.queue_event(sub_created_event)
                # 文件/目录删除事件
                # FileDeletedEvent
                elif winapi_event.is_removed:
                    self.queue_event(FileDeletedEvent(src_path))
                # 被监控目录删除事件
                # DirDeletedEvent
                elif winapi_event.is_removed_self:
                    self.queue_event(DirDeletedEvent(self.watch.path))
                    self.stop()