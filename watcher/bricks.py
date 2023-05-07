from __future__ import annotations

import queue
import sys
import threading
from typing import TYPE_CHECKING
from pathlib import Path


class ObservedWatch:

    def __init__(self, path, recursive):
        # ObservedWatch 的两个属性 _path 和 _is_recursive 都是只读的，因此为不可变对象
        # immutable and hashable，可以被用作 queue 的键对象
        if isinstance(path, Path):
            self._path = str(path)
        else:
            self._path = path
        self._is_recursive = recursive

    @property
    def path(self):
        # path 表示要监听的目录路径
        return self._path

    @property
    def is_recursive(self):
        # _is_recursive 表示是否监听 path 路径下的子目录内容变化
        return self._is_recursive

    @property
    def key(self):
        return self.path, self.is_recursive

    # 比较判断 ObservedWatch 是否相等，通过判断 path 和 is_recursive 变量
    def __eq__(self, watch):
        return self.key == watch.key

    # 比较判断 ObservedWatch 是否相等，通过判断 path 和 is_recursive 变量
    def __ne__(self, watch):
        return self.key != watch.key

    # 生成 ObservedWatch 的哈希值
    def __hash__(self):
        return hash(self.key)

    def __repr__(self):
        return f"<{type(self).__name__}: path={self.path!r}, is_recursive={self.is_recursive}>"


class SkipRepeatsQueue(queue.Queue):

    """
    Thread-safe implementation of a special queue where a put of the last-item put'd will be dropped.
    """

    def _init(self, maxsize):
        super()._init(maxsize)
        self._last_item = None

    # 重写了 Queue 类中的 _put 方法
    def _put(self, item):
        # 如果 item 与 _last_item 相等，则此 item 不会被添加到 queue 中，这样就保证了队列中的元素不会重复
        if self._last_item is None or item != self._last_item:
            super()._put(item)
            self._last_item = item
        else:
            # Queue#put 方法中，会调用我们重写的 _put 方法，将元素真正添加到队列中，然后在 Queue#put 方法中会将
            # unfinished_tasks 增加 1，所以必须要手动将 unfinished_tasks 减 1
            self.unfinished_tasks -= 1

    def _get(self):
        item = super()._get()
        if item is self._last_item:
            self._last_item = None
        return item


# Collection classes
class EventQueue(SkipRepeatsQueue):
    """
    Thread-safe event queue based on a special queue that skips adding
    the same event (:class:`FileSystemEvent`) multiple times consecutively.
    Thus avoiding dispatching multiple event handling
    calls when multiple identical events are produced quicker than an observer
    can consume them.
    """


class UnsupportedLibc(Exception):
    pass


class WatchdogShutdown(Exception):
    """
    Semantic exception used to signal an external shutdown event.
    """
    pass


class BaseThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        """
        Some threads do background tasks, like sending keepalive packets, or performing periodic garbage collection, 
        or whatever. These are only useful when the main program is running, and it's okay to kill them off once the other, 
        non-daemon, threads have exited.
        By setting them as daemon threads, you can let them run and forget about them, and when your program quits, 
        any daemon threads are killed automatically.
        """
        if hasattr(self, "daemon"):
            self.daemon = True
        else:
            self.setDaemon(True)
        # 一个事件对象管理一个内部标识，调用 set() 方法可将其设置为 true ，调用 clear() 方法可将其设置为 false ，
        # 调用 wait() 方法将进入阻塞直到标识为 true
        # 停止线程事件，为 True，表示线程停止运行；反之，线程正常运行
        self._stopped_event = threading.Event()

    @property
    def stopped_event(self):
        return self._stopped_event

    def should_keep_running(self):
        return not self._stopped_event.is_set()

    def on_thread_stop(self):
        # 可以继承 BaseThread，重写 on_thread_stop 方法，在线程关闭之前做一些清理工作
        pass

    def stop(self):
        # 将停止线程事件设置为 True，表示停止线程运行
        self._stopped_event.set()
        self.on_thread_stop()

    def on_thread_start(self):
        # 可以继承 BaseThread，重写 on_thread_start 方法，在线程启动之前做一些初始化工作
        pass

    def start(self):
        self.on_thread_start()
        # 使用类名调用实例方法，但此方式需要手动给 self 参数传值
        threading.Thread.start(self)


if TYPE_CHECKING or sys.version_info >= (3, 8):
    # using `as` to explicitly re-export this since this is a compatibility layer
    from typing import Protocol as Protocol
else:
    # Provide a dummy Protocol class when not available from stdlib.  Should be used
    # only for hinting.  This could be had from typing_protocol, but not worth adding
    # the _first_ dependency just for this.
    class Protocol:
        pass