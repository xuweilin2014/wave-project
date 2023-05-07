from __future__ import annotations

import queue
import threading
import sys

from .bricks import BaseThread
from .bricks import ObservedWatch, EventQueue
from .emitter import WindowsApiEmitter

assert sys.platform.startswith("win"), f"{__name__} requires Windows"

DEFAULT_OBSERVER_TIMEOUT = 1  # in seconds.

"""
EventEmitter 和 Observer 是一个简单的生产者和消费者关系，EventEmitter 和 Observer 各为一个单独的线程，
EventEmitter 为生产者，循环监控路径下文件/目录的变化，生成事件对象放入到 event_queue 中，
Observer 为一个消费者，从 event_queue 中获取事件对象，分发给 event_handler 进行事件处理
"""


class EventDispatcher(BaseThread):
    """
    Consumer thread base class subclassed by event observer threads
    that dispatch events from an event queue to appropriate event handlers.
    """

    _stop_event = object()
    """Event inserted into the queue to signal a requested stop."""

    def __init__(self, timeout=DEFAULT_OBSERVER_TIMEOUT):
        super().__init__()
        # EventQueue 继承了 SkipRepeatsQueue，保证队列中的元素不重复
        self._event_queue = EventQueue()
        self._timeout = timeout

    @property
    def timeout(self):
        """Timeout value to construct emitters with."""
        return self._timeout

    def stop(self):
        # 将停止事件 _stopped_event 设置为 True，并且调用 on_thread_stop 方法，
        # 清控 BaseObserver 中 watch 和 emitter 的数据结构
        BaseThread.stop(self)
        try:
            self.event_queue.put_nowait(EventDispatcher._stop_event)
        except queue.Full:
            pass

    @property
    def event_queue(self):
        """
        The event queue which is populated with file system events
        by emitters and from which events are dispatched by a dispatcher
        thread.
        """
        return self._event_queue

    def dispatch_events(self, event_queue):
        """
        Override this method to consume events from an event queue, blocking
        on the queue for the specified timeout before raising :class:`queue.Empty`.
        """

    def run(self):
        # 如果 Observer 线程没有被停止的话（具体说就是没有收到 _stopped_event 事件）
        while self.should_keep_running():
            try:
                # 每次从 event_queue 中取出一个 event，然后将 event 分发给所有的 handler 进行处理
                self.dispatch_events(self.event_queue)
            except queue.Empty:
                continue


class BaseObserver(EventDispatcher):
    """Base observer."""

    def __init__(self, emitter_class, timeout=DEFAULT_OBSERVER_TIMEOUT):
        super().__init__(timeout)
        self._emitter_class = emitter_class
        self._lock = threading.RLock()
        # _watches 是一个 ObservedWatch 对象的集合
        self._watches = set()
        # _handlers 是一个 dict，{ObservedWatch -> set(EventHandler1, EventHandler2,....)}
        # 用户可能对一个监控路径注册了多个 event_handler 来处理文件变化事件
        self._handlers = dict()
        # _emitters 是 EventEmitter 对象的集合
        self._emitters = set()
        # _emitter_for_watch 是一个 dict，保存了 ObservedWatch -> EventEmitter 的映射
        # EventEmitter 循环监控文件变化等事件，由于一个 ObservedWatch 代表了一个监控路径，
        # 也就是说一个 ObservedWatch 只需要一个 EventEmitter 循环监控即可
        self._emitter_for_watch = dict()

    def _add_emitter(self, emitter):
        self._emitter_for_watch[emitter.watch] = emitter
        self._emitters.add(emitter)

    def _remove_emitter(self, emitter):
        # 将 emitter 从 _emitter_for_watch 以及 _emitters 两个数据结构中移除
        del self._emitter_for_watch[emitter.watch]
        self._emitters.remove(emitter)
        # 暂停 emitter 线程，并且调用 join 方法等待 emitter 线程暂停完毕
        emitter.stop()
        try:
            emitter.join()
        except RuntimeError:
            pass

    def _clear_emitters(self):
        for emitter in self._emitters:
            emitter.stop()
        for emitter in self._emitters:
            try:
                emitter.join()
            except RuntimeError:
                pass
        self._emitters.clear()
        self._emitter_for_watch.clear()

    def _add_handler_for_watch(self, event_handler, watch):
        # _handlers 是一个 dict 字典，{ObservedWatch -> set(EventHandler1, EventHandler2,....)}
        # 同一个 ObservedWatch，代表一个监听路径，可以注册多个 EventHandler，当事件被触发时，进行回调
        if watch not in self._handlers:
            self._handlers[watch] = set()
        self._handlers[watch].add(event_handler)

    @property
    def emitters(self):
        """Returns event emitter created by this observer."""
        return self._emitters

    def start(self):
        # 循环遍历 _emitters 集合中的每一个 emitter，开启 emitter 线程
        # emitter 线程会循环检测被监控路径下文件的变化，并将其保存到 event_queue 中
        for emitter in self._emitters.copy():
            try:
                emitter.start()
            except Exception:
                self._remove_emitter(emitter)
                raise
        # 开启 Observer 线程
        super().start()

    def schedule(self, event_handler, path, recursive=False):

        # 由于要把 EventEmitter 和 ObservedWatch 保存到 dict 和 set 这种数据结构中，因此需要加锁来进行并发控制
        with self._lock:
            # 创建一个 watch 对象，代表了一个监听路径
            watch = ObservedWatch(path, recursive)
            # 创建 watch 和 event_handler 之间的映射关系，一个 watch 可以对应多个 event_handler
            # 同理，一个 event_handler 可以用来处理多个 watch 上的事件
            self._add_handler_for_watch(event_handler, watch)

            # If we don't have an emitter for this watch already, create it.
            if self._emitter_for_watch.get(watch) is None:
                emitter = self._emitter_class(event_queue=self.event_queue, watch=watch, timeout=self.timeout)
                # 如果 Observer 线程仍然在运行，那么开启 EventEmitter 线程循环监控目录下文件的变化
                if self.is_alive():
                    emitter.start()
                self._add_emitter(emitter)
            self._watches.add(watch)
        return watch

    def add_handler_for_watch(self, event_handler, watch):
        with self._lock:
            self._add_handler_for_watch(event_handler, watch)

    def remove_handler_for_watch(self, event_handler, watch):
        with self._lock:
            self._handlers[watch].remove(event_handler)

    def _remove_handlers_for_watch(self, watch):
        del self._handlers[watch]

    def unschedule(self, watch):

        with self._lock:
            emitter = self._emitter_for_watch[watch]
            del self._handlers[watch]
            self._remove_emitter(emitter)
            self._watches.remove(watch)

    def unschedule_all(self):

        with self._lock:
            self._handlers.clear()
            # 暂停同时等待所有的 emitter 线程执行完毕，然后将 emitter 从 _emitters 和 _emitter_for_watch 中移除
            self._clear_emitters()
            self._watches.clear()

    def on_thread_stop(self):
        self.unschedule_all()

    def dispatch_events(self, event_queue):
        # 从 event_queue 中阻塞获取一个元素项
        entry = event_queue.get(block=True)
        # 如果元素项为 _stop_event，那么说明 Observer 线程被停止，因此直接返回，不处理
        if entry is EventDispatcher._stop_event:
            return
        event, watch = entry

        with self._lock:
            # To allow unschedule/stop and safe removal of event handlers, check if the handler is still registered after every dispatch.
            # 获取此 watch 对应的所有 handler，然后依次将 event 事件传递给 handler
            for handler in list(self._handlers.get(watch, [])):
                if handler in self._handlers.get(watch, []):
                    handler.dispatch(event)

        # Indicate that a formerly enqueued task is complete.
        #
        # Used by Queue consumer threads.  For each get() used to fetch a task, a subsequent call to task_done()
        # tells the queue that the processing on the task is complete.
        #
        # If a join() is currently blocking, it will resume when all items have been processed (meaning that a task_done()
        # call was received for every item that had been put() into the queue).
        event_queue.task_done()


# 真正返回给用户的 Observer，继承关系为 WindowsApiObserver -> BaseObserver -> EventDispatcher -> Thread.threading
class WindowsApiObserver(BaseObserver):
    """
    Observer thread that schedules watching directories and dispatches calls to event handlers.
    """

    def __init__(self, timeout=DEFAULT_OBSERVER_TIMEOUT):
        super().__init__(emitter_class=WindowsApiEmitter, timeout=timeout)
