from __future__ import annotations

import os.path

EVENT_TYPE_MOVED = "moved"
EVENT_TYPE_DELETED = "deleted"
EVENT_TYPE_CREATED = "created"
EVENT_TYPE_MODIFIED = "modified"
EVENT_TYPE_CLOSED = "closed"
EVENT_TYPE_OPENED = "opened"


class FileSystemEvent:
    """
    All FileSystemEvent objects are required to be immutable and hence
    can be used as keys in dictionaries or be added to sets.
    """

    # The type of the event as a string.
    event_type = ""

    # True if event was emitted for a directory; False otherwise.
    is_directory = False

    # True if event was synthesized; False otherwise.
    # These are events that weren't actually broadcast by the OS, but are presumed to have happened based on other, actual events.
    # True 表示文件事件是由 OS 产生并由 ReadDirectoryChangeW 函数生成的，否则文件是由
    is_synthetic = False

    def __init__(self, src_path):
        self._src_path = src_path

    @property
    def src_path(self):
        """Source path of the file system object that triggered this event."""
        return self._src_path

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            f"<{type(self).__name__}: event_type={self.event_type}, "
            f"src_path={self.src_path!r}, is_directory={self.is_directory}>"
        )

    # Used for comparison of events.
    @property
    def key(self):
        return self.event_type, self.src_path, self.is_directory

    def __eq__(self, event):
        return self.key == event.key

    def __ne__(self, event):
        return self.key != event.key

    def __hash__(self):
        return hash(self.key)


class FileSystemMovedEvent(FileSystemEvent):
    """
    File system event representing any kind of file system movement.
    """

    event_type = EVENT_TYPE_MOVED

    def __init__(self, src_path, dest_path):
        super().__init__(src_path)
        self._dest_path = dest_path

    @property
    def dest_path(self):
        """The destination path of the move event."""
        return self._dest_path

    # Used for hashing this as an immutable object.
    @property
    def key(self):
        return self.event_type, self.src_path, self.dest_path, self.is_directory

    def __repr__(self):
        return (
            f"<{type(self).__name__}: src_path={self.src_path!r}, "
            f"dest_path={self.dest_path!r}, is_directory={self.is_directory}>"
        )


# File events.


class FileDeletedEvent(FileSystemEvent):
    """
    File system event representing file deletion on the file system.
    """

    event_type = EVENT_TYPE_DELETED


class FileModifiedEvent(FileSystemEvent):
    """
    File system event representing file modification on the file system.
    """

    event_type = EVENT_TYPE_MODIFIED


class FileCreatedEvent(FileSystemEvent):
    """
    File system event representing file creation on the file system.
    """

    event_type = EVENT_TYPE_CREATED


class FileMovedEvent(FileSystemMovedEvent):
    """
    File system event representing file movement on the file system.
    """


# Directory events.


class DirDeletedEvent(FileSystemEvent):
    """
    File system event representing directory deletion on the file system.
    """

    event_type = EVENT_TYPE_DELETED
    is_directory = True


class DirModifiedEvent(FileSystemEvent):
    """
    File system event representing directory modification on the file system.
    """

    event_type = EVENT_TYPE_MODIFIED
    is_directory = True


class DirCreatedEvent(FileSystemEvent):
    """
    File system event representing directory creation on the file system.
    """

    event_type = EVENT_TYPE_CREATED
    is_directory = True


class DirMovedEvent(FileSystemMovedEvent):
    """
    File system event representing directory movement on the file system.
    """

    is_directory = True


class FileSystemEventHandler:
    """
    Base file system event handler that you can override methods from.
    """

    def dispatch(self, event):
        """
        Dispatches events to the appropriate methods.
        """
        self.on_any_event(event)
        # 这段代码使用了字典推导式来进行函数调用
        # { EVENT_TYPE_CREATED: self.on_created, ... } 创建了一个字典，其中键为事件类型，值为相应的处理器方法。
        # 然后将这个字典用作查找表，以检索处理事件类型所需的适当方法。
        {
            EVENT_TYPE_CREATED: self.on_created,
            EVENT_TYPE_DELETED: self.on_deleted,
            EVENT_TYPE_MODIFIED: self.on_modified,
            EVENT_TYPE_MOVED: self.on_moved,
            EVENT_TYPE_CLOSED: self.on_closed,
            EVENT_TYPE_OPENED: self.on_opened,
        }[event.event_type](event)

    def on_any_event(self, event):
        """
        Catch-all event handler.
        """

    def on_moved(self, event):
        """
        Called when a file or a directory is moved or renamed.
        """

    def on_created(self, event):
        """
        Called when a file or directory is created.
        """

    def on_deleted(self, event):
        """
        Called when a file or directory is deleted.
        """

    def on_modified(self, event):
        """
        Called when a file or directory is modified.
        """

    def on_closed(self, event):
        """
        Called when a file opened for writing is closed.
        """

    def on_opened(self, event):
        """
        Called when a file is opened.
        """


def generate_sub_moved_events(src_dir_path, dest_dir_path):
    """Generates an event list of :class:`DirMovedEvent` and
    :class:`FileMovedEvent` objects for all the files and directories within
    the given moved directory that were moved along with the directory.

    :param src_dir_path:
        The source path of the moved directory.
    :param dest_dir_path:
        The destination path of the moved directory.
    :returns:
        An iterable of file system events of type :class:`DirMovedEvent` and
        :class:`FileMovedEvent`.
    """
    # os.walk() 是 Python 标准库中用于遍历目录树的函数，它返回一个三元组 (dirpath, dirnames, filenames)，其中：
    #   1).dirpath：当前目录的路径字符串。
    #   2).dirnames：当前目录下所有子目录名称的列表（不包含路径）。
    #   3).filenames：当前目录下所有非目录文件名称的列表（不包含路径）
    for root, directories, filenames in os.walk(dest_dir_path):
        # 每遍历一次，就将得到的所有目录生成 DirMovedEvent，将得到的所有文件生成 FileMovedEvent
        # 并且由于这些 DirMovedEvent 和 FileMovedEvent 都是我们自己生成的，而不是由 ReadDirectoryChangesW
        # 函数生成的，因此需要设置 is_synthetic 为 True
        for directory in directories:
            full_path = os.path.join(root, directory)
            renamed_path = (full_path.replace(dest_dir_path, src_dir_path) if src_dir_path else None)
            dir_moved_event = DirMovedEvent(renamed_path, full_path)
            dir_moved_event.is_synthetic = True
            yield dir_moved_event
        for filename in filenames:
            full_path = os.path.join(root, filename)
            renamed_path = (full_path.replace(dest_dir_path, src_dir_path) if src_dir_path else None)
            file_moved_event = FileMovedEvent(renamed_path, full_path)
            file_moved_event.is_synthetic = True
            yield file_moved_event


def generate_sub_created_events(src_dir_path):
    """
    Generates an event list of :class:`DirCreatedEvent` and
    :class:`FileCreatedEvent` objects for all the files and directories within
    the given moved directory that were moved along with the directory.

    :param src_dir_path:
        The source path of the created directory.
    :returns:
        An iterable of file system events of type :class:`DirCreatedEvent` and
        :class:`FileCreatedEvent`.
    """
    for root, directories, filenames in os.walk(src_dir_path):
        # 每遍历一次，就将得到的所有目录生成 DirCreatedEvent，将得到的所有文件生成 FileCreatedEvent
        # 并且由于这些 DirCreatedEvent 和 FileCreatedEvent 都是我们自己生成的，而不是由 ReadDirectoryChangesW
        # 函数生成的，因此需要设置 is_synthetic 为 True
        for directory in directories:
            dir_created_event = DirCreatedEvent(os.path.join(root, directory))
            dir_created_event.is_synthetic = True
            yield dir_created_event
        for filename in filenames:
            file_created_event = FileCreatedEvent(os.path.join(root, filename))
            file_created_event.is_synthetic = True
            yield file_created_event
