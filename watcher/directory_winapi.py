from __future__ import annotations

import os.path
import sys
from functools import reduce

# 使用 sys 判断当前使用的平台是否为 win32，因为需要使用 win32 中的 ReadDirectoryChangesW 函数
# 来判断被监控的文件夹下文件的变化
assert sys.platform.startswith("win"), f"{__name__} requires Windows"

import ctypes.wintypes

# ctypes.wintypes.LPVOID 是一个 ctypes 模块中定义的 Windows 数据类型，它表示一个指向无类型数据的指针（即一个空指针）
# LPVOID 实际上是一个类型别名，对应于 ctypes.c_void_p 类型，也就是一个 C 语言中的 void* 类型
LPVOID = ctypes.wintypes.LPVOID

# Invalid handle value.
# 在 CreateFileW 函数中，返回值如果为 INVALID_HANDLE_VALUE，说明函数调用出错，此时需要查看函数的错误码来确定具体原因
# INVALID_HANDLE_VALUE 是一个表示无效句柄的常量（宏定义），定义如下所示：
# #define INVALID_HANDLE_VALUE ((HANDLE)(LONG_PTR)-1)
# 其中，HANDLE 是一个指向句柄的指针类型，LONG_PTR 是一个长整型指针类型。这个宏定义会将 -1 转换为 LONG_PTR 类型，并将其强制转换为
# HANDLE 类型的指针，从而表示一个无效的句柄。
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

# File notification constants.
# 更改包括重命名、创建或删除文件
FILE_NOTIFY_CHANGE_FILE_NAME = 0x01
# 更改包括创建或删除目录
FILE_NOTIFY_CHANGE_DIR_NAME = 0x02
# 更改包括任何目录或者文件的属性变化
FILE_NOTIFY_CHANGE_ATTRIBUTES = 0x04
# 监视目录或子树中的任何文件大小更改都会导致更改通知等待操作返回
FILE_NOTIFY_CHANGE_SIZE = 0x08
# 对监视目录或子树中文件的最后写入时间的任何更改都会导致更改通知等待操作返回
FILE_NOTIFY_CHANGE_LAST_WRITE = 0x010
# 对监视目录或子树中文件的最后访问时间的任何更改都会导致更改通知等待操作返回
FILE_NOTIFY_CHANGE_LAST_ACCESS = 0x020
# 对监视目录或子树中文件创建时间的任何更改都会导致更改通知等待操作返回
FILE_NOTIFY_CHANGE_CREATION = 0x040
# 监视目录或子树中的任何安全描述符更改都会导致更改通知等待操作返回
FILE_NOTIFY_CHANGE_SECURITY = 0x0100

FILE_FLAG_BACKUP_SEMANTICS = 0x02000000

# Grants the right to read data from the file. For a directory, this value grants the right to
# list the contents of the directory.
# FILE_LIST_DIRECTORY 是一个 Windows 操作系统的文件访问权限，它允许用户或程序列出目录中的文件名。
FILE_LIST_DIRECTORY = 1

# 在 CreateFileW 中，有一个参数为 dwShareMode，指定打开文件的共享模式
# If this parameter is zero and CreateFile succeeds, the file or device cannot be shared and cannot be opened again until the
# handle to the file or device is closed. To enable a process to share a file or device while another process has the file or
# device open, use a compatible combination of one or more of the following values.
FILE_SHARE_READ = 0x01
FILE_SHARE_WRITE = 0x02

# FILE_SHARE_DELETE 是 Windows 平台上用于打开文件或目录的一个共享模式标志位，它指示系统允许其他进程删除该文件或目录。具体来说，当应用程序使用
# CreateFile() 函数以此标志方式打开文件或目录时，其他进程可以同时访问并删除该文件或目录（包括目录下的文件）。
FILE_SHARE_DELETE = 0x04

# 在 CreateFileW 中，有一个参数为 dwCreationDisposition，指定当要打开的文件存在或者不存在时要采取的措施
# OPEN_EXISTING: Opens a file or device, only if it exists.
# If the specified file or device does not exist, the function fails and the last-error code is set
# to ERROR_FILE_NOT_FOUND (2).
OPEN_EXISTING = 3

# 当我们要在 Windows 操作系统中获取文件的最终路径时，可以使用 GetFinalPathNameByHandleW 函数
# VOLUME_NAME_NT，对于驱动器卷，返回 NTFS 文件系统名称路径
VOLUME_NAME_NT = 0x02

# File action constants.
# ReadDirectoryChangesW 使用 _FILE_NOTIFY_INFORMATION 结构体来描述文件系统的变化
# _FILE_NOTIFY_INFORMATION 结构体中使用 Action 字段来描述发生了哪种类型的变化，
FILE_ACTION_CREATED = 1
FILE_ACTION_DELETED = 2
FILE_ACTION_MODIFIED = 3
FILE_ACTION_RENAMED_OLD_NAME = 4
FILE_ACTION_RENAMED_NEW_NAME = 5
FILE_ACTION_DELETED_SELF = 0xFFFE

# Aliases
FILE_ACTION_ADDED = FILE_ACTION_CREATED
FILE_ACTION_REMOVED = FILE_ACTION_DELETED
FILE_ACTION_REMOVED_SELF = FILE_ACTION_DELETED_SELF

# Error codes
ERROR_OPERATION_ABORTED = 995


class OVERLAPPED(ctypes.Structure):
    _fields_ = [
        ("Internal", LPVOID),
        ("InternalHigh", LPVOID),
        ("Offset", ctypes.wintypes.DWORD),
        ("OffsetHigh", ctypes.wintypes.DWORD),
        ("Pointer", LPVOID),
        ("hEvent", ctypes.wintypes.HANDLE),
    ]


# 只要 ReadDirectoryChangesW 调用结束，_errcheck_bool 就会被调用
# 如果 _errcheck_bool 返回的 args 和传入其中的 args 参数没有发生变化，那么最后调用 ReadDirectoryChangesW 函数的结果为常规值，
# 即如果函数成功，则返回值为非零，否则返回零。
# 如果 _errcheck_bool 返回的 args 发生了变化，那么调用 ReadDirectoryChangesW 会把 _errcheck_bool 返回的 args 作为最终的结果
def _errcheck_bool(value, func, args):
    if not value:
        # WinError 函数会根据 GetLastError 返回的错误封装成一个 OSError python 错误并抛出
        raise ctypes.WinError()
    return args


def _errcheck_handle(value, func, args):
    if not value:
        raise ctypes.WinError()
    if value == INVALID_HANDLE_VALUE:
        raise ctypes.WinError()
    return args


def _errcheck_dword(value, func, args):
    if value == 0xFFFFFFFF:
        raise ctypes.WinError()
    return args


kernel32 = ctypes.WinDLL("kernel32")

"""
ctypes._FuncPtr 可以通过为其特殊属性（restype、argtypes、errcheck）赋值来自定义其行为
restype：赋值为一个 ctypes 类型来指定外部函数的结果类型。 使用 None 表示 void，即不返回任何结果的函数
argtypes：赋值为一个 ctypes 类型的元组来指定函数所接受的参数类型，当外部函数被调用时，每个实际参数都会被传
         给 argtypes 元组中条目的 from_param() 类方法，此方法允许将实际参数适配为此外部函数所接受的对象。
errcheck：将一个 Python 函数或其他可调用对象赋值给此属性。 该可调用对象将附带三个及以上的参数被调用
          callable(result, func, arguments)
              - result 是外部函数返回的结果，由 restype 属性指明。
              - func 是外部函数对象本身，这样就允许重新使用相同的可调用对象来对多个函数进行检查或后续处理。
              - arguments 是一个包含最初传递给函数调用的形参的元组，这样就允许对所用参数的行为进行特别处理。   
"""

ReadDirectoryChangesW = kernel32.ReadDirectoryChangesW
# BOOL = ctypes.c_long，其中，ctypes.c_long 表示 C 语言中的 long 类型，是一个有符号的长整型数据类型
# ReadDirectoryChangesW 如果函数成功，则返回值为非零。对于同步调用，这意味着操作成功，如果函数失败，则返回值为零。
# 要获取扩展的错误信息，可以调用 GetLastError
ReadDirectoryChangesW.restype = ctypes.wintypes.BOOL
# 将 _errcheck_bool 这个函数对象赋值给 errcheck 属性，errcheck 所指明的异常处理函数都会被调用，不管 win api
# 调用过程中是否出现异常，类似于 aop，对原函数进行了增强
ReadDirectoryChangesW.errcheck = _errcheck_bool
ReadDirectoryChangesW.argtypes = (
    ctypes.wintypes.HANDLE,  # hDirectory
    LPVOID,  # lpBuffer
    ctypes.wintypes.DWORD,  # nBufferLength
    ctypes.wintypes.BOOL,  # bWatchSubtree
    ctypes.wintypes.DWORD,  # dwNotifyFilter
    ctypes.POINTER(ctypes.wintypes.DWORD),  # lpBytesReturned
    ctypes.POINTER(OVERLAPPED),  # lpOverlapped
    LPVOID,  # FileIOCompletionRoutine # lpCompletionRoutine
)

# CreateFileW 函数调用成功的话，返回的为指向特定文件、设备、命名管道的句柄；
# 反之，调用失败的话，返回值为 INVALID_HANDLE_VALUE
CreateFileW = kernel32.CreateFileW
CreateFileW.restype = ctypes.wintypes.HANDLE
CreateFileW.errcheck = _errcheck_handle
# LPCWSTR 是一个 Python ctypes 库中的 Windows 平台数据类型，表示 Unicode 或宽字符字符串的指针，意思是指向常量宽字符串的长指针
# 在 Windows 编程中，许多函数的参数需要接受 Unicode 字符串。如果正在使用 Python 调用这些 API 函数，那么需要使用 ctypes
# 库来创建 Unicode 字符串并将其转换为 LPCWSTR 指针格式才能传递给这些函数。
# LPCWSTR = LPWSTR = ctypes.c_wchar_p
# ctypes.c_wchar_p 是 ctypes 库提供的一种数据类型，在 C 语言中，它通常表示为 wchar_t* 类型，
# wchar_t 是一种 C 语言数据类型，它是宽字符类型，用于表示一个 Unicode 字符或多个 Unicode 字符组成的字符串
CreateFileW.argtypes = (
    ctypes.wintypes.LPCWSTR,  # lpFileName
    ctypes.wintypes.DWORD,  # dwDesiredAccess
    ctypes.wintypes.DWORD,  # dwShareMode
    LPVOID,  # lpSecurityAttributes
    ctypes.wintypes.DWORD,  # dwCreationDisposition
    ctypes.wintypes.DWORD,  # dwFlagsAndAttributes
    ctypes.wintypes.HANDLE,  # hTemplateFile
)

GetFinalPathNameByHandleW = kernel32.GetFinalPathNameByHandleW
GetFinalPathNameByHandleW.restype = ctypes.wintypes.DWORD
GetFinalPathNameByHandleW.errcheck = _errcheck_dword
GetFinalPathNameByHandleW.argtypes = (
    ctypes.wintypes.HANDLE,  # hFile
    ctypes.wintypes.LPWSTR,  # lpszFilePath
    ctypes.wintypes.DWORD,  # cchFilePath
    ctypes.wintypes.DWORD,  # DWORD
)

CancelIoEx = kernel32.CancelIoEx
CancelIoEx.restype = ctypes.wintypes.BOOL
CancelIoEx.errcheck = _errcheck_bool
CancelIoEx.argtypes = (
    ctypes.wintypes.HANDLE,  # hObject
    ctypes.POINTER(OVERLAPPED),  # lpOverlapped
)

CloseHandle = kernel32.CloseHandle
CloseHandle.restype = ctypes.wintypes.BOOL
CloseHandle.argtypes = (ctypes.wintypes.HANDLE,)  # hObject


# ctypes.Structure 是 Python 中 ctypes 模块提供的一种数据类型，用于表示 C 结构体（struct）
# NextEntryOffset：The number of bytes that must be skipped to get to the next record. A value of zero indicates
#                  that this is the last record
# Action：The type of change that has occurred.
# FileNameLength：The size of the file name portion of the record, in bytes
# FileName：ctypes.c_char 是 ctypes 库中的一种数据类型，代表一个 C 语言中的 char 类型，而 ctypes.c_char * 1 表示创建一个长度为 1
#           的数组，创建数组类型的推荐方式是使用一个类型乘以一个正数，FileName 表示相对于目录句柄的文件名的可变长度字段
class FILE_NOTIFY_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("NextEntryOffset", ctypes.wintypes.DWORD),
        ("Action", ctypes.wintypes.DWORD),
        ("FileNameLength", ctypes.wintypes.DWORD),
        ("FileName", (ctypes.c_char * 1)),
    ]


LPFNI = ctypes.POINTER(FILE_NOTIFY_INFORMATION)

# We don't need to recalculate these flags every time a call is made to the win32 API functions.
WATCHDOG_FILE_FLAGS = FILE_FLAG_BACKUP_SEMANTICS
# 将 FILE_SHARE_READ、FILE_SHARE_WRITE 和 FILE_SHARE_DELETE 通过 | 拼接到一起
WATCHDOG_FILE_SHARE_FLAGS = reduce(
    lambda x, y: x | y,
    [
        FILE_SHARE_READ,
        FILE_SHARE_WRITE,
        FILE_SHARE_DELETE,
    ],
)
# 将下列所有的 FILE_NOTIFY_CHANGE_XXX 通过 | 拼接到一起
WATCHDOG_FILE_NOTIFY_FLAGS = reduce(
    lambda x, y: x | y,
    [
        FILE_NOTIFY_CHANGE_FILE_NAME,
        FILE_NOTIFY_CHANGE_DIR_NAME,
        FILE_NOTIFY_CHANGE_ATTRIBUTES,
        FILE_NOTIFY_CHANGE_SIZE,
        FILE_NOTIFY_CHANGE_LAST_WRITE,
        FILE_NOTIFY_CHANGE_SECURITY,
        FILE_NOTIFY_CHANGE_LAST_ACCESS,
        FILE_NOTIFY_CHANGE_CREATION,
    ],
)

# ReadDirectoryChangesW buffer length.
# To handle cases with lots of changes, this seems the highest safest value we can use.
# Note: it will fail with ERROR_INVALID_PARAMETER when it is greater than 64 KB and the application is monitoring a
# directory over the network.
BUFFER_SIZE = 64000

# Buffer length for path-related stuff.
PATH_BUFFER_SIZE = 2048


def _parse_event_buffer(readBuffer, nBytes):
    results = []
    # readBuffer 指向结果缓冲区，缓冲区中包含多个 FILE_NOTIFY_INFORMATION 结构体
    # nbytes 表示缓冲区中实际存储的字节数
    while nBytes > 0:
        # 将 readBuffer 指针转换为 LPFNI 指针，并且获取 readBuffer 中第一个 FILE_NOTIFY_INFORMATION 结构体
        fni = ctypes.cast(readBuffer, LPFNI)[0]
        # 在 ctypes.Structure 中，field 的 offset 表示该字段在结构体中的偏移量。偏移量指的是该字段相对于结构体开头的字节数
        # ptr 指向结构体中的字符串
        ptr = ctypes.addressof(fni) + FILE_NOTIFY_INFORMATION.FileName.offset
        filename = ctypes.string_at(ptr, fni.FileNameLength)
        results.append((fni.Action, filename.decode("utf-16")))
        numToSkip = fni.NextEntryOffset
        if numToSkip <= 0:
            break
        readBuffer = readBuffer[numToSkip:]
        nBytes -= numToSkip  # numToSkip is long. nBytes should be long too.
    return results


# 如果文件夹被删除，那么通过 GetFinalPathNameByHandleW 函数根据其句柄 handle 获取到的实际路径与被监控的路径会不一致
def _is_observed_path_deleted(handle, path):
    # Comparison of observed path and actual path, returned by
    # GetFinalPathNameByHandleW. If directory moved to the trash bin, or
    # deleted, actual path will not be equal to observed path.
    buff = ctypes.create_unicode_buffer(PATH_BUFFER_SIZE)
    GetFinalPathNameByHandleW(handle, buff, PATH_BUFFER_SIZE, VOLUME_NAME_NT)
    return buff.value != path


# 创建一个 FILE_NOTIFY_INFORMATION 结构体，其中的事件为 FILE_ACTION_DELETED_SELF（自定义）
# 然后将此结构体保存到 buff 缓冲区中并返回
def _generate_observed_path_deleted_event():
    path = ctypes.create_unicode_buffer(".")
    event = FILE_NOTIFY_INFORMATION(
        0, FILE_ACTION_DELETED_SELF, len(path), path.value.encode("utf-8")
    )
    event_size = ctypes.sizeof(event)
    buff = ctypes.create_string_buffer(PATH_BUFFER_SIZE)
    # ctypes.memmove(dst, src, count)，将 dst 是目标内存区域的起始地址，src 是源内存区域的起始地址，count 是要移动的字节数
    ctypes.memmove(buff, ctypes.addressof(event), event_size)
    return buff, event_size


# 返回指向一个特定文件夹路径的句柄
def get_directory_handle(path):
    return CreateFileW(
        path,
        FILE_LIST_DIRECTORY,
        WATCHDOG_FILE_SHARE_FLAGS,
        None,
        OPEN_EXISTING,
        WATCHDOG_FILE_FLAGS,
        None,
    )


def close_directory_handle(handle):
    try:
        # force ReadDirectoryChangesW to return
        CancelIoEx(handle, None)
        # close directory handle
        CloseHandle(handle)
    except OSError:
        try:
            # close directory handle
            CloseHandle(handle)
        except Exception:
            return


def read_directory_changes(handle, path, recursive):
    # 创建一个大小为 64KB 的缓冲区用来存放目录下文件变化的结果，即 FILE_NOTIFY_INFORMATION 结构体
    event_buffer = ctypes.create_string_buffer(BUFFER_SIZE)
    # 表示最后写入到 event_buffer 中的字节数
    # ctypes.wintypes.DWORD() 是一个函数调用，它会返回一个 DWORD 值为 0 的 ctypes 对象
    # ctypes.wintypes.DWORD 是一个 DWORD 类型的 ctypes 声明
    nbytes = ctypes.wintypes.DWORD()
    try:
        # ReadDirectoryChangesW 同步调用时，会被阻塞
        ReadDirectoryChangesW(
            handle,
            ctypes.byref(event_buffer),
            len(event_buffer),
            recursive,
            WATCHDOG_FILE_NOTIFY_FLAGS,
            ctypes.byref(nbytes),
            None,
            None,
        )
    except OSError as e:
        if e.winerror == ERROR_OPERATION_ABORTED:
            return [], 0

        # Handle the case when the root path is deleted
        # 在使用 CreateFileW 函数获取文件夹的句柄时，设置了 FILE_SHARE_DELETE 标志位，允许此文件夹
        # 在被某个进程使用时，此文件夹本身以及文件夹下的所有文件可以被其它进程删除
        if _is_observed_path_deleted(handle, path):
            return _generate_observed_path_deleted_event()

        raise e

    # 1.ctypes.create_string_buffer() 是用于创建指定长度的字符串缓冲区的 ctypes 函数，其返回值是一个 ctypes.Array 对象，
    # 可以用来存储二进制数据，raw 属性是 ctypes.Array 类型的一个属性，表示数组的原始字节串（即二进制数据）
    # 对于 create_string_buffer() 返回的 ctypes.Array 对象来说，它包含了缓冲区中当前存储的所有二进制数据
    # 2.调用 DWORD() 构造函数创建一个 DWORD 对象，并使用 value 属性获取该对象的值
    return event_buffer.raw, int(nbytes.value)


class WinAPINativeEvent:
    def __init__(self, action, src_path):
        self.action = action
        self.src_path = src_path

    @property
    def is_added(self):
        return self.action == FILE_ACTION_CREATED

    @property
    def is_removed(self):
        return self.action == FILE_ACTION_REMOVED

    @property
    def is_modified(self):
        return self.action == FILE_ACTION_MODIFIED

    @property
    def is_renamed_old(self):
        return self.action == FILE_ACTION_RENAMED_OLD_NAME

    @property
    def is_renamed_new(self):
        return self.action == FILE_ACTION_RENAMED_NEW_NAME

    @property
    def is_removed_self(self):
        return self.action == FILE_ACTION_REMOVED_SELF

    def __repr__(self):
        return (
            f"<{type(self).__name__}: action={self.action}, src_path={self.src_path!r}>"
        )


def read_events(handle, path, recursive):
    buf, nbytes = read_directory_changes(handle, path, recursive)
    # events 是一个元组的集合，每一个元组中包括（Action，Filename）两个字段
    events = _parse_event_buffer(buf, nbytes)
    return [WinAPINativeEvent(action, src_path) for action, src_path in events]
