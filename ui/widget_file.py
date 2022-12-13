import threading
import time

import xlwt
from PyQt5.Qt import *
import sys
import os
from wave_gjy import *
from multiprocessing import Pool
from collections import OrderedDict
import atexit
import logging
import traceback
import functools


# noinspection PyAttributeOutsideInit
class Window(QWidget):
    def __init__(self):
        super().__init__()
        # 保存用户选取的 msd 文件名称
        self.file_names = np.array([])
        self.logger = LoggerFactory.instance()

        self.show_logo()

        # 创建进程池，进程池的进程数设置为 cpu 核心数的二分之一
        self.process_pool = Pool(os.cpu_count() // 2)
        self.logger.info("进程池创建完毕: " + str(self.process_pool))
        # 设置线程池的大小为 2
        # 使用重写的线程池，控制打印输出线程池状态
        self.thread_pool = ThreadPoolExecutorPrint()
        # 设置线程池中的最大线程数
        self.thread_pool.setMaxThreadCount(1)
        # 线程池中空闲线程的超时时间，当线程的空闲时间超过 60000 毫秒时，就会被清除
        self.thread_pool.setExpiryTimeout(60000)
        self.logger.info("线程池创建完毕: " + str(self.thread_pool))

        # 保存进程池异步计算的结果
        self.async_result = None
        self.counter = 0

        self.dir_path = ''

        # 程序正常/异常终止时，都会调用 destroy 方法
        atexit.register(self.destory)
        # 绘制窗口界面
        self.setup_ui()
        self.logger.info("窗口创建完毕")

    def show_logo(self):
        with open("../wave-logo.txt", "r") as f:
            lines = f.readlines()
            for line in lines:
                if line.find('Arthuor') != -1:
                    self.logger.info(line)
                else:
                    line = line.rstrip()
                    print(line)

    # 当用户编辑时，将新输入的文件夹路径保存到 dir_path 变量中
    def change_text(self, text):
        self.dir_path = text

    def setup_ui(self):
        # 设定顶层窗口的标题，以及设置为固定大小
        self.setWindowTitle("地震烈度计算器")
        self.setFixedSize(600, 400)
        self.setWindowIcon(QIcon("../static/netty.png"))

        self.file_widget = QWidget(self)
        widget = self.file_widget

        self.fetch_file_btn = QPushButton(widget)
        self.fetch_file_btn.setText("选取文件")
        self.fetch_file_btn.clicked.connect(self.fetch_file)

        self.cal_btn = QPushButton(widget)
        self.cal_btn.setText("生成烈度文件")
        # 连接信号与槽，点击计算按钮时，回调 cal_intensity_parallel 方法
        self.cal_btn.clicked.connect(self.cal_intensity_parallel)

        self.file_display_text = QTextEdit(widget)
        self.file_display_text.setPlaceholderText("msd 文件路径")
        self.file_display_text.setFocusPolicy(Qt.NoFocus)

        self.save_line = QLineEdit(widget)
        self.save_line.setPlaceholderText("保存文件夹路径")
        self.save_line.textEdited.connect(self.change_text)
        self.tool_btn = QToolButton(widget)

        self.fetch_file_btn.resize(120, 40)
        self.cal_btn.resize(120, 40)
        self.file_display_text.resize(500, 240)
        self.save_line.resize(500, 30)
        self.tool_btn.resize(30, 30)

        self.tool_btn.setIcon(QIcon("../static/dir.png"))
        self.tool_btn.setAutoRaise(True)
        self.tool_btn.setIconSize(QSize(20, 20))
        self.tool_btn.clicked.connect(self.save_file_dir)

        margin_between = 20
        margin_btn_between = 170
        text_margin_left = int((self.width() - self.file_display_text.width()) / 2)
        text_margin_top = int(
            (self.height() - (self.fetch_file_btn.height() + margin_between * 2 + self.file_display_text.height()
                              + self.save_line.height())) / 2)
        btn_margin_left = int((self.width() - self.fetch_file_btn.width() * 2 - margin_btn_between) / 2)

        self.file_display_text.move(text_margin_left, text_margin_top)
        self.save_line.move(text_margin_left, text_margin_top + margin_between + self.file_display_text.height())
        self.tool_btn.move(text_margin_left + self.save_line.width() - self.tool_btn.width(),
                           text_margin_top + margin_between + self.file_display_text.height())

        self.fetch_file_btn.move(btn_margin_left, self.save_line.y() + self.save_line.height() + margin_between)
        self.cal_btn.move(btn_margin_left + margin_btn_between + self.fetch_file_btn.width(),
                          self.save_line.y() + self.save_line.height() + margin_between)

    def fetch_file(self):
        self.file_names = np.array([])
        # 实例化 QFileDialog
        tmp_tuple = QFileDialog().getOpenFileNames(self, "选择 msd 文件", os.getcwd(), "MSD Files (*.msd)")[0]
        filenames = np.array(list(tmp_tuple))
        self.file_names = np.r_[self.file_names, filenames]

        self.file_display_text.clear()
        for name in filenames:
            # 列表中的第一个元素即是文件路径，以只读的方式打开文件
            f = open(name, 'r')

            with f:
                # 接受读取的内容，并显示到多行文本框中
                data = f.name
                self.file_display_text.insertPlainText(data + '\n')

        self.file_display_text.setFocusPolicy(Qt.NoFocus)

    def save_file_dir(self):
        # 返回用户选中的【已经存在的】文件夹路径
        self.dir_path = QFileDialog.getExistingDirectory(self, "选取文件夹", os.getcwd())
        self.save_line.clear()
        self.save_line.setText(self.dir_path)

    def func(self):
        # 将 file_names 中的属性依次分配给 cal_intensity0 方法执行，返回 MapResult 对象（继承了 AsyncResult）
        self.async_result = self.process_pool.map_async(cal_intensity0, self.file_names)
        # 阻塞等待所有进程的计算结果
        # 如果远程调用发生异常，这个异常会通过 get() 重新抛出。这里抛出的异常会抛出，由 worker 统一捕获然后封装
        results = self.async_result.get()

        # 创建 excel 文件
        workbook = xlwt.Workbook(encoding="ascii")
        # 创建新的 sheet 表
        sheet = workbook.add_sheet("地震烈度计算")
        workbook_name = results[0]['name']

        # 将 name、PGA、PGV、intensity、grade 信息保存到 excel 表中
        for row, res in enumerate(results):
            if row == 0:
                for col, key in enumerate(res):
                    sheet.write(row, col, key)
            row += 1
            for col, key in enumerate(res):
                sheet.write(row, col, res[key])

        # 将 excel 文件保存到 dir_path 路径下
        path = os.path.join(self.save_line.text(), workbook_name + ".xls")
        if os.path.exists(path):
            path = os.path.join(self.save_line.text(), workbook_name + "(" + str(self.counter) + ")" + ".xls")
            self.counter += 1
        workbook.save(path)

        # 返回执行结果给 worker 类中的 result 变量
        return True

    def cal_done_callback(self, *args):
        try:
            flag = False
            args = args[0]

            # args 为 bool 类型，说明执行成功
            if isinstance(args, bool):
                flag = args
            # args 为 tuple 类型，说明执行出现错误，被 worker 封装成 tuple 对象，发送 error 信号
            elif isinstance(args, tuple):
                flag = False
                cls_type, err, trace_info = args
                # 打印出错堆栈信息
                self.logger.debug(trace_info)

            if flag:
                QMessageBox.information(self, '提示', "计算成功完成", QMessageBox.Yes, QMessageBox.Yes)
                self.logger.info(str(len(self.file_names)) + " 个任务执行完毕")
            else:
                QMessageBox.warning(self, '提示', "计算失败，详情请查看日志", QMessageBox.Yes, QMessageBox.Yes)
                self.logger.info(str(len(self.file_names)) + " 个任务执行过程中出现错误")

            self.cal_btn.setEnabled(True)
        except BaseException:
            self.logger.debug(traceback.format_exc())

    def cal_intensity_parallel(self):
        if len(self.dir_path) == 0 and len(self.file_names) == 0:
            QMessageBox.warning(self, '警告', "msd 文件路径和保存文件夹路径都未选择", QMessageBox.Yes, QMessageBox.Yes)
            return

        if len(self.dir_path) == 0:
            QMessageBox.warning(self, '警告', "保存文件夹路径未选择", QMessageBox.Yes, QMessageBox.Yes)
            return

        if not os.path.exists(self.dir_path):
            QMessageBox.warning(self, '警告', "选取的文件夹路径不存在", QMessageBox.Yes, QMessageBox.Yes)
            return

        if len(self.file_names) == 0:
            QMessageBox.warning(self, '警告', "msd 文件路径未选择", QMessageBox.Yes, QMessageBox.Yes)
            return

        self.logger.info(str(len(self.file_names)) + " 个任务开始执行")
        # 计算过程中设置按钮为禁用状态，防止重复点击
        self.cal_btn.setEnabled(False)

        try:
            worker = Worker(self.func)
            # 将 finished 和 error 信号连接到同一个槽，当 worker 执行完成之后，回调 cal_done_callback
            worker.signals.finished.connect(self.cal_done_callback)
            worker.signals.error.connect(self.cal_done_callback)

            # Execute
            # 计算线程提交到线程池中，防止冻结 GUI 界面
            self.thread_pool.start(worker)
        except Exception:
            self.logger.debug(traceback.format_exc())

    # 实现窗口的拖拽功能
    def mousePressEvent(self, evt):
        self.mouse_x = evt.globalX()
        self.mouse_y = evt.globalY()
        self.origin_x = self.x()
        self.origin_y = self.y()

    def mouseMoveEvent(self, evt):
        move_x = evt.globalX() - self.mouse_x
        move_y = evt.globalY() - self.mouse_y

        self.move(self.origin_x + move_x, self.origin_y + move_y)
        self.setCursor(Qt.ClosedHandCursor)

    def mouseReleaseEvent(self, evt):
        self.unsetCursor()

    def closeEvent(self, evt):
        # 调用 ready 方法判断进程池中的任务是否执行完毕
        if self.async_result is not None and not self.async_result.ready():
            reply = QMessageBox.warning(self, '警告', "进程仍然在处理任务，确认是否退出?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        else:
            reply = QMessageBox.question(self, '确认关闭', '系统将退出，是否确认?', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            evt.accept()
        else:
            evt.ignore()

    # 在 atexit 模块中注册 destory 方法，销毁线程池和进程池，优雅停机
    def destory(self):
        self.logger.info("程序退出：销毁线程池与进程池")

        if self.async_result is not None:
            try:
                # 调用 wait 等待进程池中的多个任务执行完毕
                self.async_result.wait()
            except Exception:
                self.logger.debug(traceback.format_exc())

        # 最终调用 _terminate_pool 函数，终止 _handle_results 线程、_task_handler 线程、_work_handler 线程以及进程，最后清除队列中的数据
        self.process_pool.terminate()
        self.logger.info("销毁进程池：" + str(self.process_pool))

        # 阻塞 2s 等待 QThreadPool 线程池执行完毕
        self.thread_pool.waitForDone(2000)
        self.logger.info("销毁线程池：" + str(self.thread_pool))


# 真正执行地震烈度计算程序
def cal_intensity0(file_path):
    wave = Wave()
    wave.load_data(file_path, preprocess=True)
    # 将返回的 PGA、PGV、intensity 和 grade 封装成一个有序字典
    output = OrderedDict(wave.cal_ins_intensity())

    name = file_path[file_path.rfind('/') + 1: file_path.find('.')]
    output['name'] = name
    # 将 name 键值对移动到第一个
    output.move_to_end('name', last=False)

    logger = LoggerFactory.instance()
    logger.info(name + " 计算完毕")

    return output


# 全局信号
class WorkerSignals(QObject):

    error = pyqtSignal(tuple)
    finished = pyqtSignal(object)


# 把要执行的不同函数统一封装成 worker 对象，提交给 QThreadPool 进行执行
class Worker(QRunnable):

    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()

        # fn 为要放入到线程池执行的函数
        self.fn = fn
        # args 和 kwargs 为 fn 函数的参数
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self):

        try:
            # fire processing
            result = self.fn(*self.args, **self.kwargs)
        except Exception:
            # 获取 fn 执行过程中产生的异常和值
            exctype, value = sys.exc_info()[:2]
            # 将 error 信号发送给 Qt 主线程，告知计算过程中出现错误
            self.signals.error.emit((exctype, value, traceback.format_exc()))
        else:
            # 将 finished 信号发送给 Qt 主线程，并将计算结果返回
            self.signals.finished.emit(result)


# 重写 ThreadPoolExecutor 的 __str__ 属性，控制对象的打印输出
class ThreadPoolExecutorPrint(QThreadPool):

    def __str__(self):
        # 打印输出线程池的 3 个关键属性信息：
        # _max_workers：最大线程数
        # _shutdown：线程池是否关闭
        # expire_timeout：线程池中线程超时时间
        attr_dict = {
            'active_threads': self.activeThreadCount(),
            'max_workers': self.maxThreadCount(),
            'expire_timeout': self.expiryTimeout()
        }

        attrs = " ".join("{}={}".format(key, attr_dict[key]) for key in attr_dict)
        # 返回输出线程池状态
        return "<{}:{}>".format(self.__class__.__bases__[0].__name__, attrs)


# 日志类工厂，使用单例模式创建唯一的一个日志记录对象
class LoggerFactory:

    # 创建类属性 _instance_lock（整个类唯一）
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls):
        # 如果 _logger 对象已经创建，直接返回
        if not hasattr(LoggerFactory, "_logger"):
            # 加锁，防止线程 race condition
            with LoggerFactory._instance_lock:
                # 如果 _logger 对象已经创建，直接返回
                if not hasattr(LoggerFactory, "_logger"):
                    # 创建日志记录对象
                    LoggerFactory._logger = logging.getLogger("wave-logger")
                    cls._logging_configuration(LoggerFactory._logger)
                return LoggerFactory._logger
        return LoggerFactory._logger

    @classmethod
    def _logging_configuration(cls, _logger):
        # 设置日志记录级别为 DEBUG
        # 日志登记从小到达依次为：DEBUG、INFO、WARNING、ERROR、CRITICAL
        _logger.setLevel(logging.DEBUG)

        # 格式化本地时间
        timestamp = time.strftime('%Y_%m_%d_%H_%M', time.localtime())
        # 创建一个 handler 用于写入日志文件
        file_handler = logging.FileHandler("../log/wave_" + timestamp + ".log")
        file_handler.setLevel(logging.DEBUG)

        # 创建一个 handler，用于输出到控制台
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)

        # 定义 handler 的输出格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # 给 handler 添加 formatter
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        # 给 logger 添加 handler
        _logger.addHandler(stream_handler)
        _logger.addHandler(file_handler)


if __name__ == '__main__':
    app = QApplication(sys.argv)

    win = Window()
    win.show()

    sys.exit(app.exec_())
