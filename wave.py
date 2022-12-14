# This is a sample Python script.
import logging

import numpy as np
import obspy.signal
import obspy.signal.detrend
import scipy.integrate
from obspy import read
from obspy.signal.filter import bandpass
from scipy import signal

COLORS = ['red', 'blue', 'green']
GRAVITY = 9.79865


class Wave:

    ACCS = ['XOHNE, g', 'YOHNN, g', 'ZOHNZ, g']

    """
    地震波数据
    """
    def __init__(self, name):
        self.trace_vector = np.array([])
        self.PGA = 0
        self.PGV = 0
        self.ins_intensity = 0
        self.intensity_grade = 0
        self.output = {}
        self.name = name

    """
    从 data_path 指明的 msd 文件中读取地震波形数据，一个 msd 地震波形数据对应 3 个分量，
    每一个分量，创建一个 TraceData 对象，保存到 trace_vector 向量中
    """
    def load_data(self, data_path, preprocess=True):
        msd = read(data_path)
        trace_len = len(msd.traces)

        data_counter = 0
        acc_vector = np.array([])

        for i in range(trace_len):
            if len(msd.traces[i].data) != 0:
                if data_counter == 3:
                    logging.warning("there's more than 3 traces in msd file")
                    break

                acc_id = ''
                acc = msd.traces[i].data
                sample_rate = msd.traces[i].meta.sampling_rate
                start_time = msd.traces[i].meta.starttime.datetime
                end_time = msd.traces[i].meta.endtime.datetime

                trace = TraceData(Wave.ACCS[data_counter], acc, sample_rate, start_time, end_time, COLORS[data_counter])

                if preprocess:
                    trace.preprocess_data()

                acc_vector = np.concatenate((acc_vector, [trace]))
                data_counter += 1

        self.trace_vector = acc_vector

    """
    按照 GB/T17742—2020 计算 PGA 参数
    """
    def acc_vector_sum(self):
        trace_vector = self.trace_vector
        acc_vector_sum = np.zeros((len(trace_vector[0].acc_data)))

        for i in range(len(trace_vector[0].acc_data)):
            x_acc = np.power(trace_vector[0].acc_data[i], 2)
            y_acc = np.power(trace_vector[1].acc_data[i], 2)
            z_acc = np.power(trace_vector[2].acc_data[i], 2)

            acc_vector_sum[i] = np.sqrt(x_acc + y_acc + z_acc)

        self.PGA = np.max(acc_vector_sum)
        return self.PGA

    """
    按照 GB/T17742—2020 计算 PGV 参数
    """
    def vel_vector_sum(self):
        # 先对加速度数据进行积分
        trace_vector = self.trace_vector
        integrals = np.array([])

        for _ in range(len(trace_vector)):
            integrals = np.append(integrals, trace_vector[_].integrate_acc())

        integrals = integrals.reshape((len(trace_vector), len(trace_vector[0].acc_data)))
        vel_vector_sum = np.zeros((len(trace_vector[0].acc_data)))
        for _ in range(len(trace_vector[0].acc_data)):
            x_vel = np.power(integrals[0][_], 2)
            y_vel = np.power(integrals[1][_], 2)
            z_vel = np.power(integrals[2][_], 2)

            vel_vector_sum[_] = np.sqrt(x_vel + y_vel + z_vel)

        self.PGV = np.max(vel_vector_sum)
        return self.PGV

    """
    根据 PGA 参数和 PGV 参数来计算最终的地震烈度 I 
    """
    def cal_ins_intensity(self):

        PGA = self.acc_vector_sum() * GRAVITY
        PGV = self.vel_vector_sum() * GRAVITY

        self.output['PGA'] = PGA * 100
        self.output['PGV'] = PGV * 100

        Ia = 3.17 * np.log10(PGA) + 6.59
        Iv = 3.0 * np.log10(PGV) + 9.77

        if Ia >= 6.0 and Iv >= 6.0:
            intensity = Iv
        else:
            intensity = (Ia + Iv) / 2
        ins_intensity = min(max(intensity, 1.0), 12.0)

        self.output['intensity'] = ins_intensity
        self.output['grade'] = round(ins_intensity)


class TraceData:

    def __init__(self, acc_id, acc_data, sample_rate, start_time, end_time, color):
        self.acc_id = acc_id
        # 地震波形数据的采样频率
        self.sample_rate = int(sample_rate)
        # 地震波形数据的开始时间
        self.start_time = start_time
        # 地震波形数据的结束时间
        self.end_time = end_time

        # full_scale 是传感器的满程参数
        self.full_scale = 2
        # 传感器参数，要把所有的数据除以此固定参数，得到最终的数据
        self.scaling_factor = np.divide(0.9 * np.power(2, 23), self.full_scale)
        self.acc_data = np.array(list(map(lambda x: np.divide(x, self.scaling_factor), acc_data)))

        self.vel_data = np.array([])
        self.color = color

    @staticmethod
    def butter_bandpass(lowcut, highcut, fs, order=4):
        nyq = 0.5 * fs
        low = lowcut / nyq
        high = highcut / nyq
        b, a = signal.butter(order, [low, high], btype='band')
        return b, a

    def preprocess_data(self):
        # 1.使用高通滤波进行基线校正
        # （使用 scipy 中的趋势消除函数来进行基线校正）
        self.acc_data = obspy.signal.detrend.polynomial(self.acc_data, order=3, plot=False)
        # 2.使用带通滤波来进行滤除噪音，平滑数据
        self.acc_data = bandpass(self.acc_data, 0.1, 10, 200, corners=4, zerophase=False)

        return self.acc_data

    """
    将加速度数据积分成速度
    """
    def integrate_acc(self):
        for i in range(len(self.acc_data)):
            self.vel_data = np.append(self.vel_data, [scipy.integrate.trapz(self.acc_data[: i + 1], dx=0.005)])
        return self.vel_data

    def print_acc_max(self):
        return self.acc_id[0] + '方向加速度最大值为: ' + str(np.max(np.abs(self.acc_data)) * GRAVITY * 100)[0:6] + ' gal\n'

    def print_vel_max(self):
        return self.acc_id[0] + '方向速度最大值为: ' + str(np.max(np.abs(self.vel_data)) * GRAVITY * 100)[0:6] + ' cm/s\n'

