# This is a sample Python script.
import logging
import math

import obspy.signal.detrend
import scipy.integrate
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from obspy import read
import numpy as np
import datetime
import matplotlib.pyplot as plt
from scipy import signal
import obspy.signal
from obspy.signal.filter import bandpass
from obspy.signal.differentiate_and_integrate import integrate_cumtrapz

COLORS = ['red', 'blue', 'green']


class Wave:
    """
    地震波数据
    """
    def __init__(self):
        self.trace_vector = np.array([])
        self.PGA = 0
        self.PGV = 0

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

                if data_counter == 0:
                    acc_id = 'x_acc'
                elif data_counter == 1:
                    acc_id = 'y_acc'
                elif data_counter == 2:
                    acc_id = 'z_acc'

                trace = TraceData(acc_id, acc, sample_rate, start_time, end_time, COLORS[data_counter])

                if preprocess:
                    trace.preprocess_data()

                acc_vector = np.concatenate((acc_vector, [trace]))
                data_counter += 1

        self.trace_vector = acc_vector

    """
    按照 GB/T17742—2020 计算 PGA 参数
    """
    def acc_vector_sum(self, plot=False):
        trace_vector = self.trace_vector
        acc_vector_sum = np.zeros((len(trace_vector[0].acc_data)))

        for _ in range(len(self.trace_vector)):
            self.trace_vector[_].print_acc_max()
            if plot:
                self.trace_vector[_].plot_accform()

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
    def vel_vector_sum(self, plot=False):
        # 先对加速度数据进行积分
        trace_vector = self.trace_vector
        integrals = np.array([])
        for _ in range(len(trace_vector)):
            integrals = np.append(integrals, trace_vector[_].integrate_acc())
            trace_vector[_].print_vel_max()
            if plot:
                trace_vector[_].plot_velform()

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
    def cal_intensity(self):
        gravity = 9.79865
        PGA = self.acc_vector_sum(plot=False) * gravity
        PGV = self.vel_vector_sum(plot=False) * gravity

        print('PGA: %.5f g, PGV: %.5f g' % (self.PGA, self.PGV))

        Ia = 3.17 * np.log10(PGA) + 6.59
        Iv = 3.0 * np.log10(PGV) + 9.77

        if Ia >= 6.0 and Iv >= 6.0:
            intensity = Iv
        else:
            intensity = (Ia + Iv) / 2
        intensity = min(max(intensity, 1.0), 12.0)

        return intensity


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
        self.acc_data = obspy.signal.detrend.polynomial(self.acc_data, order=3, plot=True)
        # 2.使用带通滤波来进行滤除噪音，平滑数据
        self.acc_data = bandpass(self.acc_data, 0.1, 10, 200, corners=4, zerophase=False)

        return self.acc_data

    """
    将加速度数据积分成速度
    """
    def integrate_acc(self):
        for i in range(len(self.acc_data)):
            self.vel_data = np.append(self.vel_data, [scipy.integrate.trapz(self.acc_data[: i + 1], dx=0.005)])
        # self.vel_data = integrate_cumtrapz(self.acc_data, 0.005)
        return self.vel_data

    def print_acc_max(self):
        print(self.acc_id + ': ' + str(np.max(np.abs(self.acc_data))))

    def print_vel_max(self):
        print(self.acc_id + ': ' + str(np.max(np.abs(self.vel_data))))

    def plot_accform(self):
        data_len = len(self.acc_data)
        x = np.linspace(1, data_len, data_len, dtype=int)
        plt.figure(figsize=(40, 5), dpi=300)
        plt.title(self.acc_id)
        plt.plot(x, self.acc_data, label='acceleration', color=self.color)
        plt.legend()
        plt.grid()
        plt.show()

    def plot_velform(self):
        data_len = len(self.vel_data)
        x = np.linspace(1, data_len, data_len, dtype=int)
        plt.figure(figsize=(40, 5), dpi=300)
        plt.title(self.acc_id)
        plt.plot(x, self.vel_data, label='velocity', color=self.color)
        plt.legend()
        plt.grid()
        plt.show()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    wave = Wave()
    wave.load_data("./models/226.msd", preprocess=True)
    print(wave.cal_intensity())
