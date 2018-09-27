# -*- coding: utf-8 -*-
# @Time    : 18/9/20 下午3:09
# @Author  : Tang Jinghong
# @Email   : tangjinghong@iie.ac.cn
# @File    : test.py
# @Software: PyCharm
import thread
from data_load.mysql_data import mysql
import math

if __name__ == '__main__':
    # 先获取所有
    # 取模=0  计算多少个线程
    s = int(math.ceil(45 /float(40)))
    print s
