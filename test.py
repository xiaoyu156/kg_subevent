# -*- coding: utf-8 -*-
# @Time    : 18/9/20 下午3:09
# @Author  : Tang Jinghong
# @Email   : tangjinghong@iie.ac.cn
# @File    : test.py
# @Software: PyCharm
from data_load.mysql import mysql

db = mysql()
print db.getAllEvents()
