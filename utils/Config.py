# -*- coding: utf-8 -*-
# @Time    : 18/9/20 下午3:07
# @Author  : Tang Jinghong
# @Email   : tangjinghong@iie.ac.cn
# @File    : Config.py
# @Software: PyCharm

import ConfigParser


class Config:
    def __init__(self):
        # 加载配置文件
        self.config = ConfigParser.ConfigParser()
        self.config.read('F:/work/kg_data/kg_subevent/config/config.ini')

    def get(self, source, key):
        return self.config.get(source, key)

if __name__ == '__main__':
    print Config().get('mysql', 'host')