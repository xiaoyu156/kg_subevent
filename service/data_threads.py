# -*- coding: utf-8 -*-
# @Time    : 18/9/20 下午3:13
# @Author  : Tang Jinghong
# @Email   : tangjinghong@iie.ac.cn
# @File    : move_data.py
# @Software: PyCharm
import sys

reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('F:/work/kg_data/kg_subevent')

from data_load.mysql_data import mysql
import random
import json
import math
import threading
import traceback


class deal_data:
    def __init__(self):
        pass

    def move_data(self, events):
        db = mysql()
        for event in events:
            # 一、遍历事件，查询事件所对应样本
            # 事件相关
            event_id = event['event_id']
            event_org = event['org'].encode('utf-8').split(',')
            event_person = event['person'].encode('utf-8').split(',')
            place = event['place'].encode('utf-8').split(',')

            # 二、随机一批样本作为子事件
            samples = db.getSamplesByEventId(event_id)
            randomSamples = self.randomSamples(samples)
            result_list = []

            # 三、获取账号信息，每个子事件随机一批账号
            accounts = db.getSomeAccount(event_id)

            # 四、构建子事件
            for sample in randomSamples:
                result = {}
                # 构建子事件基础信息
                keywords = sample['keywords'].encode('utf-8').split(';')
                sub_event = str(sample['newsTitle'].encode('utf-8'))
                event_time = sample['publishDate'].strftime('%Y-%m-%d')

                # 随机账号
                randomAccouts = self.randomAccount(accounts)
                event_account = []

                for account in randomAccouts:
                    event_account.append(account['account_name'])
                result['eventName'] = sub_event
                result['eventAccount'] = event_account
                result['eventTime'] = event_time
                result['eventKeyword'] = self.randomPlace(keywords)
                result['eventOutfit'] = self.randomOrgs(event_org)
                result['eventPeople'] = self.randomPeople(event_person)
                result['eventAdress'] = self.randomPlace(place)
                result_list.append(result)

            # 五、更新存储
            # 转换json
            r = json.dumps(result_list, ensure_ascii=False)
            print "event_id:", event_id, ",sub_event:", r
            db.updateEvent(event_id, r)

    # 随机样本
    def randomSamples(self, samples):
        randomSamples = []
        sampleSize = len(samples)
        randomCount = sampleSize
        if len(samples) > 2:
            randomCount = random.randint(1, sampleSize)
            if randomCount > 20:
                randomCount = 20
        randomSamples = random.sample(samples, randomCount)
        # 根据publishDate字段排序
        randomSamples = sorted(randomSamples, key=lambda s: s['publishDate'])
        return randomSamples

    # 随机账户
    def randomAccount(self, accounts):
        randomAccount = []
        accountSize = len(accounts)
        if len(accounts) >= 2:
            accountSize = random.randint(1, accountSize / 2)
        randomAccount = random.sample(accounts, accountSize)
        return randomAccount

    # 随机关键词
    def randomKeywords(self, keywords):
        length = len(keywords)
        if length > 2:
            length = random.randint(1, length)
        return random.sample(keywords, length)

    # 随机组织机构
    def randomOrgs(self, orgs):
        length = len(orgs)
        if length > 1:
            length = random.randint(1, length)
        return random.sample(orgs, length)

    # 随机地点
    def randomPlace(self, place):
        length = len(place)

        if length > 1:
            length = random.randint(1, length)
        return random.sample(place, length)

    # 随机人物
    def randomPeople(self, people):
        length = len(people)
        if length > 1:
            length = random.randint(1, length)
        return random.sample(people, length)

    # 处理流程，多线程处理
    def process(self, count):
        db = mysql()
        threads = []
        # 获取所有事件
        events = db.getAllEvents();
        events_size = len(events)
        print "本次补全sub_events记录：", events_size
        if events_size > 0:
            thread_num = int(math.ceil(events_size / float(count)))
            for i in range(0, events_size, count):
                sub_list = events[i:i + count]
                t = threading.Thread(target=self.move_data, args=(sub_list,))
                threads.append(t)
            for j in range(thread_num):
                print "线程启动：thread-", j
                # th.setDaemon(True)
                threads[j].start()


if __name__ == '__main__':
    data = deal_data()
    # 每个线程分配的任务
    count = 300
    # 数据库每次默认获取1000条数据，所以每次最多开启20个线程工作
    while (True):
        try:
            data.process(count)
            threading._sleep(10)
        except Exception as e:
            # 打印异常信息
            traceback.print_exc()
