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


class move_data:
    def __init__(self):
        pass

    def move_data(self):
        print sys.path
        db = mysql()
        # 获取所有事件
        events = db.getAllEvents();
        # 遍历事件，查询事件所对应样本
        f = open('test.txt', 'w')
        for event in events:
            # 事件相关

            event_id = event['event_id']
            event_org = event['org'].encode('utf-8').split(',')
            event_person = event['person'].encode('utf-8').split(',')
            place = event['place'].encode('utf-8').split(',')
            # 随机样本
            samples = db.getSamplesByEventId(event_id)
            # print event
            randomSamples = self.randomSamples(samples)
            result_list = []
            print "event_id:", event_id
            for sample in randomSamples:
                result = {}
                keywords = sample['keywords'].encode('utf-8').split(';')
                sub_event = str(sample['newsTitle'].encode('utf-8'))
                event_time = sample['publishDate'].strftime('%Y-%m-%d')
                # 获取账号信息
                accounts = db.getSomeAccount(event_id, 10)
                # 随机账号
                randomAccouts = self.randomAccount(accounts)
                event_account = []
                for account in randomAccouts:
                    event_account.append(account['name'])
                result['eventName'] = sub_event
                result['eventAccount'] = event_account
                result['eventTime'] = event_time
                result['eventKeyword'] = self.randomPlace(keywords)
                result['eventOutfit'] = self.randomOrgs(event_org)
                result['eventPeople'] = self.randomPeople(event_person)
                result['eventAdress'] = self.randomPlace(place)
                result_list.append(result)

            result_json = {}
            result_json = result_list
            r = json.dumps(result_json, ensure_ascii=False)
            print "sub_events:", r
            db.updateEvent(r, event_id)

    def randomSamples(self, samples):
        randomSamples = []
        sampleSize = 0
        if len(samples) > 2:
            sampleSize = random.randint(1, len(samples) / 2)
        randomSamples = random.sample(samples, sampleSize)
        return randomSamples

    def randomAccount(self, accounts):
        randomAccount = []
        accountSize = 0
        if len(accounts) >= 2:
            accountSize = random.randint(1, len(accounts) / 2)
        randomAccount = random.sample(accounts, accountSize)
        return randomAccount

    def randomKeywords(self, keywords):
        length = len(keywords)
        if length > 2:
            length = random.randint(1, length)
        return random.sample(keywords, length)

    def randomOrgs(self, orgs):
        length = len(orgs)
        if length > 1:
            length = random.randint(1, length)
        return random.sample(orgs, length)

    def randomPlace(self, place):
        length = len(place)

        if length > 1:
            length = random.randint(1, length)
        return random.sample(place, length)

    def randomPeople(self, people):
        length = len(people)
        if length > 1:
            length = random.randint(1, length)
        return random.sample(people, length)


if __name__ == '__main__':
    move_data().move_data()
