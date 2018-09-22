# -*- coding: utf-8 -*-
# @Time    : 18/9/20 下午3:06
# @Author  : Tang Jinghong
# @Email   : tangjinghong@iie.ac.cn
# @File    : mysql.py
# @Software: PyCharm

import traceback

import MySQLdb



class mysql:
    def __init__(self):
        self.conn = MySQLdb.connect(
            host="127.0.0.1",
            user="root",
            passwd="admin",
            db="kg",
            charset="utf8")

    def getAllEvents(self):
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        sql = "SELECT * from event_details WHERE subevents is NULL or subevents='' AND subevents NOT LIKE '%[*%' AND state=2 limit 500"
        n = cursor.execute(sql)

        return cursor.fetchall()

    def getSamplesByEventId(self, event_id):
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        sql = "select * from  event_sample_content WHERE specialId= %s"
        n = cursor.execute(sql, [event_id])

        return cursor.fetchall()

    def insertSamples(self, event, samples):
        # 使用cursor()方法获取操作游标
        cursor = self.conn.cursor()

        # SQL 插入语句
        sql = 'insert into event_sample_content(specialId, specialTitle, newsId, newsTitle, publishDate, content, keywords, digest) values(%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            # 执行sql语句
            for sample in samples:
                cursor.execute(sql, [event['id'], event['name'], sample['id'], sample['sample_title'],
                                     sample['publish_time'], sample['sample_content'], sample['keyword'],
                                     sample['sample_content']])
            # 提交到数据库执行
            self.conn.commit()
        except Exception as e:
            msg = traceback.format_exc()  # 方式1
            print (msg)
            # 回滚
            self.conn.rollback()

    def insertEvent(self, event, samples):
        if (event['descript'] == None or event['descript'] == '') and samples != None and len(samples) > 0:
            event['descript'] = samples[0]['sample_content']
        # 使用cursor()方法获取操作游标
        cursor = self.conn.cursor()

        sql = 'insert into event_details(event_id, title, time, digest, news_sum, news_literal, subevents) values(%s,%s,%s,%s,%s,%s,%s)'

        try:
            cursor.execute(sql,
                           [event['id'], event['name'], event['occurrence_time'], event['descript'], str(len(samples)),
                            '', ''])
            self.conn.commit
        except Exception as e:
            msg = traceback.format_exc()
            print msg

            self.conn.rollback()

    def getSomeAccount(self, event_id, some):
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

        sql = 'select * from account where event_id=%s'
        n = cursor.execute(sql, event_id)

        return cursor.fetchall()

    def updateEvent(self, subEvent, eventId):
        cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        sql = 'update event_details set subevents = %s,state=3 where event_id = %s'
        try:
            n = cursor.execute(sql, [str(subEvent), eventId])
            self.conn.commit()
        except Exception as e:
            print e
            # 回滚
            self.conn.rollback()
