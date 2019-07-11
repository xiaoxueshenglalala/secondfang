# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
from scrapy.exceptions import DropItem
from secondfang.items import DetailItem

# 获取数据库连接


def getConn():
    conn = pymysql.Connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='root',
        db='fangjia',
        charset='utf8'
    )
    return conn

# 关闭数据库资源


def closeConn(cursor, conn):
    # 关闭游标
    if cursor:
        cursor.close()
    # 关闭数据库连接
    if conn:
        conn.close()


class MySQLPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['gpfyid'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['gpfyid'])
            if item.__class__ == DetailItem:
                self.insert(item)
                return
        return item

    def insert(self, item):
        try:
            # 获取数据库连接
            conn = getConn()
            # 获取游标
            cursor = conn.cursor()
            # 插入数据库
            sql = "INSERT INTO second(cjsj, cqmc, cyrybh, fczsh, fwtybh, fwyt, fwytValue, gpfyid, gpid, gplxrxm, gply, gpzt, gpztValue, hxs, hxt, hxw, hyid, hyjzsj, isnew, jzmj, mdmc, qyid, qyzt, scgpshsj, sellnum, sjhm, sqhysj, szlc, szlcname, tygpbh, wtcsjg, wtdqts, wtxybh, wtxycode, wtxyid, xqid, xqmc, xzqh, xzqhname, zzcs)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            params = (
                item['cjsj'],
                item['cqmc'],
                item['cyrybh'],
                item['fczsh'],
                item['fwtybh'],
                item['fwyt'],
                item['fwytValue'],
                item['gpfyid'],
                item['gpid'],
                item['gplxrxm'],
                item['gply'],
                item['gpzt'],
                item['gpztValue'],
                item['hxs'],
                item['hxt'],
                item['hxw'],
                item['hyid'],
                item['hyjzsj'],
                item['isnew'],
                item['jzmj'],
                item['mdmc'],
                item['qyid'],
                item['qyzt'],
                item['scgpshsj'],
                item['sellnum'],
                item['sjhm'],
                item['sqhysj'],
                item['szlc'],
                item['szlcname'],
                item['tygpbh'],
                item['wtcsjg'],
                item['wtdqts'],
                item['wtxybh'],
                item['wtxycode'],
                item['wtxyid'],
                item['xqid'],
                item['xqmc'],
                item['xzqh'],
                item['xzqhname'],
                item['zzcs'])
            cursor.execute(sql, params)

            # 事务提交
            conn.commit()
        except Exception as e:
            # 事务回滚
            print(e)
            conn.rollback()
            print('插入失败')
        finally:
            # 关闭游标和数据库连接
            closeConn(cursor, conn)
