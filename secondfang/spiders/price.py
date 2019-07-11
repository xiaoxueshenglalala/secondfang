# -*- coding: utf-8 -*-
from scrapy import Spider, Request
import json
from secondfang.items import DetailItem


class PriceSpider(Spider):
    name = 'price'
    allowed_domains = ['jjhygl.hzfc.gov.cn']
    start_urls = ['http://jjhygl.hzfc.gov.cn/webty/WxAction_getGpxxSelectList.jspx?page=1']

    gpfyid_url = 'http://jjhygl.hzfc.gov.cn/webty/WxAction_getGpxxSelectList.jspx?page={page}'
    detail_url = 'http://jjhygl.hzfc.gov.cn//webty/WxAction_toGpxxInfo.jspx?gpfyid={gpfyid}'

    def start_requests(self):
        i = 1
        while i:
            yield Request(self.gpfyid_url.format(page=i), callback=self.parse_gpfyid)
            i += 1

    def parse_gpfyid(self, response):
        result = json.loads(response.text)
        if result['isover'] is False:
            for list in result['list']:
                if list['scgpshsj'] == '2019-07-11':
                    yield Request(self.detail_url.format(gpfyid=list['gpfyid']), callback=self.parse_detail)
                else:
                    print('-----------时间区间错误-----------')
        else:
            print('------------已跑完，请停止---------------')

    def parse_detail(self, response):
        result1 = json.loads(response.text)
        if result1['flag'] is True:
            item = DetailItem()
            for field in item.fields:
                if field in result1['gpxx'].keys():
                    item[field] = result1['gpxx'][field]
            yield item












