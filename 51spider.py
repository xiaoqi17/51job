# -*- coding: utf-8 -*-
import pymongo
import requests
from bs4 import BeautifulSoup
from multiprocessing import Pool
import re
import time
import json
import sys
from requests import HTTPError, ConnectionError

reload(sys)
sys.setdefaultencoding('utf-8')

client = pymongo.MongoClient('localhost', connect=False)
db = client['51job']
def page_html(headers,urls):
    try:
        r = requests.get(urls, headers=headers)
        time.sleep(5)
        r.encoding = r.apparent_encoding  # 编码改为UTF-8
        if response.status_code == 200:
            pages = re.compile('<span class="td">.*?(\d+).*?</span><input id="jump_page"',re.S)
            page = pages.findall(r.text)[0]
            return page
        else:
            print '%s返回码错误'%urls
    except ConnectionError:
        print('Error occurred')

def index_html(headers,url):
    try:

        r = requests.get(url, headers=headers)
        time.sleep(5)
        r.encoding = r.apparent_encoding  # 编码改为UTF-8
        if response.status_code == 200:
            dome = r.text
            soup = BeautifulSoup(dome, 'lxml')
            links = soup.findAll("a", href=re.compile("com\/guangzhou-"))  # 利用正则和BeautifulSoup结合
            for link in links:
                urls = link.attrs['href']
                if urls == None:
                    pass
                else:
                    yield url
        else:
            print '%s返回码错误'%urls
    except ConnectionError:
        print('Error occurred')

def page_one_page(url, headers):
    try:
        print '正在爬取的链接%s'%url
        if db['51jobs'].find_one({'招聘链接': url}):
            print '%s爬过'%url
        else:
            response = requests.get(url, headers=headers)
            time.sleep(3)
            if response.status_code == 200:
                response.encoding = response.apparent_encoding
                text = response.text
                text = re.sub('<br>','',text)
                text = re.sub('<br/>','',text)
                text = re.sub('\r\n\t\t\t\t\t\t','',text)
                com = re.compile(
                    r'div class="cn">.*?<h1 title="(.*?)".*?<span class="lname">(.*?)</span>.*?<strong>(.*?)</strong>.*?<p class="cname">.*?title="(.*?)".*?</p>' \
                    '.*?<div class="t1">.*?<em class="i3"></em>(.*?)</span>.*?<em class="i4"></em>(\d+-\d+).*?</span>' \
                    '.*?<span class="label">.*?</span>(.*?)<div class="mt10">', re.S)  # 正则
                items = re.findall(com, text)
                for i in items:
                    return {
                        '标题': i[0],
                        '工作地点': i[1],
                        '薪酬': i[2],
                        '公司名': i[3],
                        '招聘人数': i[4],
                        '发布时间': i[5],
                        '职位描述': i[6].strip(),
                        '招聘链接': url
                    }
                else:
                    print '%s返回码错误' % url
    except HTTPError:
        pass

# def write_to_file(content):
#     with open('51jobs.txt', 'a', ) as f:
#         f.write(json.dumps(content, ensure_ascii=False) + '\n')  #关闭写入的文件中编码
#         f.close()

'''保存mongodb'''
def save_to_mongo(datail):
    if db['51jobs'].insert(datail):
        print('Successfully Saved to Mongo', datail)
        return True
    return False

def main():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '}
    urls = 'http://search.51job.com/list/030200%252C00,000000,0000,00,9,99,python,2,1.html'
    page = page_html(headers, urls)
    for i in range(1, int(page) + 1):
        urls = 'http://search.51job.com/list/030200%252C00,000000,0000,00,9,99,python,2,{}.html'.format(str(i))
        url = index_html(headers,urls)
        for url in url:
            data = page_one_page(url, headers)
            # write_to_file(data)
            if data:save_to_mongo(data)

if __name__ == '__main__':
    # pool = Pool()
    # pool.map(main, [i for i in range(1, 44)])
    # pool.close()
    # pool.join()
    main()


