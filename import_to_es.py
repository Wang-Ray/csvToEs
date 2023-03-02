# encoding: utf-8
####################################################  python源码【import_to_es.py】 - start ###############################################################
# 功能实现，将cvs、或者json数据导入到ES，这里需要执行ID所在列
# 如何使用, `python import_to_es.py`，用之前，先改一下相关信息
#
# 在线安装es依赖
# pip install elasticsearch==7.10.1
# pip install pandas
import json
 
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
 
 
#############################################################  需要修改的参数 - start ############################################################################
 
 
# 安全认证需要加上http_auth=('elastic', 'elastic')，详细参考：ES集群与安全
# 创建es对象，这里注意修改ES连接
es = Elasticsearch([{"host": "192.168.0.22", "port": 9200}, {"host": "192.168.0.22", "port": 9201}, {"host": "192.168.0.22", "port": 9202}], timeout=60)
# 将数据导入这个索引
index = "demo_index"
# 这是数据文件路径, *.csv或者*.json
data_file = "/home/ray/Documents/WeChat Files/AngiWANG/FileStorage/File/2023-03/mall_home_search(2).csv"
# data_file = "C:\\Users\\Administrator\\Desktop\\temp\\查询商城主站搜索索引格式产品(2).json"
# id所在列
id_field = "productId"
# 批量提交条数
bulk_size = 1000
###############################################################  需要修改的参数 - end #####################################################################
 
 
# 获取文件格式, 直接截取文件名
# return, csv或者json
def get_file_fmt(filename):
    return filename[filename.rindex('.') + 1:len(filename)]
 
 
# 读取csv文件，返回列表, 忽略空字符串
def read_csv(filename):
    df = pd.read_csv(filename, encoding="UTF-8", sep=",", keep_default_na=False, low_memory=False)
    # 列名
    keys = df.keys()
    data = []
    # 多少列
    length = len(keys)
    for index, row in df.iterrows():
        dist = {}
        for i in range(length):
            value = row[keys[i]]
            if type(value) == 'str':
                if value.strip() != '':
                    dist[keys[i]] = value
            else:
                dist[keys[i]] = value
        # TODO 这里需要删除
        # if index == 10:
        #     break
 
        data.append(dist)
    return data
 
 
# 读取json文件
def read_json(filename):
    file = open(filename, encoding="UTF-8")
    lines = file.read().splitlines()
    print(lines)
    json_list = []
    for line in lines:
        json_list.append(json.loads(line))
    return json_list
 
 
# main，读取文件并批量导入es
if __name__ == '__main__':
    fmt = get_file_fmt(data_file)
    print("file format: " + fmt)
    data_list = []
    if "csv" == fmt:
        print("import csv file")
        data_list = read_csv(data_file)
    elif "json" == fmt:
        print("import json file")
        data_list = read_json(data_file)
    else:
        print("未知文件格式: " + fmt)
 
    page = 1
    # 批量导入
    actions = []
    for item in data_list:
        action = {
            "_index": index,
            "_id": item[id_field],
            "_source": item
        }
        actions.append(action)
        if len(actions) >= bulk_size:
            bulk_response = helpers.bulk(es, actions)
            print(bulk_response)
            print("current page: " + str(page))
            page += 1
            actions.clear()
 
    if len(actions) > 0:
        bulk_response = helpers.bulk(es, actions)
        print(bulk_response)
        print("current page: " + str(page))
 
    print("done")
############################################################  python源码【import_to_es.py】 - end ################################################################################
