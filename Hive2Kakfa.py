#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyhive import hive
import os,itertools,json,datetime,optparse
from pykafka import KafkaClient
from datetime import date,datetime
import time,pykafka
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

kafka_config = {
    "prd" :"dn49.hadoop.unicom:6667,dn50.hadoop.unicom:6667,dn51.hadoop.unicom:6667,dn54.hadoop.unicom:6667,dn55.hadoop.unicom:6667,dn56.hadoop.unicom:6667",
    "test":"dn31.hadoop.unicom:6667,dn32.hadoop.unicom:6667,dn33.hadoop.unicom:6667",
    "dev1":"dn57.hadoop.unicom:6667,dn57.hadoop.unicom:6668,dn57.hadoop.unicom:6669",
    "dev2":"dn58.hadoop.unicom:6667,dn58.hadoop.unicom:6668,dn58.hadoop.unicom:6669"
}

def main(source,sql,topic,partitionkey):
    client = KafkaClient(hosts=kafka_config[source])
    topic = client.topics[topic]
    p = topic.get_producer(use_rdkafka = True)

    conn = hive.Connection(host="nn01.hadoop.unicom", port=10000, username="ocdp", password="ocdp123", auth="LDAP")
    curs = conn.cursor()
    curs.execute(sql)
    column_names = [d[0].split('.')[-1].upper() for d in curs.description]

    table_name = dict(itertools.izip(column_names,curs.fetchone())).get('TABLE_NAME__')
    if table_name==None :
        table_name=''
    curs.execute(sql)

    i = 0
    d1 = datetime.now()
    for row in curs:
        if i==5000000:
            d2 = datetime.now()
            s = (d2-d1).seconds
            i = 0
            d1 = datetime.now()
            print("%s seconds:%s rows:5000000\n" % (table_name,s))

        row=[ None if j == u'null' else j for j in row]
        msg = dict(itertools.izip(column_names,row))
        msg['OPERATION_COUNT__']=0
        err_cnt = 0

        while True:
            try:
                data = json.dumps(msg,ensure_ascii=False)
                if partitionkey=="*":
                    p.produce(str(data))
                else:
                    p.produce(str(data), partition_key = str(msg[partitionkey]))
                break
            except pykafka.exceptions.ProducerQueueFullError,e:
                time.sleep(0.1)
                err_cnt += 1
                if err_cnt==20:
                    print("异常 %s %s %s\n" % (source, topic,table_name))
                    break
            except KeyError,e:
                print("KeyError,%s", e)
                exit(-1)
        i += 1

    d2 = datetime.now()
    s = (d2-d1).seconds
    print "%s seconds:%s rows:%s\n" % (table_name,s,i)
    p.stop()

if __name__ == '__main__':
    usage_desc = u"""

调用方式：

  python Hive2Kakfa.py -k prd -s "select * from cbss_tf_b_trade limit 100" -p TRADE_ID -t cbss.order.main.json

  数据源说明:
  prd = %s

  test = %s

  dev1 = %s

  dev2 = %s
""" % (kafka_config["prd"],kafka_config["test"],kafka_config["dev1"],kafka_config["dev2"])

    opt = optparse.OptionParser(usage="usage: python %prog.py [options1] arg1 [options2] arg2 [options3] arg3" + usage_desc ,version="%prog 0.1")
    opt.prog = 'Hive2Kakfa'
    opt.add_option("-k" ,action="store" ,type="string" ,metavar='prd' ,help=u"数据源")
    opt.add_option("-s" ,action="store" ,type="string" ,metavar='select * from cbss_tf_b_trade' ,help=u"SQL")
    opt.add_option("-p" ,action="store" ,type="string" ,metavar='TRADE_ID'                          ,help=u"分区字段(可选)")
    opt.add_option("-t" ,action="store" ,type="string" ,metavar='cbss.order.main.json'              ,help=u"topic名称")
    options = opt.parse_args()[0]
    DictOpt = eval(str(options))

    if not(DictOpt['s'] or DictOpt['t']):
        print "参数缺失\n"
        opt.print_help()
        exit(1)

    if DictOpt['p'] == None: DictOpt['p'] = "*"

    main(
        source = DictOpt['k'],
        sql = DictOpt['s'],
        topic = DictOpt['t'],
        partitionkey = DictOpt['p'])