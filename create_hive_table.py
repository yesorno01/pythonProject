#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyhive import hive
import json, optparse
from pymysql import Connection
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def getddl(tablename, curs):
    tab_name = tablename.upper()
    curs.execute("select tab_id,tab_name,extend_items from dacp_meta_tab where tab_name=%s", tab_name)
    data1 = curs.fetchall()

    if len(data1) == 0:
        print;
        "无模型\n"
        exit(-1)

    if len(data1) > 1:
        print;
        "模型重复,请核查\n"
        exit(-1)

    curs.execute("select field_name,data_type,field_posi from dacp_meta_tab_field where tab_id=%s order by field_posi",
                 data1[0][0])
    data2 = curs.fetchall()

    dct1 = json.loads(data1[0][2])

    if data2:
        col = ',\n'.join(['%s %s' % (i[0], i[1]) for i in data2])
    else:
        col = "val string"

    para = {
        'tablename': data1[0][1],
        'separator': dct1['separator'],
        'row_separator': dct1['row_separator'],
        'storage_path': dct1['storage_path'],
        'col': col
    }

    templ = """
create external table ext.{tablename}(
{col}
)
ROW FORMAT DELIMITED
fields terminated by '{separator}'
lines terminated by '{row_separator}'  
STORED AS TEXTFILE
location '{storage_path}'
    """
    return templ.format(**para)


def chkTable(tablename, curs):
    try:
        curs.execute("select * from ext.%s limit 1" % tablename)
        return True
    except Exception as e:
        return False


if __name__ == '__main__':
    usage_desc = u"""

调用方式：

  python create_hive_table.py -t HNBSS_TD_B_ELEMENT_LIMIT

  基于模型开发填写的元数据构建hive外部表,外部表规划统一建在 ext 数据库下,存储目录在 /archive 目录下
"""

    opt = optparse.OptionParser(usage="usage: python %prog.py [options1] arg1" + usage_desc, version="%prog 0.1")
    opt.prog = 'create_hive_table'
    opt.add_option("-t", action="store", type="string", metavar='HNBSS_TD_B_ELEMENT_LIMIT', help=u"表名")
    options = opt.parse_args()[0]
    DictOpt = eval(str(options))

    if not (DictOpt['t']):
        print
        "参数缺失\n"
        opt.print_help()
        exit(1)

    conn = Connection(
        host='10.245.33.38',
        port=3306,
        user='prod_sjgj_dparam',
        password='pROD_sjgj_dparam1',
        db='prod_sjgj_dparam',
        charset='utf8')

    curs = conn.cursor()

    ddl = getddl(DictOpt['t'], curs)

    conn = hive.Connection(host="nn01.hadoop.unicom", port=10000, username="ocdp", password="ocdp123", auth="LDAP")
    curs = conn.cursor()
    if chkTable(DictOpt['t'], curs):
        print
        "表存在 %s\n" % DictOpt['t']
        exit(-1)

    print
    "--建表语句"
    print
    ddl
    curs.execute(ddl)
    print
    'OK: %s' % DictOpt['t']