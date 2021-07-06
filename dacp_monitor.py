#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: BigC
# 定时任务、http请求、数据库查询
import requests
import time
import hashlib
import paramiko
from pymysql import Connection


# send_message("17600101372", '【中国联通】尊敬的用户，您本次操作的验证码是：111111，有效时间5分钟。')
def send_message(phone, msg):
    user = '000065'
    tick = str(int(time.time()))
    m = hashlib.md5()
    m.update((user + tick + 'd3564X3em9S5').encode('utf-8'))
    key = m.hexdigest()[0:16]
    tid = '12324'
    coding = '0'
    params = {
        'user': user,
        'tick': tick,
        'key': key,
        "tid": tid,
        "m": phone,
        "c": msg,
        "coding": coding
    }

    response = requests.get("http://132.46.11.12:10002/send", params=params)
    return response.text


# 配置库里的dacp流程对应的进程个数（如果未主动配置，默认起1个进程）
def get_dacp_runcfg(curs):
    curs.execute("SELECT unit_id,process_num from dacp_datastash_unit_runcfg")
    runcfg = curs.fetchall()
    return dict(runcfg)


def get_hosts(curs):
    host_list = []
    curs.execute("SELECT broker_ip from  dacp_datastash_broker_cfg")
    hosts = curs.fetchall()
    for host in hosts:
        host_list.append(host[0])
    return host_list

# 获取每个dacp流程的后台实际进程数量
def query_unit_num(curs):
    result = {}
    # for item in open('dacp_all.conf'):
    for item in get_hosts(curs):
        hostitem = item.strip()
        username = 'dacp'
        passwd = 'Tsq83Yb!'
        port = 22
        # 执行命令
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostitem, port, username, passwd)
        query_cmd = 'ps -ef|grep dacp-ordercollect-streams.jar|grep -v grep|grep -v bash|awk "{print \$NF}"'
        stdin, stdout, stderr = client.exec_command(query_cmd)
        unit_ids = stdout.read().decode().split()
        for unit_id in unit_ids:
            if unit_id in result:
                result[unit_id] = result[unit_id] + 1
            else:
                result[unit_id] = 1
        client.close()
    return result


if __name__ == "__main__":
    while True:
        conn = Connection(
            host='10.245.33.38',
            port=3306,
            user='prod_sjgj_dparam',
            password='pROD_sjgj_dparam1',
            db='prod_sjgj_dparam',
            charset='utf8')

        curs = conn.cursor()
        hosts = get_hosts(curs)  # 获取dacp调度主机列表
        all_unit_runcfg = get_dacp_runcfg(curs)  # dacp配置库中配置的dacp流程运行个数
        unit_real_num = query_unit_num(curs)  # 每个流程后台实际运行的数量
        # 短信内容
        msg1 = []  # cfg < real
        msg2 = []  # cfg > real
        for unit_id in unit_real_num:
            unit_num = unit_real_num[unit_id]
            if unit_id in all_unit_runcfg:  # 参数配置配置了流程的进程数
                cfg_unit_num = all_unit_runcfg[unit_id]
                if unit_num - cfg_unit_num > 0:
                    # 后台实际进程数量 > 参数配置数量
                    # print('%s cfg: %s real: %s' % (unit_id, cfg_unit_num, unit_num))
                    # 发短信
                    msg1.append("%s:%s:%s" % (unit_id, cfg_unit_num, unit_num))
                elif unit_num - cfg_unit_num < 0:
                    # 后台实际进程数量 < 参数配置数量
                    # print('%s cfg: %s real: %s' % (unit_id, cfg_unit_num, unit_num))
                    # 发短信
                    msg2.append("%s:%s:%s" % (unit_id, cfg_unit_num, unit_num))
            else:  # 参数配置未配置流程的进程数，则默认流程进程数应该为1
                if unit_num > 1:
                    # 参数未配置进程数，后台进程数大于1
                    # print('%s cfg: %s real: %s' % (unit_id, 1, unit_num))
                    # 发短信
                    msg1.append("%s:%s:%s" % (unit_id, 1, unit_num))
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        print('-' * 25, '实际后台进程个数大于参数配置个数', '-' * 25)
        print(msg1)
        print('-' * 25, '实际后台进程个数小于参数配置个数', '-' * 25)
        print(msg2)

        msg1.extend(msg2)

        phoens = ['17600101372']
        send_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        del_num = 5
        for phone in phoens:
            if(len(msg1) >= del_num):
                if(len(msg1) % del_num == 0):
                    for i in range(len(msg1) // del_num):
                        message = msg1[i * del_num:i * del_num + del_num]
                        print(message)
                        res = send_message(phone, send_time+' '+','.join(message))
                        print(res)
                else:
                    for i in range(len(msg1) // del_num):
                        message = msg1[i * del_num:i * del_num + del_num]
                        print(message)
                        res = send_message(phone, send_time+' '+','.join(message))
                        print(res)

                    message = msg1[len(msg1) // del_num * del_num: len(msg1)]
                    print(message)
                    res = send_message(phone, send_time+' '+','.join(message))
                    print(res)
            else:
                res = send_message(phone, send_time+' '+','.join(message))
                print(res)
        time.sleep(3600)

