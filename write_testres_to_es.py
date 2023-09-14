from es import Es
from mysql import Mysql
import re
import sys


def parse():
    task_id = str(sys.argv[1])
    return task_id

def connect_mysql():
    MYSQL_USER = "***"
    MYSQL_PASSWD = "***"
    MYSQL_HOST = "***"
    MYSQL_DB="***"
    ms = Mysql(host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWD, database=MYSQL_DB)
    return ms

def query_mysql(taskid,ms):

    sql="select a.user_id,media,bg,age,age_ori,degree,degree_ori,xsdzcr,experience,ipdiff,hchdgap,hchdgap_ori,isapk,ds,taskid,callback_proba \
    from( select user_id,isapk,callback_proba,taskid from ch_hd_model_testres where taskid='" + taskid + \
    "') a join( select user_id,media,bg,age,age_ori,degree,degree_ori,xsdzcr,experience,ipdiff,hchdgap,hchdgap_ori,ds from ch_complete_user_features ) b on a.user_id = b.user_id;"
    fields, data = ms.query(sql)
    ms.close()
    return fields, data


def build_data_4es(fields, data):
    # 配合工程,需要转换的字段
    field_dic = {"taskid":"task_id"}
    #列名
    column_list = []
    for field in fields:
        column_list.append(field_dic.get(field[0], field[0]))
    #具体行数据
    es_list = []
    for row in data:
        result = {}
        for i in range(len(column_list)):
            result[column_list[i]] = str(row[i])
        es_list.append(result)
    return es_list

def write2es(es_list):
    actions =[]
    es = Es("***","***")
    for elem in es_list:
        action = {
            "_index": "nlp_test_data",   ##表名
            "_id": str(elem["user_id"])+"_"+str(elem["task_id"]),  # 将id作为索引的唯一标志
            "_source": elem
        }
        actions.append(action)
    es.write_bunch(actions,index="nlp_test_data")





if __name__ == "__main__":
    # 获取task_id
    task_id=parse()
    # 连接mysql
    ms = connect_mysql()
    # 查询mysql数据
    fields, data = query_mysql(task_id,ms)
    # 构造es格式的数据
    es_list = build_data_4es(fields, data)
    # 数据打入ES
    write2es(es_list)


