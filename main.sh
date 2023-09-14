#!/bin/bash
# author：Kai Li
# bash xxx/train_ch_hd_model.sh ${taskid} ${media} ${train_sdate10} ${train_edate10} ${test_sdate10} ${test_edate10}

source /etc/profile

# input params
taskid=$1
media=$2
# select training data according to start date and end date
train_sdate10=$3
train_edate10=$4
# select testing data according to start date and end date
test_sdate10=$5
test_edate10=$6
echo ${taskid} ${media} ${train_sdate10} ${train_edate10} ${test_sdate10} ${test_edate10}

##############
# path
base_path=/***/
# oss tool used to transfer data
jar=${base_path}/oss-tool.jar
# log
log_file=${base_path}/log/${taskid}.log
train_file=${base_path}/input/${taskid}_train.txt
test_file=${base_path}/input/${taskid}_test.txt
# test result
testres_file=${base_path}/output/${taskid}_testres.txt
# pmml model
pmml_file=${base_path}/output/${taskid}.pmml
##############

# respond to server after the model training finished
response(){
    info=$1
    message=$2
    pmml_uri=$3
    source /data/anaconda3/bin/activate hd
    python3 ${base_path}/response.py ${taskid} ${info} ${message} ${pmml_uri} >> ${log_file}
}

# check for the presence of files
check_file_exists(){
    if [ -s $1 ];then
      echo $1" 文件存在且不空" >> ${log_file}
    else
      echo $1" 文件不存在或为空" >> ${log_file}
      response "ERROR" $2 "EMPTY"
      exit -1
    fi
}

get_input_data(){
    echo "start get input data" >> ${log_file}
    train_sql="
        select age_ori,degree_ori,ipdiff,hchdgap_ori,isapk
        from dac_twelve_dev.ch_complete_user_features
        where media='${media}'
        and ds between '$train_sdate10' and '$train_edate10'
    "
    echo ${train_sql} >> ${log_file}
    hive -S -e "${train_sql}" > ${train_file}
    check_file_exists ${train_file} "错误:训练文件不存在"

    test_sql="
        select user_id,age_ori,degree_ori,ipdiff,hchdgap_ori,isapk
        from dac_twelve_dev.ch_complete_user_features
        where media='${media}'
        and ds between '$test_sdate10' and '$test_edate10'
    "
    echo ${test_sql} >> ${log_file}
    hive -S -e "${test_sql}" > ${test_file}
    check_file_exists ${test_file} "错误:测试文件不存在"
    echo "get input data success" >> ${log_file}
}

train_model_get_testres(){
    echo "start train model" >> ${log_file}
    source /data/anaconda3/bin/activate hd
    if [ ${media} == 'dz_toutiao' ];then
        echo "开始运行dz_toutiao" >> ${log_file}
        python3 ${base_path}/train_model_dztt.py ${train_file} ${test_file} ${testres_file} ${pmml_file} ${taskid} >> ${log_file}
    else
        python3 ${base_path}/train_model.py ${train_file} ${test_file} ${testres_file} ${pmml_file} ${taskid} >> ${log_file}
    fi

    # 检查文件是否存在
    check_file_exists ${pmml_file} "错误:模型文件不存在"
    check_file_exists ${testres_file} "错误:测试结果文件不存在"
    echo "train model success" >> ${log_file}
}

load_testres_to_mysql(){

    table_name=ch_hd_model_testres
    MYSQL_USER="****"
    MYSQL_PASSWD="****"
    MYSQL_PORT="****"
    MYSQL_HOST="****"
    echo "start load testres data" >> ${log_file}
    sql="
    use ${MYSQL_USER};
    delete from ${table_name} where taskid='${taskid}';
    LOAD DATA LOCAL INFILE '${testres_file}' into table ${table_name}
    Character Set utf8
    fields terminated by ','
    OPTIONALLY ENCLOSED BY '\"' lines terminated by '\n'
    (
    user_id
    ,isapk
    ,callback_proba
    ,taskid
    );
    "
    echo "$sql"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWD -P$MYSQL_PORT -e "$sql"
    echo "load testres success" >> ${log_file}
}

write_testres_to_es(){
    echo "start write to es" >> ${log_file}
    source /data/anaconda3/bin/activate hd
    python3 ${base_path}/write_testres_to_es.py ${taskid} >> ${log_file}
    echo "write to es success" >> ${log_file}
}

upload_pmml2oss(){
    java -jar ${jar} "upload&uri" "***" ${pmml_file} "/hd_model/"${taskid}".pmml" "315360000" >> ${log_file}
    uri_line=`cat ${log_file} | grep "临时下载链接"`
    uri=${uri_line:7}
    response "SUCCESS" "SUCCESS" ${uri}

}

get_input_data
train_model_get_testres
load_testres_to_mysql
write_testres_to_es
upload_pmml2oss









