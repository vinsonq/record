#!/usr/bin/env bash

date=$1
hour=$2
minute=$3

if [ -z ${date} ]; then
        date=`date -d"-0 hours" +"%Y%m%d"`
fi

if [ -z ${hour} ]; then
        hour=`date -d"-0 hours" +"%H"`
fi

if [ -z ${minute} ]; then
        minute=`date -d"-0 hours" +"%M"`
fi

a_1=$((10#$minute/5))
a_2=$((10#$minute%5))

if [ $a_2 -ne 0 ];then
  minute=$(($a_1*5))
fi

length=${#minute}
if [ $length -lt 2 ];then
  minute=0${minute}
fi
echo $date $hour $minute

s3=hdfs://wuxihdp/user/fraud_data/anti-fraud/rcv/pv/success
ifs=/ifs/anti-fraud/rcv_data/pv
hadoop fs -test -e $s3/${date}/${hour}/${minute}/_SUCCESS
if [ $? -eq 0 ]; then
    s3_num=`hadoop fs -ls $s3/${date}/${hour}/${minute}/ | wc -l`
    ifs_num=`ls -l ${ifs}/success/${date}/${hour}/${minute}/ | wc -l`
 if [ $s3_num -gt $ifs_num ];then
        mkdir -p ${ifs}/success/${date}/${hour}/
        hadoop fs -copyToLocal $s3/${date}/${hour}/${minute}/ ${ifs}/success/${date}/${hour}/
        echo 'success' $s3/${date}/${hour}/${minute}/ ${ifs}/success/${date}/${hour}/
    
fi
fi
