#!/usr/bin/env bash
date "+%Y-%m-%d %H:%M:%S"
ifs=/ifs/anti-fraud/rcv_data/pv
day_path=`hadoop fs -ls hdfs://wuxihdp/user/fraud_data/anti-fraud/rcv/pv/success/|sort -k6,7|tail -n 1| awk '{print $8}'`
hour_path=`hadoop fs -ls $day_path|sort -k6,7|tail -n 1| awk '{print $8}'`
minute_path=`hadoop fs -ls $hour_path|sort -k6,7|tail -n 1| awk '{print $8}'`
echo $minute_path
day=`echo $day_path| cut -d "/" -f 10`
hour=`echo $hour_path| cut -d "/" -f 11`
minute=`echo $minute_path| cut -d "/" -f 12`
COUNTER=0
while [ $COUNTER -lt 2 ]
do
    hadoop fs -test -e $minute_path/_SUCCESS
    if [ $? -eq 0 ]; then
        echo enter
	    s3_num=`hadoop fs -ls $minute_path | wc -l`
        ifs_num=`ls -l ${ifs}/success/${day}/${hour}/${minute}/ | wc -l`
        if [ $s3_num -gt $ifs_num ];then
            mkdir -p ${ifs}/success/${day}/${hour}/${minute}/
            hadoop fs -copyToLocal $minute_path/ ${ifs}/success/${day}/${hour}/
            cp ${ifs}/success/${day}/${hour}/${minute}/_SUCCESS ${ifs}/success/${day}/${hour}/${minute}/_SUCCESS.done
            echo 'success' $minute_path/ ${ifs}/success/${day}/${hour}/
            break
	else
	    echo 'ifs has exists.'
	    break
        fi
    fi
    echo 'while wait' $minute_path/ ${ifs}/success/${day}/${hour}/ 'counter:'$COUNTER
    sleep 30s
    let COUNTER+=1
done
