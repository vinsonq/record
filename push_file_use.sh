#!/usr/bin/env bash

rcvOutputSuccess=$1
day=`echo $rcvOutputSuccess| cut -d "/" -f 10`
hour=`echo $rcvOutputSuccess| cut -d "/" -f 11`
minute=`echo $rcvOutputSuccess| cut -d "/" -f 12`
ifs=/ifs/anti-fraud/rcv_data/pv/success
s3=hdfs://wuxihdp/user/fraud_data/anti-fraud/rcv/pv/success

echo $day $hour $minute

hdfs dfs -test -e $s3/$day/$hour/$minute/_SUCCESS
if [ $? -eq 0 ]; then
    rm -rf $ifs/$day/$hour/$minute
    mkdir -p $ifs/$day/$hour/$minute/
    hadoop fs -copyToLocal $s3/$day/$hour/$minute/ $ifs/$day/$hour/
    cp $ifs/$day/$hour/$minute/_SUCCESS $ifs/$day/$hour/$minute/_SUCCESS.done
fi
