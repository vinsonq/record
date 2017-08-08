#!/usr/bin/env bash
dt=`date -d "1 weeks ago" +%Y%m%d`
success_path=/ifs/anti-fraud/rcv_data/pv/success/
cd $success_path
ls -l | awk -v t=$dt '$9 < t{print $0}' | xargs rm -rf
fail_path=/ifs/anti-fraud/pre_deal/classify/other/pv_join_failed/
cd $fail_path
ls -l | awk -v t=$dt '$9 < t{print $0}'| xargs rm -rf
