#!/bin/bash

dis=1
sel=3
n=60000
epsilon=100
rho=10
alpha=1.5
beta=10.859
pred=$1
pred_idx=$2
win_size=$3
win_size_t=$4
win_int=$5
warm=100000
run=400000
LR=0.0001	#$6
NumIter=1000	#$7
RegParam=0.0	#$8
BatchFr=1.0		#$9
IP="192.168.0.10"
file_name="ucr_input.csv"

if [ ${dis} -eq 1 ];then
	class="kvscala.scalatest"
elif [ ${dis} -eq 2 ];then
	class="kvscala.ML_test2"
elif [ ${dis} -eq 3 ];then
	class="kvscala.ML_test1"
elif [ ${dis} -eq 9 ];then
        class="kvmatch.Fileread"
elif [ ${dis} -eq 0 ];then
        class="kvmatch.Targetread"
else
	class=${dis}
fi

#sh run.sh pred pred_idx win_size win_size_t win_int warm run

master="spark://dsl-desktop5003:7077"	#for spark standalone

python3 stream.py &
../../spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class $class --master $master KV-match-1.0-SNAPSHOT-jar-with-dependencies.jar $sel $n $epsilon $rho $alpha $beta $pred $pred_idx $win_size $win_size_t $win_int $warm $run $LR $NumIter $RegParam $BatchFr $IP $file_name >>after_revision_13
echo $class $sel $n $epsilon $rho $alpha $beta $pred $win_size $win_size_t $win_int $LR $NumIter $RegParam $BatchFr $file_name >> after_revision_13
#cat lo2
