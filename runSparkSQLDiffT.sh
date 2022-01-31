#!/usr/bin/zsh
clear
mvn package -DskipTests
python3 hive_conf.py
cd target
for i in {1..100}
do
    j=$RANDOM
    s1=$(($RANDOM % 3))
    s2=$(( ($s1 + $RANDOM % 2) % 3))
    find ./res/spark0 -maxdepth 1 -type f -delete
    find ./res/spark1 -maxdepth 1 -type f -delete
    ../../spark0/sbin/start-thriftserver.sh
    sleep 10s
    timeout --foreground 6h java -jar sqlancer-*.jar --random-seed $j --num-threads 1 sparksql --oracle DIFFT --store-type $s1
    ../../spark0/sbin/stop-thriftserver.sh
    mv ./res/*-cur.log ./res/spark0
    ../../spark1/sbin/start-thriftserver.sh
    sleep 10s
    timeout --foreground 6h java -jar sqlancer-*.jar --random-seed $j --num-threads 1 sparksql --oracle DIFFT --store-type $s2
    ../../spark1/sbin/stop-thriftserver.sh
    mv ./res/*-cur.log ./res/spark1
    python3 ../src/sqlancer/sparksql/oracle/SparkSQLDiffT.py
done
