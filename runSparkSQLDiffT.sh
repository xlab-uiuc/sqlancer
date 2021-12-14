#!/usr/bin/zsh
clear
mvn package -DskipTests
cd target
for i in {1..10}
do
    find ./res/spark-v3.0.3 -maxdepth 1 -type f -delete
    find ./res/spark-v3.2.0 -maxdepth 1 -type f -delete
    ../../spark-v3.0.3/sbin/start-thriftserver.sh
    sleep 10s
    gtimeout 10m java -jar sqlancer-*.jar --random-seed $i --num-threads 1 sparksql --oracle DIFFT
    ../../spark-v3.0.3/sbin/stop-thriftserver.sh
    mv ./res/*-cur.log ./res/spark-v3.0.3
    ../../spark-v3.2.0/sbin/start-thriftserver.sh
    sleep 10s
    gtimeout 10m java -jar sqlancer-*.jar --random-seed $i --num-threads 1 sparksql --oracle DIFFT
    ../../spark-v3.2.0/sbin/stop-thriftserver.sh
    mv ./res/*-cur.log ./res/spark-v3.2.0
    python3 ../src/sqlancer/sparksql/oracle/SparkSQLDiffT.py
done