#!/usr/bin/zsh
clear
mvn package -DskipTests
cd target
../../spark-v3.0.3/sbin/start-thriftserver.sh
sleep 10s
gtimeout 1m java -jar sqlancer-*.jar --random-seed 10 --num-threads 1 sparksql --oracle DIFFT
../../spark-v3.0.3/sbin/stop-thriftserver.sh
# 'find res -type f -print0 | xargs -0 mv -t res/spark-v3.0.3' in Linux
mv res/sparksql/*(.) res/spark-v3.0.3
../../spark-v3.2.0/sbin/start-thriftserver.sh
sleep 10s
gtimeout 1m java -jar sqlancer-*.jar --random-seed 10 --num-threads 1 sparksql --oracle DIFFT
../../spark-v3.2.0/sbin/stop-thriftserver.sh
# 'find res -type f -print0 | xargs -0 mv -t res/spark-v3.2.0' in Linux
mv res/sparksql/*(.) res/spark-v3.2.0
python3 ../src/sqlancer/sparksql/oracle/SparkSQLDiffT.py