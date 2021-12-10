clear
mvn package -DskipTests
cd target
java -jar sqlancer-*.jar --random-seed 10 sparksql --oracle NOREC