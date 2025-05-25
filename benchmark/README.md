
mvn clean generate-sources;

mvn clean verify && java -jar target/benchmarks.jar

java -jar target/benchmarks.jar PaxosBenchmark.paxosProtobuf -f 1 -wi 1 -i 1
java -jar target/benchmarks.jar PaxosBenchmark
