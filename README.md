Credit Links
-----
http://wiki.github.com/brianfrankcooper/YCSB/  
https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/
ycsb-users@yahoogroups.com  

Building from source
--------------------

To build the full distribution, with all database bindings:

    mvn clean package -DskipTests

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package -DskipTests -Dcheckstyle.skip 
    
Memcached
--------------------

Ensure memcached is running before running GeoYCSB.

    memcached -m 1024

Executing Benchmark
--------------------

MongoDB (Microbenchmark without output into a txt file)

./bin/ycsb load mongodb -P workloads/geo/workloadga -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true" > outputLoad.txt

./bin/ycsb run mongodb -P workloads/geo/workloadga -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true" > outputRun.txt

MongoDB (Macrobenchmark)

./bin/ycsb load mongodb -P workloads/geo/workloadgm1 -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true"

./bin/ycsb run mongodb -P workloads/geo/workloadgm1 -p mongodb.url="mongodb://localhost:27017/grafittiDB?w=1" -p mongodb.auth="true"
    
Yahoo! Cloud System Benchmark (YCSB)
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)
