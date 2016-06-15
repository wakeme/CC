# CloudComputing
## Runtime Environment
    * OS:       CentOS 7.1
    * JAVA:     Jdk-1.7.0_80
    * HADOOP:   Hadoop-2.7.2
    * SPARK:    Spark-1.6.1
    * SCALA:    Scala-2.11.8
<br>
## How to Run My Code
First, make sure you have `jdk, hadoop, spark, scala, hbase` on your computer<br>
Then, configure environment variables
### Homework1
copy the project to your computer<br>
enter the /src directory of the project<br>
Compile `PairsMapReduce.java` and create a jar:
```Bash
[src]$ hadoop com.sun.tools.javac.Main com/wakeme/homework1/PairsMapReduce.java 
[src]$ jar cf com.wakeme.homework1.pairs.jar com/wakeme/homework1/*.class
```
Run the application
```Bash
[src]$ hadoop jar com.wakeme.homework1.pairs.jar com.wakeme.homework1.PairsMapReduce input.txt hw1.out
```
