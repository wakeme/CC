# CloudComputing
## Runtime Environment
    * OS:       CentOS 7.1
    * JAVA:     jdk-1.7.0_80
    * HADOOP:   hadoop-2.7.2
    * SPARK:    spark-1.6.1
    * SCALA:    scala-2.10.6
    * HBASE:    hbase-1.1.5
    * PHP:      php
<br>
## How to Run My Code
First, make sure you have `jdk, hadoop, spark, scala, hbase` on your server<br>
Then, configure environment variables
### Homework1
`scp` the `/Homework1` folder to server<br>
Enter the `/src` directory of the project on the server<br>
Compile `Main.java` and create a jar:
```Bash
[src]$ hadoop com.sun.tools.javac.Main com/wakeme/homework1/Main.java 
[src]$ jar cf com.wakeme.homework1.jar com/wakeme/homework1/*.class
```
Run the application
```Bash
[src]$ hadoop jar com.wakeme.homework1.jar com.wakeme.homework1.Main input.txt h1.out
```
the output file is in the /h1.out directory
### Homework2
Open the project in Scala-IDE or Eclipse<br>
Add `hadoop` and `spark` jars into Build Path<br>
Export the project as JAR file<br>
`scp` JAR and input.txt to the server<br>
Submit Spark job
```Bash
spark-submit --class com.wakeme.homework2.Main --master local[4] /path/to/jar/com.wakeme.homework2.jar path/to/txt/input.txt h2.out
```
###  Homework3
Open the project in Eclipse<br>
Add `hbase` jars into Build Path<br>
Configure the server ip and port in `Homework3/src/com/wakeme/homework3/Main.java` from line 34-36<br>
Export the projects as a Runnable JAR file<br>
`scp` JAR file and h1.out to the server<br>
Run JAR file to save the results into HBase
```Bash
java -jar com.wakeme.homework3.jar
```
The web page gets data by `HBase REST API`, so make sure you've started HBase REST service by
```Bash
hbase rest start -p <port>
```
Edit the request url in line 12 of `Homework3/getCooc.php` to your HBase REST base url<br>
`scp` PHP file to the server<br>
Visit the `http://server_url/getCooc.php`<br>
Input one word or both words that you want to query
