!curl -o taxi_zone_lookup.csv https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

# GIT BASH
export JAVA_HOME="/c/tools/jdk-11.0.21"
export PATH="$JAVA_HOME/bin:$PATH"

export HADOOP_HOME="/c/tools/hadoop-3.2.0"
export PATH="$HADOOP_HOME/bin:$PATH"

export SPARK_HOME="/c/tools/spark-3.3.2-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$PATH"

# GIT CMD
set JAVA_HOME=C:\tools\jdk-11.0.19
set PATH=%JAVA_HOME%\bin;%PATH%

set HADOOP_HOME=C:\tools\hadoop-3.2.0
set PATH=%HADOOP_HOME%\bin;%PATH%

set SPARK_HOME=C:\tools\spark-3.3.2-bin-hadoop3
set PATH=%SPARK_HOME%\bin;%PATH%

call %SPARK_HOME%\bin\spark-shell.cmd

pip install pandas==2.0.1

pip install pyspark==3.5.0

# CONNECTING MASTER AND WORKER LOCALLY WINDOWs
cd %SPARK_HOME%
bin\spark-class2.cmd org.apache.spark.deploy.master.Master

http://localhost:8080/

cd %SPARK_HOME%
bin\spark-class2.cmd org.apache.spark.deploy.worker.Worker -c 1 -m 4G spark://YOUR_IP_ADDRESS:7077

