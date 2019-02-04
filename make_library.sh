hdfs dfs -rm -r /user/output0
hdfs dfs -rm -r /user/output1
hdfs dfs -rm -r /user/output2
hdfs dfs -rm -r /user/output3
hdfs dfs -rm -r /user/output4
hdfs dfs -rm -r /user/output5
hdfs dfs -rm -r /user/output6
hdfs dfs -rm -r /user/output7
hdfs dfs -rm -r /user/output8
hdfs dfs -rm -r /user/output9
$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar wav_to_hash3.py
$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar save_elastic_search2.py


