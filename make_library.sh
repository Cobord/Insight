#!/bin/sh
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

# replace with loop so that can scale up

rm try_hashes2.txt

$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar wav_to_hash6.py

hdfs dfs -cat /user/output0/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output1/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output2/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output3/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output4/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output5/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output6/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output7/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output8/part-00000 >> try_hashes2.txt
hdfs dfs -cat /user/output9/part-00000 >> try_hashes2.txt

# replace with loop to make sure scales

$SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-9:7077 --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar save_elastic_search2.py

#hdfs dfs -rm -r /user/output0
#hdfs dfs -rm -r /user/output1
#hdfs dfs -rm -r /user/output2
#hdfs dfs -rm -r /user/output3
#hdfs dfs -rm -r /user/output4
#hdfs dfs -rm -r /user/output5
#hdfs dfs -rm -r /user/output6
#hdfs dfs -rm -r /user/output7
#hdfs dfs -rm -r /user/output8
#hdfs dfs -rm -r /user/output9
#rm try_hashes2.txt
#rm *.wav

