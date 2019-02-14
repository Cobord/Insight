import subprocess

for i in range(10):
	subprocess.call(["hdfs","dfs","-rm","-r","/user/output"+str(i)])

subprocess.call(["rm","try_hashes2.txt"])

subprocess.call(["$SPARK_HOME/bin/spark-submit","--master","spark://ip-10-0-0-9:7077","--jars","$SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar","wav_to_hash5.py"],shell=True)

for i in range(10):
	xs=subprocess.check_output(["hdfs","dfs","-ls","/user/output"+str(i)]).split(" ")
	xs=filter(lambda y: 'part' in y,xs)
	for x in xs:
		subprocess.call(["hdfs","dfs","-cat",x,">>","try_hashes2.txt"])

subprocess.call(["$SPARK_HOME/bin/spark-submit","--master",
                "spark://ip-10-0-0-9:7077","--jars",
                "$SPARK_HOME/jars/elasticsearch-hadoop-2.1.0.Beta2.jar",
                "save_elastic_search2.py"],shell=True)

for i in range(10):
        subprocess.call(["hdfs","dfs","-rm","-r","/user/output"+str(i)])

subprocess.call(["rm","try_hashes2.txt"])
subprocess.call(["rm","*.wav"])
