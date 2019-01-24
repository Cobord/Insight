from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setMaster("local")
        .setAppName("word count")
        .set("spark.executor.memory", "1g"))
sc=SparkContext(conf=conf)

text = sc.textFile("alice.txt")
counts=text.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a,b: a+b)
answer=counts.collect()
print(answer)
