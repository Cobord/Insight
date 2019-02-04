from pyspark import SparkContext, SparkConf

#from pyspark.sql import SQLContext,createDataFrame
#from pyspark.sql.functions import lit

import json
import hashlib

def addID(dict):
	my_json=json.dumps(dict)
	j=my_json.encode('ascii','ignore')
	dict['extra_id']=hashlib.sha224(j).hexdigest()
	return dict

def addID_json(dict):
	my_json=json.dumps(dict)
	j=my_json.encode('ascii','ignore')
	dict['extra_id']=hashlib.sha224(j)
	return (dict['extra_id'],my_json)

def parse(tuple):
	filename,spectra,hashes=tuple
	d={}
	d['filename']=filename
	#d['spectrum']=spectra
	d['hash1']=hashes[0]
	d['hash2']=hashes[1]
	d['hash3']=hashes[2]
	d['hash4']=hashes[3]
	d['hash5']=hashes[4]
	return d

def parse_exp(tuple):
	filename,spectra,hashes=tuple
	return ('key',{'field0': 'filename', 'val': filename,
		'hash1': hashes[0], 'hash2' : hashes[1], 'hash3': hashes[2],
		'hash4': hashes[3], 'hash5': hashes[4]})

def remove_id(doc):
	doc.pop('_id','')
	return doc

def format_json_exp(dict):
	return (dict['filename'],json.dumps(dict))

def format_data_example(dict):
	return (dict['doc-id'],json.dumps(dict))

conf = SparkConf().setAppName("update_sound_data")
sc = SparkContext(conf=conf)
#sc.setLogLevel("INFO")
#spark=SQLContext(sc)

data1 = [('file1',[1,2,3,3,5],[0,1,0,0,0]),('file2',[1,0,-1,0,0],[0,0,0,0,0])]
data2 = [('file3',[1,2,3,3,5],[0,1,0,0,0]),('file4',[1,0,-1,0,0],[0,0,0,0,0])]

data3 = [{'some-key': 'some-value', 'doc-id': 123},
	{'some-key': 'some-value', 'doc-id': 456},
	{'some-key': 'some-value', 'doc-id': 789}]

rdd = sc.parallelize(data3).map(lambda x: format_data_example(x))

#df=createDataFrame(rdd.map(addID))
#rdd=rdd.map(parse).map(remove_id).map(json.dumps).map(lambda x: ('key',x))
#rdd=rdd.map(parse).map(format_json_exp)

es_write_conf={}
es_write_conf["es.nodes"]='localhost'
es_write_conf["es.port"]='9200'
es_write_conf["es.resource"]='es_data/testing'
es_write_conf["es.input.json"]="yes"
es_write_conf["es.mapping.id"]="doc-id"
#es_write_conf["es.mapping.id"]="extra_id"
#es_write_conf["es.write.operation"]='upsert'

#df.write.format("org.elasticsearch.spark.sql").options(**es_write_conf).mode("append").save("es_data/test_elastic_search")

rdd.saveAsNewAPIHadoopFile(
	path='-',
	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
	keyClass="org.apache.hadoop.io.NullWritable",
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
	conf=es_write_conf)

#rdd=sc.parallelize(data2)
#rdd = rdd.map(parse).map(addID_json)
