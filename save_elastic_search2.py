from elasticsearch import Elasticsearch
import json

es = Elasticsearch([{'host':'localhost','port':9200}])

e1={"file_name": "Placeholder1.wav","hash1": 103,
	"hash2": 290, "hash3": 308, "hash4": 3, "hash5": 0}
e2={"file_name": "Placeholder2.wav","hash1": 103,
        "hash2": 90, "hash3": 38, "hash4": 13, "hash5": 20}
e3={"file_name": "Placeholder3.wav","hash1": 13,
        "hash2": 29, "hash3": 3, "hash4": 93, "hash5": 200}

all_files=[(1,e1),(2,e2),(3,e3)]

for (index,file) in all_files:
	es.index(index='insight',doc_type='wavHashes',id=index,body=file)

print(es.get(index='insight',doc_type='wavHashes',id=2)['_source']['file_name'])

def get_any_matches(hash1,hash2,hash3,hash4,hash5):
	result1=es.search(index='insight',
		body={'query':{'match':{'hash1': hash1}}})['hits']['hits']
	result1=map(lambda entry:entry['_source']['file_name'],result1)

	result2=es.search(index='insight',
                body={'query':{'match':{'hash2': hash2}}})['hits']['hits']
        result2=map(lambda entry:entry['_source']['file_name'],result2)

	result3=es.search(index='insight',
                body={'query':{'match':{'hash3': hash3}}})['hits']['hits']
        result3=map(lambda entry:entry['_source']['file_name'],result3)

	result4=es.search(index='insight',
                body={'query':{'match':{'hash4': hash4}}})['hits']['hits']
        result4=map(lambda entry:entry['_source']['file_name'],result4)

	result5=es.search(index='insight',
                body={'query':{'match':{'hash5': hash5}}})['hits']['hits']
        result5=map(lambda entry:entry['_source']['file_name'],result5)
	
	return set(result1+result2+result3+result4+result5)

z=get_any_matches(103,0,0,0,0)
print(z)
