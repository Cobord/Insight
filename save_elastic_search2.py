from elasticsearch import Elasticsearch

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

print(es.get(index='insight',doc_type='wavHashes',id=2)['_source'])


