from elasticsearch import Elasticsearch
import json

es = Elasticsearch([{'host':'localhost','port':9200}])

def make_es(from_file):
	with open(from_file) as f:
		for line in f.readlines():
			[hash1,hash2,hash3,hash4,hash5,filename]=line.split(';')
			hash1=int(hash1)
			hash2=int(hash2)
			hash3=int(hash3)
			hash4=int(hash4)
			hash5=int(hash5)
			filename=filename.rstrip()
			e={"file_name":filename,"hash1":hash1,
				"hash2":hash2,"hash3":hash3,"hash4":hash4,
				"hash5":hash5}
			es.index(index='insight',doc_type='wavHashes',id=hash(filename),body=e)

if __name__=="__main__":
	make_es('try_hashes2.txt')

# provide 5 hashes, returns those file_name's such that
# their hash1 matches the provided hash1 or for hash2 etc
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

# a version that can generalize to other numbers of times hashed
# besides 5
def get_any_matches2(my_hashes):
	all_candidates=set([])
	for i in range(len(my_hashes)):
		cur_batch=es.search(index='insight',
			body={'query':{'match':{'hash'+str(i+1):my_hashes[i]}}})['hits']['hits']
		cur_batch=map(lambda entry:entry['_source']['file_name'],cur_batch)
		all_candidates.update(cur_batch)
	return all_candidates
