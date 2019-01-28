from scipy.fftpack import fft
from scipy.signal import decimate
from scipy.io import wavfile
import math
import numpy as np
import glob

from pyspark import SparkConf, SparkContext
from pyspark.mllib.random import RandomRDDs

conf = (SparkConf()
        .setMaster("local")
        .setAppName("spectra")
        .set("spark.executor.memory","1g"))
sc = SparkContext(conf=conf)

interval_time=30
samp_freq=44100
down_factor=5

# extends periodically so it is the right length len_needed
def periodize(signal,len_needed):
	current_len=len(signal)
	to_expand=int(math.ceil(len_needed/current_len))
	signal_expanded=np.tile(signal,to_expand*2)
	signal_expanded=signal_expanded[:len_needed]
	return signal_expanded

# read from filename, make desired length
def almost_raw(filename):
	len_needed=interval_time*samp_freq
	raw_data=np.zeros(len_needed)
	try:
		freqs, raw_data=wavfile.read(filename)
	except:
		return np.zeros(26)
	if (raw_data.ndim==2):
		raw_data=raw_data.T[0]
	raw_data=periodize(raw_data.flatten(),len_needed)
	return raw_data

# downsampling step
def downsampling(raw_data):
	return decimate(raw_data,down_factor)

# from filename return (filename,spectrum)
# spectrum is given as all the real parts followed by all the imaginary parts
def to_spectrum(modified_data):
	fourier=fft(modified_data)
	length=int(len(fourier)/2)
	fourier=fourier[:length]
	fourier=fourier*[2**(-i*i/(length*length)) for i in range(length)]
	return np.append(np.real(fourier),np.imag(fourier))

def to_lhs(spectra,all_hyperplanes,num_hp_per_arr):
	return hash_point(spectra,all_hyperplanes,num_hp_per_arr)

def hash_point(point,all_hyperplanes,num_hp_per_arr):
	needed_length=all_hyperplanes.shape[1]
	cur_length=point.shape[0]
	if (cur_length>needed_length):
		point=point[:needed_length]
	else:
		all_hyperplanes=all_hyperplanes[:][:cur_length]
	pre_hashed=np.dot(all_hyperplanes,point)
	pre_hashed=pre_hashed.flatten().T
	unsimplified=list(map(lambda x: int(x>=0), pre_hashed))
	return simplify_hash_point(unsimplified,num_hp_per_arr)
	#return pre_hashed.shape

def simplify_hash_point(int_list,num_hp_per_arrangement):
	chunked_list=[int_list[i:i+num_hp_per_arrangement] for i in range(0,len(int_list),num_hp_per_arrangement)]
	return list(map(int_list_to_num,chunked_list))

def int_list_to_num(int_list):
	return np.array([int_list[i]<<i for i in range(len(int_list))]).sum()

def construct_hyperplanes(num_hp_arrangements,num_hp_per_arrangement,ambient_dimension):
	num_hps=num_hp_arrangements*num_hp_per_arrangement
	all_hp_rdd = RandomRDDs.normalVectorRDD(sc,num_hps,ambient_dimension)
	return np.matrix(all_hp_rdd.collect())

input_files_prefix="wavFiles/"
input_file_list="wavFilesList2.txt"
output_files_dest="hdfs://ec2-52-0-185-8.compute-1.amazonaws.com:9000/user/output"

result=(sc.textFile(input_file_list)
	.map(lambda name:input_files_prefix+name)
	.map(lambda file: (file,almost_raw(file)))
	.filter(lambda (file,res): len(res)>27)
	.map(lambda (file,res): (file,downsampling(res)) )
	.map(lambda (file,res): (file,to_spectrum(res)) )
	)
num_points=result.count()
num_hp_per_arrangement=int(np.log2(num_points))
num_hp_arrangements=5
ambient_dimension=interval_time*samp_freq/down_factor
all_hyperplanes=construct_hyperplanes(num_hp_arrangements,num_hp_per_arrangement,ambient_dimension)

result=result.map(lambda (file,res): (file,to_lhs(res,all_hyperplanes,num_hp_per_arrangement)))
result.persist()
string_result=result.map(lambda (filename,res): str(res)+","+filename)
string_result.saveAsTextFile(output_files_dest)

def any_matches(lhs1,lhs2):
	length=min(len(lhs1),len(lhs2))
	return any([lhs1[i]==lhs2[i] for i in range(length)])

def all_match(lhs,rhs):
	if (not (len(lhs)==len(rhs))):
		return False
	length=min(len(lhs),len(rhs))
	return all([lhs[i]==rhs[i] for i in range(length)])

def candidate_neighbors(unknown_file):
	try:
		my_spectrum=to_spectrum(downsampling(almost_raw(input_files_prefix+unknown_file)))
	except:
		return ([],0)
	my_lhs=to_lhs(my_spectrum,all_hyperplanes,num_hp_per_arrangement)
	return (result.filter(lambda (file,res): any_matches(res,my_lhs)).collect(),my_spectrum)

def score_false_positives(candidates,my_spectrum):
	scored_candidates={}
	for (cand,lhs) in candidates:
		their_spectrum=to_spectrum(downsampling(almost_raw(cand)))
		if all_match(my_spectrum,their_spectrum):
			cos_squared=1
		else:
			try:
				cos_squared=np.dot(my_spectrum,their_spectrum)**2/(np.dot(my_spectrum,my_spectrum)*np.dot(their_spectrum,their_spectrum))
			except:
				cos_squared=0
		scored_candidates[cand]=(lhs,cos_squared)
	return scored_candidates

def best_neighbors(unknown_file):
	(candidates,my_spectrum)=candidate_neighbors(unknown_file)
	return score_false_positives(candidates,my_spectrum)

print("Candidate Neighbors of aerosol can")
print(candidate_neighbors("aerosol-can-spray-01.wav"))
print(candidate_neighbors("arosol-can-spray-022.wav"))
print(best_neighbors("aerosol-can-spray-01.wav"))
