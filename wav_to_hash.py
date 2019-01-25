from scipy.fftpack import fft
from scipy.io import wavfile
import math
import numpy as np

from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("local")
        .setAppName("spectra")
        .set("spark.executor.memory","1g"))
sc = SparkContext(conf=conf)
# the function that extends periodically
# and imposes the right time length
def periodize(signal,len_needed,current_len):
    signal_expanded=np.copy(signal)
    to_expand=int(math.ceil(len_needed/current_len))
    for i in range(to_expand-1):
        signal_expanded=np.append(signal_expanded,signal)
    signal_expanded=signal_expanded[:len_needed]
    return signal_expanded

# read from filename
# make length desiredT by extending periodically
# or cutting off
def modify_raw(filename,desiredT):
    freqs, raw_data = wavfile.read(filename)
    stereo=(raw_data.shape[0]==2)
    if stereo:
        raw_data=raw_data.T[0]
    currentT=len(raw_data)/freqs
    len_needed=desiredT*freqs
    return periodize(raw_data,len_needed,len(raw_data))

def dumb_version(filename,desired_length):
    _, raw_data = wavfile.read(filename)
    raw_data=raw_data.T[0]
    return (filename,periodize(raw_data,desired_length,len(raw_data)))

# Fourier transform and a simple Gaussian filter
# this assumes fixed rate
# it has been 44k for all files so far
def with_spectrum(filename,desiredT):
    raw_data=modify_raw(filename,desiredT)
    fourier = fft(raw_data)
    length=int(len(fourier)/2)
    fourier=fourier[:length]
    #fourier=fourier*[2**(-i*i) for i in range(length)]
    return (filename,fourier)
	
# assume that desiredSteps is smaller than the result of making a desiredT seconds
# long clip
def time_domain_version(filename,desiredT,desiredSteps):
    raw_data=modify_raw(filename,desiredT)
    return (filename,periodize(raw_data,desiredSteps,len(raw_data)))

# with the Locally Sensitive Hash provided by these files
# for spectrum version
def with_lhs_spectrum(filename,desiredT,all_hyperplanes):
    _, spectra = with_spectrum(filename,desiredT)    
    return (filename,hash_point(spectra,all_hyperplanes,False))

# similarly for time domain
def with_lhs_time(filename,desiredT,all_hyperplanes):
    needed_length=all_hyperplanes.shape[1]
    #(_, signal) = time_domain_version(filename,desiredT,needed_length)
    (_,signal) = dumb_version(filename,1000000)
    return (filename,hash_point(signal,all_hyperplanes,True))
	
# compute the hash
def hash_point(point,all_hyperplanes,is_real):
    needed_length=all_hyperplanes.shape[1]
    cur_length=point.shape[0]
    if (cur_length>needed_length):
        point=point[:needed_length]
    else:
        all_hyperplanes=all_hyperplanes[:][:cur_length]
    if is_real:
        pre_hashed=np.dot(all_hyperplanes,point)
        return list(map(lambda x: x>=0,pre_hashed))
    else:
        pre_hashed=np.dot(all_hyperplanes,point)
        pre_hashed=pre_hashed-point[0]
        hashed=list(map(lambda x: [np.real(x)>=0,np.imag(x)>=0],pre_hashed))
        return [item for sublist in hashed for item in sublist]

def construct_hyperplanes(num_hyperplanes,ambient_dimension,is_real):
    all_hyperplanes=np.random.randn(num_hyperplanes,ambient_dimension-1)
    if (not is_real):
        all_hyperplanes=all_hyperplanes+1j*np.random.randn(num_hyperplanes,ambient_dimension-1)
        all_hyperplanes=np.hstack((np.ones((num_hyperplanes,1)),all_hyperplanes))
    else:
        all_hyperplanes=np.hstack((np.random.randn(num_hyperplanes,1),all_hyperplanes))
    return all_hyperplanes

def swap_and_simplify(filename,res):
    res_as_num=np.array([int(res[i])<<i for i in range(len(res))]).sum()
    return (res_as_num,filename)

times_hashed=1
num_points=10
desiredT=30
sample_rate=.05
#_,y=with_spectrum("wavFiles/aerosol-can-spray-01.wav",desiredT)
#_,y=time_domain_version("wavFiles/aerosol-can-spray-01.wav",desiredT,1000000) # slightly below the 1.32 for 44 kHz of desiredT=30
#y=len(y)

#prefix="hdfs://ec2-107-22-75-68.compute-1.amazonaws.com:9000/user/wavFiles/"
prefix="wavFiles/"
#out_destination="hdfs://ec2-107-22-75-68.compute-1.amazonaws.com:9000/user/saved_lhs_output"
out_destination="saved_lhs_output"

def make_hypers_and_hash(y_local):
    all_hyperplanes=construct_hyperplanes(int(np.log2(num_points)),y_local,False)
    #result=sc.textFile("wavFilesList2.txt").map(lambda name: prefix+name).sample(False,sample_rate,None).map(lambda file: with_lhs_spectrum(file,desiredT,all_hyperplanes)).map(lambda (filename, res): swap_and_simplify(filename,res))
    result=sc.textFile("wavFilesList2.txt").map(lambda name: prefix+name).sample(False,sample_rate,None).map(lambda file: with_lhs_time(file,desiredT,all_hyperplanes)).map(lambda (filename, res): swap_and_simplify(filename,res))
    result.map(lambda (res,filename): str(res)+","+filename).saveAsTextFile(out_destination)
    return (result,all_hyperplanes)

def candidate_neighbors(input_file,hashes,all_hyperplanes):
    # load result of hashes from memory into an RDD
    _, hash_point=with_lhs_time(input_file,desiredT,all_hyperplanes)
    hash_point,_ = swap_and_simplify(None,hash_point)
    return hashes.filter(lambda (res,filename): res==hash_point).map(lambda (res,filename): filename).collect()

all_candidate_nbhrs=[]
for i in range(times_hashed):
    (hashes,all_hyperplanes)=make_hypers_and_hash(1000000)
    all_candidate_nbhrs=all_candidate_nbhrs+candidate_neighbors("wavFiles/aerosol-can-spray-01.wav",hashes,all_hyperplanes)
print(all_candidate_nbhrs)
