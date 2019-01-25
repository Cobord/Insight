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

def periodize(signal,len_needed,current_len):
    signal_expanded=np.copy(signal)
    to_expand=int(math.ceil(len_needed/current_len))
    for i in range(to_expand-1):
        signal_expanded=np.append(signal_expanded,signal)
    signal_expanded=signal_expanded[:len_needed]
    return signal_expanded

def modify_raw(filename,desiredT):
    freqs, raw_data = wavfile.read(filename)
    raw_data=raw_data.T[0]
    currentT=len(raw_data)/freqs
    len_needed=desiredT*freqs
    return periodize(raw_data,len_needed,len(raw_data))

def with_spectrum(filename,desiredT):
    raw_data=modify_raw(filename,desiredT)
    fourier = fft(raw_data)
    length=int(len(fourier)/2)
    fourier=fourier[:length]
    fourier=fourier*[2**(-i*i) for i in range(length)]
    return (filename,fourier)

def with_lhs(filename,desiredT,all_hyperplanes):
    _, spectra = with_spectrum(filename,desiredT)
    needed_length=all_hyperplanes.shape[1]
    cur_length=len(spectra)
    if (cur_length>needed_length):
        spectra=spectra[:needed_length]
    else:
        spectra=np.append(spectra,np.zeros(needed_length-cur_length))
    return (filename,hash_point(spectra,all_hyperplanes,False))

def hash_point(point,all_hyperplanes,is_real):
    if is_real:
        pre_hashed=np.dot(all_hyperplanes,point)
        return list(map(lambda x: x>=0,pre_hashed))
    else:
        pre_hashed=np.dot(all_hyperplanes,point)-point[0]
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
_,y=with_spectrum("wavFiles/aerosol-can-spray-01.wav",desiredT)
all_hyperplanes=construct_hyperplanes(int(np.log2(num_points)),len(y),False)
# append another copy of all_hyperplanes with different randomness
#  for each times_hashed

sample_rate=.05
result=sc.textFile("wavFilesList2.txt").map(lambda name: "wavFiles/"+name).sample(False,sample_rate,None).map(lambda file: with_lhs(file,desiredT,all_hyperplanes)).map(lambda (filename, res): swap_and_simplify(filename,res))
result.map(lambda (res,filename): str(res)+","+filename).saveAsTextFile("saved_lhs_output")
