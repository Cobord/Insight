from scipy.fftpack import fft
from scipy.signal import decimate
from scipy.signal import stft
from scipy.io import wavfile
import math
import numpy as np
import glob

def get_interval_time():
	return 30

def get_samp_freq():
	return 44100

def get_down_factor():
	return 5

def get_num_hp_arrangements():
	return 5

# extends periodically so it is the right length len_needed
def periodize(signal,len_needed):
	current_len=len(signal)
	to_expand=int(math.ceil(len_needed/current_len))
	signal_expanded=np.tile(signal,to_expand*2)
	signal_expanded=signal_expanded[:len_needed]
	return signal_expanded

# read from filename, make desired length
# exceptions are returned with length 26 because
# in the next step they can be filtered away for being too short
# that removes the fileames that didn't give a wav file that could
# be opened
def almost_raw(filename):
	interval_time=get_interval_time()
	samp_freq=get_samp_freq()
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
	down_factor=get_down_factor()
	return decimate(raw_data,down_factor)

# from the downsampled periodized data give spectrum
# spectrum is given as all the real parts followed by all the imaginary parts
def to_spectrum(modified_data):
	#fourier=fft(modified_data)
	#length=int(len(fourier)/2)
	#fourier=fourier[:length]
	#if (fourier[0]>0):
	#	phase=math.e**(1j*np.angle(fourier[0]))
	#	fourier=fourier/phase
	#fourier=fourier*[2**(-i*i/(length*length)) for i in range(length)]
	samp_freq=get_samp_freq()
	down_factor=get_down_factor()
	num_per_sec=int(math.ceil(samp_freq/down_factor))
	fourier=stft(modified_data,nperseg=num_per_sec*5,noverlap=num_per_sec,return_onesided=True,padded=False)[2].flatten()
	fourier=np.append(np.real(fourier),np.imag(fourier))
	#return to_return/math.sqrt((to_return**2).sum())
	return fourier

# from a spectral data, the eqs defining hyperplanes
# this returns LSH
def to_lsh(spectra,all_hyperplanes_mat,num_hp_per_arr):
	return hash_point(spectra,all_hyperplanes_mat,num_hp_per_arr)

# given a point and all the hyperlanes compute the list of booleans for
# on + or - sideof each. That is unsimplified
# then break it up into num_hp_per_arr chunks and encode each of those
# as a single integer by binary
def hash_point(point,all_hyperplanes_mat,num_hp_per_arr):
	needed_length=all_hyperplanes_mat.shape[1]
	cur_length=point.shape[0]
	if (cur_length>needed_length):
		point=point[:needed_length]
	else:
		all_hyperplanes_mat=all_hyperplanes_mat[:][:cur_length]
	pre_hashed=np.dot(all_hyperplanes_mat,point)
	pre_hashed=pre_hashed.flatten().T
	unsimplified=list(map(lambda x: int(x>=0), pre_hashed))
	return simplify_hash_point(unsimplified,num_hp_per_arr)

# the chunking step above
def simplify_hash_point(int_list,num_hp_per_arrangement):
	chunked_list=[int_list[i:i+num_hp_per_arrangement] for i in range(0,len(int_list),num_hp_per_arrangement)]
	return list(map(int_list_to_num,chunked_list))

# the binary step above
def int_list_to_num(int_list):
	return np.array([int_list[i]<<i for i in range(len(int_list))]).sum()

# the spectrograms have adjustable parameters meaning their lengths are inconsistent across runs
# so instead this computes the length by performing the operation on one file
# and getting the needed length
# also count the total number of input files in the library
def find_hyperplane_dims(input_files_prefix):
	num_points=0
	found_dim=-1
	file_list=glob.glob(input_files_prefix+"*.wav")
	for file in file_list:
		if found_dim<0:
			x=almost_raw(file)
			if (len(x)>27):
				found_dim=len(to_spectrum(downsampling(x)))
		num_points=num_points+1
	num_hp_per_arrangement=int(np.log2(num_points))
	num_hp_arrangements=get_num_hp_arrangements()
	return (num_hp_arrangements,num_hp_per_arrangement,found_dim)

# format with ; for parsing ease, split on ;
def format_known_strings(filename,hashes):
	hashes_string=""
	for hash in hashes:
		hashes_string+=str(hash)
		hashes_string+=";"
	return hashes_string+filename

def any_matches(lhs1,lhs2):
	length=min(len(lhs1),len(lhs2))
	return any([lhs1[i]==lhs2[i] for i in range(length)])

def all_match(lhs,rhs):
	if (not (len(lhs)==len(rhs))):
		return False
	length=min(len(lhs),len(rhs))
	return all([lhs[i]==rhs[i] for i in range(length)])


# LSH of the unknown wav file
def lsh_of_unknown(unknown_file_full_path,all_hyperplanes_loc):
	all_hyperplanes_mat=np.load(all_hyperplanes_loc)
	num_hp_arrangements=get_num_hp_arrangements()
	num_hp_per_arrangement=int(all_hyperplanes_mat.shape[0]/num_hp_arrangements)
	try:
		my_spectrum=to_spectrum(downsampling(almost_raw(unknown_file_full_path)))
	except:
		return None
	my_lsh=to_lsh(my_spectrum,all_hyperplanes_mat,num_hp_per_arrangement)
	return my_lsh

# return the spectrum as well
def lsh_and_spectra_of_unknown(unknown_file_full_path,all_hyperplanes_loc):
	all_hyperplanes_mat=np.load(all_hyperplanes_loc)
	num_hp_arrangements=get_num_hp_arrangements()
	num_hp_per_arrangement=int(all_hyperplanes_mat.shape[0]/num_hp_arrangements)
	try:
		my_spectrum=to_spectrum(downsampling(almost_raw(unknown_file_full_path)))
	except:
		return None
	my_lsh=to_lsh(my_spectrum,all_hyperplanes_mat,num_hp_per_arrangement)
	return (my_lsh,my_spectrum)

def compare_spectra(my_spectrum,their_spectrum):
        if all_match(my_spectrum,their_spectrum):
		cos_squared=1
	else:
		try:
			cos_squared=np.dot(my_spectrum,their_spectrum)**2/(np.dot(my_spectrum,my_spectrum)*np.dot(their_spectrum,their_spectrum))
		except:
			cos_squared=0
	return cos_squared

# list of filenames and their hashes that are candidate neighbors
# compute the spectum of each and compute cos^2 with my_spectrum
# closer to 1 is more of a match
# returns a dictionary whose keys are filenames of potential neighbors
# the value is a pair of the hash of the candidate and the cos^2
# small number of candidates so this is done sequentially not via cluster overhead
def score_false_positives(candidates,my_spectrum):
	scored_candidates={}
	for cand in candidates:
		their_data=almost_raw(cand)
		if (len(their_data)>=27):
			their_spectrum=to_spectrum(downsampling(almost_raw(cand)))
			if all_match(my_spectrum,their_spectrum):
                        	cos_squared=1
	                else:
				try:
					cos_squared=np.dot(my_spectrum,their_spectrum)**2/(np.dot(my_spectrum,my_spectrum)*np.dot(their_spectrum,their_spectrum))
				except:
					cos_squared=0
			scored_candidates[cand]=cos_squared
	return sort_by_val(scored_candidates)

def sort_by_val(my_dict):
	return sorted(my_dict.iteritems(), key=lambda (k,v): (-v,k))

# version with spark, flask app is not run via spark-submit
# so problem with importing this, it would give error upon sc
# but if you do have spark context can use this one
#def score_false_positives_2(candidates,my_spectrum):
#	result=(sc.parallelize(candidates).map(lambda cand:(cand,almost_raw(cand)))
#		.filter(lambda (cand,res): len(res)>27)
#		.map(lambda (cand,res): (cand,downsampling(res)) )
#		.map(lambda (cand,res): (cand,to_spectrum(res)) )
#		.map(lambda (cand,res): (cand,compare_spectra(res,my_spectrum)) )
#		)
#	return result.collect()

