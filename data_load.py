import os
from pyspark import SparkContext, SparkConf
import boto3
from scipy.io import wavfile
#from boto3.s3.connection import S3Connection

s3 = boto3.resource('s3')
s3_client=boto3.client('s3')
for bucket in s3.buckets.all():
	print(bucket.name)

response=boto3.client('s3').list_buckets()
bucket_names=[bucket['Name'] for bucket in response['Buckets']]
my_bucket_name=bucket_names[1]
print(my_bucket_name)
my_bucket=s3.Bucket(my_bucket_name)

all_filenames=[]
for object in my_bucket.objects.all():
	all_filenames.append((object.bucket_name,object.key))
print(all_filenames[:10])
quit()

desired_file=all_filenames[0]
s3.Object(desired_file[0],desired_file[1]).download_file('/tmp/{desired_file[1]}')

#access_key_id=os.environ['AWS_ACCESS_KEY_ID']
#secret_key=os.environ['AWS_SECRET_ACCESS_KEY']
#print(access_key_id)
#print(secret_key)

#conn=S3Connection(access_key_id,secret_key)
#bucket=conn.get_bucket(my_bucket_name)
#keys=bucket.list()
conf=SparkConf().setAppName("Load From s3")
sc=SparkContext(conf=conf)

res=sc.parallelize(all_filenames).filter(lambda (_,file): file[-4:]=='.wav')
print(res.collect())
print(res.count())

def readWav(filename,s3):
	bucket,file=filename
	#s3_location=bucket+".s3.amazonaws.com"+file
	#s3_location="s3n://"+bucket+"/"+file
	#s3_client.download_file(bucket,file,"local.wav")
	freq,_=wavfile.read(s3_location)
	return freq
	#return bucket+file

print(res.map(lambda file: readWav(file)).take(10))

