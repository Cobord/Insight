
import boto3
import botocore
s3 = boto3.resource('s3')

def get_matching_s3_keys(bucket,suffix='.wav'):
	kwargs={'Bucket': bucket, 'Suffix': suffix}
	while (True):
		resp=s3.list_objects_v2(**kwargs)
		for obj in resp['Contets']:
			key=obj['Key']
		if (key.endsWith(suffix)):
			yield key
		try:
			kwargs['ContinuationToken']=resp['NextContinuationToken']
		except KeyError:
			break

def print_matching_keys(bucket_name):
	bucket=s3.Bucket(bucket_name)
	for key in get_matching_s3_keys(bucket):
		print(key)

def get_my_objects(bucket_name,suffix):
	for bucket in s3.buckets.all():
		if (bucket['Name']==bucket_name):
			for key in bucket.objects.all():
				if (key.key.endsWith(suffix)):
					print(key.key)

response=boto3.client('s3').list_buckets()
bucket_names=[bucket['Name'] for bucket in response['Buckets']]
print(bucket_names)
print(get_my_objects(bucket_names[1],'.wav'))
