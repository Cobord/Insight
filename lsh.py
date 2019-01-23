import numpy as np

ambient_dimension=10
num_hyperplanes=3
num_points=5
is_real=False

all_points=None
all_hyperplanes=None


for i in range(num_points):
    all_points=np.random.randn(num_points,ambient_dimension)
    if (not is_real):
        all_points=all_points+1j*np.random.randn(num_points,ambient_dimension)

for i in range(num_hyperplanes):
    all_hyperplanes=np.random.randn(num_hyperplanes,ambient_dimension-1)
    if (not is_real):
        all_hyperplanes=all_hyperplanes+1j*np.random.randn(num_hyperplanes,ambient_dimension-1)
        all_hyperplanes=np.hstack((np.ones((num_hyperplanes,1)),all_hyperplanes))
    
def hash_point(point,all_hyperplanes):
    if is_real:
        pre_hashed=np.dot(all_hyperplanes,point)
        return list(map(lambda x: x>=0,pre_hashed))
    else:
        pre_hashed=np.dot(all_hyperplanes,point)-point[0]
        hashed=list(map(lambda x : [np.real(x)>=0,np.imag(x)>=0],pre_hashed))
        return [item for sublist in hashed for item in sublist]
    
#print(all_points)
for point in all_points:
    print(hash_point(point,all_hyperplanes))