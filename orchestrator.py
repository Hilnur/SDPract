'''
Created on 27 mar. 2019

@author: Maite Bernaus
@author: David Fernandez
'''
#necessary imports
import yaml
import time
from os.path import expanduser
from ibm_cf_connector import CloudFunctions

#imports for linear testing
'''
import mapper
import reduce
from COSBackend import CosBackend
'''


#Reads cloud function credentials from config file
def readParamsCloudF ():
    try:
        with open(expanduser('~')+'/ibm-cloud_config', 'r') as config_file:
            res = yaml.safe_load(config_file)
        config = {}
        config['endpoint']= res['ibm_cf']['endpoint']
        config['namespace']  = res['ibm_cf']['namespace']
        config['api_key']  = res['ibm_cf']['api_key']
        return config
    except :
        print('Error readParamsCloudF')

#reads COS credentials from config file
def readParamsCOS ():
    try:
        with open(expanduser('~')+'/ibm-cloud_config', 'r') as config_file:  
            res = yaml.safe_load(config_file)
        config = {}
        config['ibm_cos_endpoint']= res['ibm_cos']['endpoint']
        config['ibm_cos_access_key']  = res['ibm_cos']['access_key']
        config['ibm_cos_secret_key']  = res['ibm_cos']['secret_key']
        
        return config 
    except :
        print('Error readParamsCOS')


def main(filename, nchunks, op):
    #Checking parameters
    if op not in ['count', 'diffcount']:
        print("Valid operators are count (total number of words) or diffcount (count amounts of each individual word)")
        exit(1)
    if nchunks < 1:
        print("number of parallel processes needs to be a positive integer!")
        exit(1)
        
    #Reading config files and preparing the function backend
    configIBMCloud=readParamsCloudF()
    configCOS= readParamsCOS()
    functionbackend = CloudFunctions(configIBMCloud)

    #Preparing dictionaries to serve as function arguments
    argsOriginal = {}
    argsOriginal['filename']=filename
    argsOriginal['configCOS']= configCOS
    argsOriginal['bucket'] = 'originals'
    
    argsTemp = {}
    argsTemp['configCOS']= configCOS
    argsTemp['targetBucket'] = 'temps1'
    argsTemp['sourceBucket'] = 'originals' 
    argsTemp['sourceFile'] = filename
    argsTemp['op']=op

    #Reading the header of the origin file in COS (located in bucket 'originals') and calculating chunk size
    headers=functionbackend.invoke_with_result('GetHead', argsOriginal)
    if 'error' in headers.keys():
        print('File does not exist in target COS. Exiting program')
        exit(1)
    sizeFile = int(headers.get('content-length'))

    print('All parameters obtained. Beginning chunking')
    currentByte=0
    chunkCounter =0 
    chunkSize=(sizeFile//nchunks)+1
       
    while currentByte<sizeFile:
        argsTemp['startbyte']=currentByte
        
        if (currentByte+chunkSize)>sizeFile:
            argsTemp['endbyte']=sizeFile
        else:
            argsTemp['endbyte']=currentByte+chunkSize
        
        argsTemp['targetFile']='temp'+str(chunkCounter)
        #invoke mapper
        #mapper.main(argsTemp)  #discomment this for linear mapping (requires mapper.py)
        functionbackend.invoke('Map', argsTemp)
        currentByte=currentByte+chunkSize+1
        chunkCounter+=1
    
    print('All chunks sent. Waiting for results...')
    listObjects= (functionbackend.invoke_with_result('ListObjects', { 'bucket': 'temps1' , 'configCOS': configCOS})).get('files')
    while(len(listObjects)<nchunks):
        time.sleep(1)
        listObjects= (functionbackend.invoke_with_result('ListObjects', { 'bucket': 'temps1' , 'configCOS': configCOS})).get('files')
    print(listObjects)
    #result=reduce.main({'bucket':'temps1', 'configCOS':configCOS, 'op':op, 'prefix':'temp'})
    
    #result={}
    print('we got here')
    result=functionbackend.invoke_with_result('Reduce', {'bucket':'temps1', 'configCOS':configCOS, 'op':op, 'prefix':'temp'})
    print('also here')
    return result

#VARIOUS TEST CODES - COMMENT AND DISCOMMENT FOR TESTING
'''
configIBMCloud=readParamsCloudF()
configCOS= readParamsCOS()
functionbackend = CloudFunctions(configIBMCloud)
backend=CosBackend(configCOS)

f=open('lorem.txt', mode='r', encoding='utf-8-sig')
data=f.read()
#print(len(data2))
#print((data[0:300]))

test1=mapper.wordCount(data)
print(test1)
test2=mapper.countingWords(data)
print(test2)
print ("Also, length by split is "+str(len((data).split())))
#backend.put_object('originals', 'lorem.dict', json.dumps(test1))

#test2=backend.get_object('originals', 'lorem.dict')
#test2=test2.decode('utf-8-sig')
#test3= json.loads(test2)
#print(test3)

test6=main('lorem.txt', 10, 'diffcount')
print(test6)

for elem in test6.keys():
    if elem not in test1:
        print(elem)
        
if 'awooo' not in test6.keys(): print('testing')   
for elem in test1.keys():
    if elem not in test6:
        print(elem)
'''
