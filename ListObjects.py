'''
Created on 9 abr. 2019

@author: MaiteBG
'''


from COSBackend import CosBackend
    
def listObjects(arg1):
    a = CosBackend(arg1.get('configCOS'))
    headerlist=a.list_objects(arg1.get('bucket'))
    files=[]
    for elem in headerlist:
        files.append(elem.get('Key'))
    
    return {'files':files}

    
