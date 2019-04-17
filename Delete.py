'''
Created on 9 abr. 2019

@author: MaiteBG
'''
from COSBackend import CosBackend
    
def deleteObject(arg1):
  
    a = CosBackend(arg1.get('configCOS'))
    a.delete(arg1.get('bucket'), arg1.get('filename'))
    
