'''
Created on 13 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend
        
def getObject(arg1):
    
    filename= arg1.get('filename')
    a=CosBackend(arg1.get('configCOS'))
    data=a.get_object(arg1.get('bucket'), filename, extra_get_args= {'Range':'bytes='+str(arg1.get('startbyte'))+'-'+str(arg1.get('endbyte'))})
    return {'content':data}


