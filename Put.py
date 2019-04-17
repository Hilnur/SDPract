'''
Created on 20 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend
    
def putObject(arg1):

    a = CosBackend(arg1.get('configCOS'))
    a.put_object(arg1.get('bucket'), arg1.get('filename'), arg1.get('content'))
    