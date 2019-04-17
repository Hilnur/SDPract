'''
Created on 15 abr. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend

def main(arg1):
    a=CosBackend(arg1.get('configCOS'))
    ret=a.head_object(arg1.get('bucket'), arg1.get('filename'))
    ret2={'stuff':ret}
    return ret2