'''
Created on 20 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend

def main(arg1):
    filename=arg1.get('filename')
    f=open(filename)
    a = CosBackend()
    a.put_object("hilnurtest2", filename, f.read())
    f.close()
    
#main({'filename':'prueba.txt'})