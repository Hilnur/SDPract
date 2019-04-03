'''
Created on 13 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend

def main(arg1):
    filename=arg1.get('filename')
    a=CosBackend()
    data = a.get_object('hilnurtest2', filename)
    return {'content':data}

'''
a=CosBackend()
print(a.list_objects('hilnurtest2'))
'''