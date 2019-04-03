'''
Created on 27 mar. 2019

@author: Drascin
'''
from COSBackend import CosBackend

def joinDictionary (dictionary1,dictionary2 ):
    dictionaryJoin= dict(dictionary1, **dictionary2 ) #Join de los diccionarios - si k esta en los 2, valor del segundo
    for k, v in dictionary1.items():
        if (dictionary2.__contains__(k)):    #Si esta en los dos diccionarios actualizar valor en el join con la suma de valores
            dictionaryJoin.update({k:v+dictionary2.get(k)})
    return dictionaryJoin

def main(arg1):
    
    a=CosBackend()
    
    
    return 0