'''
Created on 27 mar. 2019

@author: MaiteBG
'''


from Funcions.CloudFunctions import CloudFunctions
from Funcions.CloudFunctions import leerParametrosCloudF
from fileinput import filename


def map(filename, chunk_size):
    
    # - subir archivo al Obejct store
    
    lenFile= len(filename)
    numMap= (lenFile/chunk_size)
   # print (numMap)

    arg = {}
    arg['filename']=filename
   # arg['op']
    #arg['result_storage']
    #arg['target']
    
    i=0
    while i<numMap :
        arg['startbyte']=i*chunk_size
        if (i+1<numMap):                #si no es el ultimo map
            arg['byteend']=((i+1)*chunk_size)-1
        else :
            arg['byteend']=lenFile-1
        i=i+1
        print (arg)#- obtener bytes y realizar operacion
        


#config= leerParametrosCloudF()
#print(config)
#cloudF= CloudFunctions(config)

map('prueba.txt', 4)
#subir fichero al cloud

    
