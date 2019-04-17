'''
Created on 27 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''
from COSBackend import CosBackend
import json


def joinDictionary (dictionary1,dictionary2 ):
    dictionaryJoin= dict(dictionary1, **dictionary2 ) #Join de los diccionarios - si k esta en los 2, valor del segundo
    for k, v in dictionary1.items():
        if (dictionary2.__contains__(k)):    #Si esta en los dos diccionarios actualizar valor en el join con la suma de valores
            dictionaryJoin.update({k:v+dictionary2.get(k)})
    return dictionaryJoin

def main(arg1):
    
    a=CosBackend(arg1.get('configCOS'))
    finalresult={}
    headerlist=a.list_objects(arg1.get('bucket'))
    tempFiles=[]
    for elem in headerlist:
        tempFiles.append(elem.get('Key'))
    
    carry=''
    #for elem in tempFiles:
    counter=0
    while (counter<len(tempFiles)):
        currentfilename=arg1.get('prefix')+str(counter)
        current=json.loads(a.get_object(arg1.get('bucket'), currentfilename))

        if not ((not carry) and (not current.get('firstBlock'))):
            if (arg1.get('op')=='count'):
                current['words']+=1
            elif (arg1.get('op')=='diffcount'):
                bridgeword=carry+current.get('firstBlock')
                print(bridgeword)
                finalresult=joinDictionary(finalresult, {bridgeword:1})
                print(finalresult)
        
        carry=current.get('lastBlock')
        del current['firstBlock']
        del current['lastBlock']
        
        finalresult=joinDictionary(finalresult, current)
        a.delete(arg1.get('bucket'), currentfilename)
        counter+=1
    
    #final check (in case the text ends without a punctuation mark/whitespace/line break!)
    if carry:
        if arg1.get('op')=='count':
            finalresult['words']+=1
        elif arg1.get('op')=='diffcount':
            finalresult=joinDictionary(finalresult, {carry:1})
    #Program over, return result
    return finalresult