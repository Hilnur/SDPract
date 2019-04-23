'''
Created on 27 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''
from COSBackend import CosBackend
import json

#Joins two dictionaries
def joinDictionary (dictionary1,dictionary2 ):
    dictionaryJoin= dict(dictionary1, **dictionary2 ) 
    for k, v in dictionary1.items():
        if (dictionary2.__contains__(k)):
            dictionaryJoin.update({k:v+dictionary2.get(k)})
    return dictionaryJoin

def main(arg1):
    
    a=CosBackend(arg1.get('configCOS'))
    finalresult={}
    headerlist=a.list_objects(arg1.get('bucket'))
    tempFiles=[]
    for elem in headerlist:
        tempFiles.append(elem.get('Key'))
    
    carry='' #Carry will represent the leftover word chunk from last mapped fragment
    
    #Due to IBM COS changing the order of files, we can't use a for_in loop.
    #Otherwise the cut words between blocks would be lost, as order of blocks would be
    #shifted
    counter=0
    while (counter<len(tempFiles)):
        currentfilename=arg1.get('prefix')+str(counter)
        current=json.loads(a.get_object(arg1.get('bucket'), currentfilename))

        if not ((not carry) and (not current.get('firstBlock'))):
            if (arg1.get('op')=='count'):
                current['words']+=1
            elif (arg1.get('op')=='diffcount'):
                bridgeword=carry+current.get('firstBlock')
                finalresult=joinDictionary(finalresult, {bridgeword:1})
        
        carry=current.get('lastBlock')
        del current['firstBlock']
        del current['lastBlock']
        
        finalresult=joinDictionary(finalresult, current)
        a.put_object('temps2', currentfilename, json.dumps(finalresult))
        a.delete(arg1.get('bucket'), currentfilename)
        counter+=1
    
    #final check (in case the text ends without a punctuation sign/whitespace/line break and the carry can't be properly joined!)
    if carry:
        if arg1.get('op')=='count':
            finalresult['words']+=1
        elif arg1.get('op')=='diffcount':
            finalresult=joinDictionary(finalresult, {carry:1})
    #Program over, return result
    #We quickly ran into an error where IBM Functions was unable to return a dictionary with too many separate entries. To avoid this issue, 
    #we will save the final results to COS and just return an "okay" message
    a.put_object('temps1', 'resultados', json.dumps(finalresult))
    
    return {'result':'okay'}