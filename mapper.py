'''
Created on 27 mar. 2019

@author: David Fernandez
@author: Maite Bernaus
'''

from COSBackend import CosBackend
import json

def splitText( text ):
    for char in '=-.,\n!?,':
        text=text.replace(char,' ')
    text = text.lower()
        
    word_list = text.split()  
        
    return word_list

def countingWords(data):
    word_list = splitText(data)
    dictionary={'lastBlock':'', 'firstBlock':''}
    if data[-1].isalpha():
        dictionary['lastBlock']=word_list[-1]
        del word_list[-1]
    if data[0].isalpha():
        dictionary['firstBlock']=word_list[0]
        del word_list[0]
    
    dictionary['words']=len(word_list)
    return dictionary

def wordCount (data):
    
    word_list = splitText(data)
    dictionary ={'lastBlock':'', 'firstBlock':''}
    if data[-1].isalpha():
        dictionary['lastBlock']=word_list[-1]
        del word_list[-1]
    if data[0].isalpha():
        dictionary['firstBlock']=word_list[0]
        del word_list[0]
        
    for word in word_list: 
        dictionary[word] = dictionary.get(word, 0) + 1
    return dictionary

def main(arg1):
    target_bucket=arg1.get('targetBucket')
    target=arg1.get('targetFile')
    source_bucket= arg1.get('sourceBucket')
    source_file=arg1.get('sourceFile')
    bytestart=arg1.get('startbyte')
    byteend=arg1.get('endbyte')
    op=arg1.get('op')
    
    a=CosBackend(arg1.get('configCOS'))
    data = a.get_object(source_bucket, source_file, extra_get_args={'Range':'bytes='+str(bytestart)+'-'+str(byteend)})
    
    data=data.decode('utf-8-sig')

    if op=='diffcount':
        func = wordCount
    elif op=='count': 
        func=countingWords
    else:
        exit(1)
    
    result=func(data)
    a.put_object(target_bucket, target, json.dumps(result))
    
    #Since all functions must return a dictionary, we return an empty one
    return {}