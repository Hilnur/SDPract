'''
Created on 27 mar. 2019

@author: Drascin
'''

from COSBackend import CosBackend

def splitText( text ):
        
    for char in '=-.,\n!?,':
        text=text.replace(char,' ')
    text = text.lower()
        
    word_list = text.split()  
    #f.close()
    
    return word_list


def countingWords(data):
    word_list = splitText(data)
    return len(word_list)

def wordCount (data):
    
    word_list = splitText(data)
    dictionary ={}
    for word in word_list: 
        dictionary[word] = dictionary.get(word, 0) + 1
    return dictionary

def main(arg1):
    filename=arg1.get('filename')
    bytestart=arg1.get('startbyte')
    byteend=arg1.get('byteend')
    op=arg1.get('op')
    result_storage=arg1.get('result_storage')
    target=arg1.get('target')
    
    a=CosBackend()
    data = a.get_object('hilnurtest2', filename, extra_get_args={'Range':'bytes='+bytestart+'-'+byteend}).get('content')
    
    if op==1:
        func = wordCount
    elif op==2: 
        func=countingWords
    
    result=func(data)
    a.put_object(result_storage, target, data)
    