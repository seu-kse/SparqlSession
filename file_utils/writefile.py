import json
import pickle

def writeList(l, file_name, mod='w'):
    """
    write a list into file. for each line represent each item in list.

    input:
        l: list
        fila_name: str
    """
    fo = open(file_name, mod)
    for i in l:
        fo.write(str(i))
        fo.write("\n")
    fo.close()

def writeDict(dic, file_name):
    """
    write a dict into json file

    input:
        dic, dict
        file_name, str
    """
    json_content = json.dumps(dic)
    f = open(file_name,"w")
    f.write(json_content)
    f.close()

def write_pkl(file_name, save_content):
    fo = open(file_name, 'wb') 
    pickle.dump(save_content, fo)
