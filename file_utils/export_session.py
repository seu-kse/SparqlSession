import sys
import os
o_path = os.getcwd()
sys.path.append(o_path)

import argparse
import time
from file_utils.readfile import read_valuedSession
from file_utils.writefile import write_pkl
from agraph_utils import GetConn, GetTextFromQueryID
from franz.openrdf.sail.allegrographserver import AllegroGraphServer
from franz.openrdf.repository.repository import Repository
from tqdm import tqdm


def readSessionFromDirectory(input_dir, output_dir):
    files = os.listdir(input_dir)
    for filei in tqdm(files):
        if 'valuedSession' in filei:
            print(f'read {filei} ...')
            namei = filei.split('valuedSession')[0][:-1]
            dbpedia = True if 'access.log' in namei or 'dbpedia' in namei else False
            if not dbpedia:
                data = read_valuedSession(namei, dbpedia=dbpedia)
                writeSessionToDirectory(output_dir, namei, data, dbpedia)

def writeSessionToDirectory(output_dir, name, data, dbpedia):
    """
    output format: json 
    {
        dataset: 'swdf'
        sessions: [
                {
                    session_id: 0
                    session_length: 10
                    user: xxxx
                    queries: [
                        {
                            query_id: 0
                            query_content: SPARQL query
                            time_stamp: 
                            index_in_file: for dbpedia, the original index in file; 
                                           for others, the original IRI
                        }
                    ]
                }
            ]
        }
    }

    """
    if not dbpedia:
        conn, repo = GetConn(name, if_return_repo=True)
    else:
        conn = None

    output = {'dataset': name, 'sessions': []}
    index_in_file_key = 'idxInFile' if dbpedia else 'query'

    for session_idx, sessi in tqdm(enumerate(data), total=len(data)):
        sessioni = {'session_id': session_idx, 'session_length': len(sessi['session']),
                    'user': sessi['agent'], 'queries': []}
        texts = getTexts(sessi['session'], dbpedia, conn)
        for i in range(len(sessi['session'])):
            query_temp = {'query_id': i, 'query_content': texts[i], 
                          'time_stamp': sessi['session'].iloc[i]['time'],
                          'index_in_file': sessi['session'].iloc[i][index_in_file_key]}
            sessioni['queries'].append(query_temp)
        output['sessions'].append(sessioni)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    file_name = os.path.join(output_dir, f'{name}_session_data.json')
    write_pkl(file_name, output)
    if not dbpedia:
        conn.close()
        repo.shutDown()
        


def getTexts(session, dbpedia, conn):
    if dbpedia:
        texts = session['query'].tolist()
    else:
        query_id_list = session['query'].tolist()
        texts_list = GetTextFromQueryID(conn, query_id_list)
        texts = [i[0] for i in texts_list]        
        """
        # ------------------get conn-----------------------
        AGRAPH_HOST = os.environ.get('AGRAPH_HOST')
        AGRAPH_PORT = int(os.environ.get('AGRAPH_PORT', '10035'))
        server = AllegroGraphServer(AGRAPH_HOST, AGRAPH_PORT, 'bubble', 'bubble')           
        catalog = server.openCatalog('')
        # get repo_names
        repo_names = []
        for repo_name in catalog.listRepositories():
            repo_names.append(repo_name)

        if name in repo_names:
            with catalog.getRepository(name, Repository.OPEN) as repo:
                with repo.getConnection() as conn:
                    texts_list = GetTextFromQueryID(conn, query_id_list)
                    texts = [i[0] for i in texts_list]
        # ----------------------end------------------------- 
        print('sleeping ... ') 
        time.sleep(60)
        """
    return texts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # choose the input directory with session_files in it.
    parser.add_argument("--input", '-i', type=str, help="choose the input directory")
    # choose the output directory 
    parser.add_argument("--output", '-o', type=str, help="choose the output directory")
    args = parser.parse_args()

    readSessionFromDirectory(args.input, args.output)

