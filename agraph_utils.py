import os
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
from franz.openrdf.sail.allegrographserver import AllegroGraphServer
from franz.openrdf.repository.repository import Repository

def GetConn(name, if_return_repo=False):
    AGRAPH_HOST = os.environ.get('AGRAPH_HOST')
    AGRAPH_PORT = int(os.environ.get('AGRAPH_PORT', '10035'))

    server = AllegroGraphServer(AGRAPH_HOST, AGRAPH_PORT,
                                'bubble', 'bubble')
           
    catalog = server.openCatalog('')

    # get repo_names
    repo_names = []
    for repo_name in catalog.listRepositories():
        repo_names.append(repo_name)

    if name in repo_names:
        mode = Repository.OPEN
        my_repository = catalog.getRepository(name, mode)
        conn = my_repository.getConnection()
    else:
        print('%s repo do not exist !' % name)
    if if_return_repo:
        return conn, my_repository
    else:
        return conn

def query_agraph(conn, query):
    with conn.executeTupleQuery(query) as result:
        res = result.toPandas()
    return res

def GetQueryIDs(conn):
    """
    return all the queryids without parse error in data source: conn .
    this query do not include query with property path.
    """
    q_queryid = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query 
    WHERE {
        ?query rdf:type lsqv:Query ;
               lsqv:hasStructuralFeatures ?sf .
        FILTER NOT EXISTS { ?query lsqv:parseError ?error }
        FILTER NOT EXISTS { ?sf lsqv:triplePath ?path}
        FILTER NOT EXISTS { ?query lsqv:processingError ?er}
    }
    """
    res = query_agraph(conn, q_queryid)
    return res

def GetQueryIDs_all(conn):
    q_queryid = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query 
    WHERE {
        ?query rdf:type lsqv:Query .
    }
    """
    res = query_agraph(conn, q_queryid)
    res = res['query'].tolist()
    return res 

def GetQueryIDs_limit(conn):
    q_queryid = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query 
    WHERE {
        ?query rdf:type lsqv:Query .
    }
    LIMIT 5
    """
    res = query_agraph(conn, q_queryid)
    res = res['query'].tolist()
    return res 

def GetQueryIDs_TP(conn):
    """
    return all the queryids without parse error in data source: conn .
    this query only include querys with property path.
    query with property path can only be parsed by spin tree, triple can not directly gotten by hasTP feature.
    """
    q_queryid = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query 
    WHERE {
        ?query rdf:type lsqv:Query ;
               lsqv:hasStructuralFeatures ?sf .
        FILTER NOT EXISTS { ?query lsqv:parseError ?error }
        FILTER EXISTS { ?sf lsqv:triplePath ?path}
        FILTER NOT EXISTS { ?query lsqv:processingError ?er}
    }
    """
    res = query_agraph(conn, q_queryid)
    return res

def q_agent_time_query():
    q = """
    PREFIX aksw: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?exe ?query
    WHERE {
        ?query_id rdf:type aksw:Query ;
            aksw:hasRemoteExec ?exe ;
            aksw:text ?query .
    }
    OFFSET 800000
    LIMIT 1000
    """
    return q

def GetID_duplicate(conn):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query (COUNT (DISTINCT ?text) AS ?count) 
    WHERE {
        ?query rdf:type lsqv:Query ;
            lsqv:text ?text .
    }
    GROUP BY ?query
    ORDER BY desc(?count)

    """
    res = query_agraph(conn, q)
    res = res.loc[res['count']>1]
    return res

def GetAgentID(conn):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?agent
    WHERE {
        ?s <http://www.w3.org/ns/prov#wasAssociatedWith> ?agent
    }

    """
    res = query_agraph(conn, q)
    res = res['agent'].tolist()
    return res

def GetQuerysFromAgent(conn, agent):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?query ?time 
    WHERE {
        ?query lsqv:hasRemoteExec ?exe ;
               rdf:type           lsqv:Query .
        ?exe <http://www.w3.org/ns/prov#wasAssociatedWith> %s ;
             <http://www.w3.org/ns/prov#atTime>   ?time .
    }
    """ % agent
    # print(q)
    res = query_agraph(conn, q)
    res = res.sort_values(by='time')
    return res

def GetQueryIDFromTime(conn, atTime):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

    SELECT DISTINCT ?query  ?time
    WHERE {
        ?query lsqv:hasRemoteExec ?exe .
        ?exe <http://www.w3.org/ns/prov#atTime>  ?time .
    }
    LIMIT 10
    """ # % atTime
    print(q)
    # FILTER ( xsd:dateTime(?time) = xsd:dateTime("%s") )
    res = query_agraph(conn, q)
    return res

def GetQueryIDAndTime(conn):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

    SELECT DISTINCT ?query  ?time
    WHERE {
        ?query lsqv:hasRemoteExec ?exe .
        ?exe <http://www.w3.org/ns/prov#atTime>  ?time .
    }
    """ 
    # print(q)
    res = query_agraph(conn, q)
    return res

def GetTextFromQueryID(conn, querys):
    """
    conn, agraph connect server
    querys, a list of query ids
    """
    texts = []
    for query in querys:
        q = """
        PREFIX lsqv: <http://lsq.aksw.org/vocab#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT DISTINCT ?text
        WHERE {
            %s  lsqv:text ?text
        }
        """ % query
        res = query_agraph(conn, q)
        res = res['text'].tolist()
        texts.append(res)
    return texts

def GetQueryUsedFeature(conn, query):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?feature
    WHERE {
        %s   lsqv:hasStructuralFeatures ?sf .
        ?sf  lsqv:usesFeature   ?feature .
    }    
    """ % query
    res = query_agraph(conn, q)
    res = res['feature'].tolist()
    return res

def GetResultSetSizeAndProcessTime(conn, query):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?runTime ?resultSize
    WHERE {
        %s  lsqv:hasLocalExec     ?local .
        ?local  lsqv:resultSize   ?resultSize .
        ?local  lsqv:runTimeMs    ?runTime .      
    }    
    """ % query
    res = query_agraph(conn, q)
    return res    


def GetRdfType(conn, term):
    q = """
    PREFIX lsqv: <http://lsq.aksw.org/vocab#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>

    SELECT DISTINCT ?type
    WHERE {
        %s    rdf:type  ?type .
    }    
    """ % term
    print(q)
    res = query_agraph(conn, q)
    return res 