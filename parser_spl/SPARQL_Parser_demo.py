from rdflib.plugins.sparql import parser
from rdflib.plugins.sparql.algebra import translatePrologue
from rdflib.plugins.sparql.algebra import traverse
from rdflib.plugins.sparql.algebra import translatePName
from rdflib.plugins.sparql.algebra import translatePath
from rdflib.plugins.sparql.algebra import translate, inner_traverse, inner_simplifyFilters

def parseStr(query):
        pq = parser.parseQuery(query)    

        # using function in rdflib to absolutize/resolve prefixes
        # may cause prefix error
        prologue = translatePrologue(pq[0], None)
        # using function in rdflib to simplify filter
        inner_traverse(pq[1], inner_simplifyFilters)    
        pq[1] = traverse(
            pq[1], visitPost=functools.partial(translatePName, prologue=prologue))
        # using function in rdflib to translate path
        if 'where' in pq[1].keys():
            pq[1]['where'] = traverse(pq[1]['where'], visitPost=translatePath)
        return pq