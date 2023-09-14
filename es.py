from elasticsearch import Elasticsearch, helpers
class Es:

    def __init__(self, es_ip, es_port):
        self.es = Elasticsearch([es_ip], port=es_port)

    def read(self,index,query):
        res = helpers.scan(self.es, index=index, query=query)
        return res

    def write_bunch(self,li,index):
        res, _ = helpers.bulk(self.es, li, index=index, raise_on_error=True)
        return res

