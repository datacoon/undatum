# -*- coding: utf8 -*-
import duckdb
import logging
from iterable.helpers.detect import open_iterable
from tqdm import tqdm

from pymongo import MongoClient
from elasticsearch import Elasticsearch

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']

DUCKABLE_FILE_TYPES = ['parquet', 'csv', 'jsonl', 'json', 'jsonl.gz']
DUCKABLE_CODECS = ['gz', 'zst']


DEFAULT_BATCH_SIZE = 50000

def get_iterable_options(options):
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out            


class BasicIngester:
    def __init__(self):
        pass
    
    def ingest(self, batch):
        raise NotImplemented

class ElasticIngester(BasicIngester):
    def __init__(self, uri:str, api_key:str, search_index:str, document_id:str="id"):
        self.client = Elasticsearch(uri, api_key=api_key, verify_certs=False,ssl_show_warn=False, timeout=60, max_retries=10, retry_on_timeout=True)
        self._index = search_index
        self._item_id = document_id
        pass


    def ingest(self, batch):        
        documents = []
        for doc in batch:
            documents.append({ "index": { "_index": self._index, '_id' : doc[self._item_id]}})
            documents.append(doc)
        result = self.client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")


class MongoIngester:
    def __init__(self, uri, db, table,do_drop=False):
        self.client = MongoClient(uri)
        self.db = self.client[db]
        if do_drop:
            del self.db[table]
        self.coll = self.db[table]


    def ingest(self, batch):
        result = self.coll.insert_many(batch)


class Ingester:
    def __init__(self, batch_size=DEFAULT_BATCH_SIZE):
        self.batch_size = batch_size
        pass

    def ingest(self, fromfiles, uri, db, table, options={}):
        for filename in fromfiles:
            self.ingest_single(filename, uri, db, table, options=options)

  
    def ingest_single(self, fromfile, uri, db, table, options={}):
        """Loads single file data contents to the schemaless database like MongoDB"""
        dbtype = options['dbtype']
        processor = None
        totals = -1
        skip = options['skip']
        use_totals = options['totals']if 'totals' in options.keys() else False
        do_drop = options['drop']if 'dro[]' in options.keys() else False

        if use_totals:
            parts = fromfile.rsplit('.', 2)
            if len(parts) == 2:
                if parts[-1].lower() in DUCKABLE_FILE_TYPES:
                    totals = duckdb.sql(f"select count(*) from '{fromfile}'").fetchone()[0]
            elif len(parts) == 3:
                if parts[-2].lower() in DUCKABLE_FILE_TYPES and parts[-1].lower() in DUCKABLE_CODECS:
                     totals = duckdb.sql(f"select count(*) from '{fromfile}'").fetchone()[0]
        if dbtype == 'mongodb':
            processor = MongoIngester(uri, db, table, do_drop=do_drop)
        elif dbtype == 'elastic':
            api_key = options['api_key']
            id_key = options['doc_id']
            processor = ElasticIngester(uri=uri, api_key=api_key, search_index=table, document_id=id_key)
        iterableargs = get_iterable_options(options)
        it_in = open_iterable(fromfile, mode='r', iterableargs=iterableargs)       
        logging.info(f'Ingesting data: filename {fromfile}, uri: {uri}, db {db}, table {table}')
        n = 0
        batch = []
        for row in tqdm(it_in, total=totals):
            n += 1 
            if skip is not None and skip > 0:
                if n < skip: continue
            batch.append(row)
            if n % self.batch_size == 0:
                processor.ingest(batch)
                batch = []
        if len(batch) > 0: 
            processor.ingest(batch)

