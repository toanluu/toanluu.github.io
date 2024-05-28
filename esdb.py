import json
import logging
import time
from typing import Optional, Literal

import elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.ERROR)

REQUEST_TIMEOUT = 600  # 10 minutes
SCROLL_SIZE = '2h'


class NoSqlDB(object):
    """
    Interface of nosql db
    """

    def delete(self):
        raise NotImplementedError

    def create(self):
        raise NotImplementedError

    def delete_item(self, item_id):
        raise NotImplementedError

    def get_item(self, item_id):
        raise NotImplementedError

    def insert_item(self, item):
        raise NotImplementedError

    def insert_items(self, items):
        raise NotImplementedError

    def iterate_items(self, batch_size):
        raise NotImplementedError

    def size(self):
        raise NotImplementedError


class ESDB(NoSqlDB):
    """Utility to access ES like a database
    """
    es_client: Optional[elasticsearch.Elasticsearch]

    def __init__(self, es_client, db_name, settings=None, settings_file=None):
        """
        Initiate ES with index name
        :param es_client: es_client
        :param db_name: index name
        """
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(logging.INFO)

        self.es_client = es_client
        self.db_name = db_name
        self.settings = settings
        if settings is None and settings_file is not None:
            with open(settings_file) as f:
                self.settings = json.load(f)
        self.create()

    def delete(self):
        """"
        Delete db by name
        """
        self.log.info('Delete index %s', self.db_name)
        self.es_client.indices.delete(index=self.db_name, ignore_unavailable=True)

    def create(self):
        """
        Create db by name
        """
        if not self.es_client.indices.exists(index=self.db_name):
            self.log.info('Create index %s', self.db_name)
            self.es_client.indices.create(index=self.db_name, body=self.settings)
        else:
            self.log.info('Index %s is exists, no need to create', self.db_name)

    def _get_update_script(self, update_type, strict):
        """
        Get update script in the painless language (ES scripting language)

        :param update_type: 'partial' or 'full'
        :param strict: if True, only update fields that already exist in the document
        :return: The script as a string
        """

        script = None

        if update_type == 'partial' and strict:
            return '''
            for (entry in params.entrySet()) {
                if (ctx._source.containsKey(entry.getKey())) {
                    ctx._source[entry.getKey()] = entry.getValue();
                }
            }
            '''

        if update_type == 'partial' and not strict:
            return '''
            for (entry in params.entrySet()) {
                ctx._source[entry.getKey()] = entry.getValue();
            }
            '''

        elif update_type == 'full' and strict:

            return '''
            for (entry in ctx._source.entrySet()) {
                if (entry.getKey() != 'id') {
                    ctx._source[entry.getKey()] = null;
                }
            }

            for (entry in params.entrySet()) {
                if (ctx._source.containsKey(entry.getKey())) { 
                    ctx._source[entry.getKey()] = entry.getValue();
                }
            }
            '''

        elif update_type == 'full' and not strict:
            return '''
            for (entry in ctx._source.entrySet()) {
                if (entry.getKey() != 'id') {
                    ctx._source[entry.getKey()] = null;
                }
            }

            for (entry in params.entrySet()) {
                ctx._source[entry.getKey()] = entry.getValue();
            }
            '''

        raise ValueError(f'Unknown update type: {update_type} (should be "partial" or "full")')

    def update_items(self, query=None, id_list=None, update_fields=None, refresh=False,
                     update_type: Literal['partial', 'full'] = 'partial', strict=True):
        """
        Update items in the db by query or id_list with update_fields (partial or full update)
        :param query: Elasticsearch query to select items to update
        :param id_list: List of ids to update
        :param update_fields: Fields to update the documents with
        :param refresh: If True, refresh the index after the update
        :param update_type: 'partial' or 'full'
        :param strict: If True, only update fields that already exist in the document
        :return: The result of the update operation
        """
        if update_fields is None:
            update_fields = {}

        if id_list is not None:
            self.log.debug('Ignoring query since id_list is provided')

            if not isinstance(id_list, list) or len(id_list) == 0:
                raise ValueError('id_list should be a list of ids')

            query = {
                'bool': {
                    'must': [
                        {'terms': {'id': id_list}}
                    ]
                }
            }

        if query is None:
            raise ValueError('Either query or id_list should be provided')

        update = {
            'source': self._get_update_script(update_type, strict),
            'lang': 'painless',
            'params': update_fields,
        }

        result = self.es_client.update_by_query(index=self.db_name,
                                                body={'script': update, 'query': query})

        if refresh:
            self.refresh()

        return result

    def update_item(self, item_id, update_fields=None, refresh=False,
                    update_type: Literal['partial', 'full'] = 'partial', strict=False):
        """
        Update a single item in the db by id with update_fields (partial or full update)
        :param item_id: The id of the item to update
        :param update_fields: Fields to update the documents with
        :param refresh: If True, refresh the index after the update
        :param update_type: 'partial' or 'full'
        :param strict: If True, only update fields that already exist in the document
        :return: The result of the update operation
        """
        if update_fields is None:
            update_fields = {}

        update = {
            'lang': 'painless',
            'params': update_fields,
            'source': self._get_update_script(update_type, strict)
        }

        result = self.es_client.update(
            index=self.db_name,
            id=item_id,
            body={'script': update})

        if refresh:
            self.refresh()

        return result

    def delete_item(self, item_id):
        """
        Delete item by id
        :param item_id:
        :return:
        """
        self.es_client.delete(index=self.db_name,
                              id=item_id)
        self.refresh()

    def get_item(self, item_id, ignore_not_found=False):
        try:
            item = self.es_client.get(index=self.db_name,
                                      id=item_id)
            return item['_source']
        except Exception:
            if not ignore_not_found:
                self.log.warning('Item %s not found in index %s', item_id, self.db_name)
            return None

    def get_items(self, item_ids):
        try:
            res = self.es_client.mget(index=self.db_name,
                                      body={'ids': item_ids},
                                      request_timeout=REQUEST_TIMEOUT)
            return [item.get('_source') for item in res['docs']]
        except NotFoundError:
            self.log.warning('Item %s not found in index %s', item_ids, self.db_name)
            return None

    def insert_item(self, item):
        if 'id' not in item:
            raise RuntimeError('id is missing.')

        # always store indexed time in item
        if 'indexed' not in item:
            item['indexed'] = round(time.time() * 1000)

        self.es_client.index(index=self.db_name,
                             id=item['id'],
                             body=item)
        self.refresh()

    def insert_items(self, items, refresh=False, batch_size=1000):
        """
        Bulk index items. refresh = True if wan tto make items searchable immediately
        """
        success, failed, errors = 0, 0, ''
        current_time = round(time.time() * 1000)
        batch = []
        for cnt, item in enumerate(items, start=1):
            # always store indexed time in item
            if 'indexed' not in item:
                item['indexed'] = current_time

            doc_for_index = {
                '_index': self.db_name,
                '_id': item['id'],
                '_source': item
            }

            batch.append(doc_for_index)
            if cnt % batch_size == 0 or cnt == len(items):
                try:
                    success, failed = helpers.bulk(self.es_client, batch, chunk_size=1000,
                                                   request_timeout=REQUEST_TIMEOUT)
                except Exception as e:
                    errors = 'Error with bulk index: %s' % str(e)[0:512]
                    # TODO get correct failed values
                    failed = len(batch)
                    self.log.error(errors)
                batch = []
                if refresh:
                    self.refresh()

        res = {
            'success': success,
            'failed': failed
        }
        if errors:
            res['errors'] = errors
        return res

    def bulk(self, bulk_body, refresh=True):
        res = self.es_client.bulk(index=self.db_name,
                                  body=bulk_body,
                                  request_timeout=REQUEST_TIMEOUT)
        if refresh:
            self.refresh()
        return res

    def iterate_items(self, batch_size=100):
        """
        Return iterator of all items in db
        """
        query = {
            'query': {'match_all': {}},
            'size': batch_size
        }
        for r in self._scroll_items(query):
            yield r

    def query_items(self, q=None, sort=None, start=0, size=100):
        if q is None or q == '*':
            query = {'match_all': {}}
        else:
            query = {
                'query_string': {
                    'query': q,
                    'default_operator': 'AND'
                }
            }

        dsl_query = {
            'query': query,
            'size': size,
            'from': start,
            'track_total_hits': True,
        }
        if sort is not None:
            dsl_query['sort'] = sort

        res = self.es_client.search(index=self.db_name,
                                    body=dsl_query)
        results = []
        total = res.get('hits', {}).get('total')['value']
        for hit in res.get('hits', {}).get('hits', {}):
            results.append(hit['_source'])

        return total, results

    def raw_query(self, query):
        res = self.es_client.search(index=self.db_name,
                                    body=query)
        results = []
        total = res.get('hits', {}).get('total')['value']
        for hit in res.get('hits', {}).get('hits', {}):
            results.append(hit['_source'])

        return total, results

    def scroll_items(self, q=None):
        if q is None or q == '*':
            query = {'match_all': {}}
        else:
            query = {'query_string': {'query': q}}

        return self._scroll_items({
            'query': query,
            'size': 100
        })

    def facets(self, field_name, q=None, size=100, sort_key=False):
        """
        Aggregate by term with simple query filter
        """
        if q is None or q == '*':
            query = {'match_all': {}}
        else:
            query = {'query_string': {'query': q}}

        dsl_query = {
            'query': query,
            'size': 0,
            'aggs': {
                field_name: {
                    'terms': {
                        'field': field_name,
                        'size': size
                    }
                }
            }
        }
        if sort_key:
            dsl_query['aggs'][field_name]['terms']['order'] = {'_key': 'asc'}

        res = self.es_client.search(index=self.db_name,
                                    body=dsl_query)
        results = []
        for agg in res.get('aggregations').get(field_name, {}).get('buckets'):
            results.append(
                {
                    'key': agg['key'],
                    'count': agg['doc_count']
                }
            )

        return results

    def iterate_random_items(self, dsl_query=None, size=1000):
        if dsl_query is None:
            dsl_query = {'match_all': {}}
        dsl_query = {
            "size": size,
            "query": {
                "function_score": {
                    "random_score": {},
                    "query": dsl_query
                }
            }
        }
        res = self.es_client.search(index=self.db_name,
                                    body=dsl_query)
        for hit in res.get('hits', {}).get('hits', {}):
            yield hit['_source']

    def iterate_query_items(self, q=None, sort=None, max_size=None):
        if q is None or q.strip() == '*':
            query = {'match_all': {}}
        else:
            query = {'query_string': {'query': q}}

        dsl_query = {
            'query': query,
            'size': 100
        }
        if sort is not None:
            dsl_query['sort'] = sort
        # use scroll to get all items
        cnt = 0
        for r in self._scroll_items(dsl_query, max_size):
            cnt += 1
            yield r
            if max_size is not None and cnt > max_size:
                print('Reach to max_size', max_size)
                break

    def _scroll_items(self, dsl_query, max_size=None):
        """
        Return iterator of all items in db
        """
        res = self.es_client.search(index=self.db_name,
                                    body=dsl_query,
                                    scroll=SCROLL_SIZE)
        num_items = res.get('hits', {}).get('total', {}).get('value')
        if not num_items:
            yield None
        self.log.info('Start iterate through %s items in %s...', num_items, self.db_name)
        cnt = 0
        for hit in res.get('hits', {}).get('hits', {}):
            cnt += 1
            yield hit['_source']
        scroll_id = res.get('_scroll_id')
        has_next = True
        while has_next:
            has_next = False
            res = self.es_client.scroll(body={'scroll_id': scroll_id}, scroll=SCROLL_SIZE)
            for hit in res.get('hits', {}).get('hits', {}):
                has_next = True
                cnt += 1
                yield hit['_source']
                if max_size is not None and cnt > max_size:
                    has_next = False
                    print('Reach to max_size', max_size)
                    break
            scroll_id = res.get('_scroll_id')

    def size(self):
        return self.es_client.count(index=self.db_name).get('count')

    def count(self, query=None):
        if query is None or query == '*':
            return self.size()
        else:
            dsl_query = {
                'query': {'query_string': {'query': query}}

            }
            return self.es_client.count(index=self.db_name, body=dsl_query).get('count')

    def refresh(self):
        self.es_client.indices.refresh(index=self.db_name, request_timeout=REQUEST_TIMEOUT)

    def scroll_items_by_size(self, dsl_query, scroll_size, max_size=None):
        cnt = 0
        dsl_query["size"] = scroll_size
        for r in self._scroll_items_fast(dsl_query, max_size):
            cnt += len(r)
            if max_size is not None and cnt > max_size:
                print('Reach to max_size', max_size)
                break
            else:
                yield r

    def _scroll_items_fast(self, dsl_query, max_size=None):
        """
        Return iterator of all items in db
        """
        res = self.es_client.search(index=self.db_name,
                                    body=dsl_query,
                                    scroll=SCROLL_SIZE)
        num_items = res.get('hits', {}).get('total', {}).get('value')
        if not num_items:
            yield None
        self.log.info('Start iterate through %s items in %s...', num_items, self.db_name)
        cnt = 0
        hits = [hit['_source']["id"] for hit in res.get('hits', {}).get('hits', {})]
        yield hits
        cnt += len(hits)
        scroll_id = res.get('_scroll_id')
        while True:
            res = self.es_client.scroll(body={'scroll_id': scroll_id}, scroll=SCROLL_SIZE)
            hits = [hit['_source']["id"] for hit in res.get('hits', {}).get('hits', {})]
            if not hits:
                self.log.info('Stop iterate through %s items in %s...', num_items, self.db_name)
                break
            hits = [hit['_source']["id"] for hit in res.get('hits', {}).get('hits', {})]
            cnt += len(hits)
            if max_size is not None and cnt > max_size:
                print('Reach to max_size', max_size)
                break
            else:
                yield hits
            scroll_id = res.get('_scroll_id')

    def raw_search(self, query):
        return self.es_client.search(index=self.db_name,
                                     body=query)
