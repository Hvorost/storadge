from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from tqdm import tqdm


class SearchIndexUpdater:
    def __init__(self, es_hosts, index_name, doc_type='item', scroll_ttl='5m'):
        self._es = Elasticsearch(hosts=es_hosts)
        self.index_name = index_name
        self.doc_type = doc_type
        self.scroll_ttl = scroll_ttl

    @property
    def mapping(self):
        return self._es.indices.get_mapping(doc_type=self.doc_type, index=self.index_name)

    def update_mapping(self, mapping_fragment):
        resp = self._es.indices.put_mapping(doc_type=self.doc_type, index=self.index_name, body=mapping_fragment)
        return resp['acknowledged']

    def index_item(self, item):
        self._es.update(index=self.index_name, doc_type=self.doc_type, id=item[0], body={"doc": item[1]})

    def index_items(self, items, verbose=False, raise_on_error=False):
        update_actions = map(lambda x: self._prepare_update(x[0], x[1]), items)
        items_count = len(items) if hasattr(items, '__len__') else None

        errors = []
        wrapped_process = tqdm(parallel_bulk(self._es, actions=update_actions, raise_on_error=raise_on_error),
                               disable=not verbose,
                               total=items_count,
                               postfix={'errors': len(errors)})
        for success, response in wrapped_process:
            if not success:
                errors.append(response)
                if verbose:
                    wrapped_process.set_postfix({'errors': len(errors)})
        return errors

    def refresh(self):
        return self._es.indices.refresh(index=self.index_name)

    def _prepare_update(self, item_id, update_doc):
        return {
            "_index": self.index_name,
            "_type": self.doc_type,
            "_op_type": 'update',
            "_id": int(item_id),
            "doc": update_doc
        }
