import logging

from pymongo import MongoClient, ASCENDING, DESCENDING, ReplaceOne
from pymongo.errors import BulkWriteError, WriteError

from .neuron import Neuron

logger = logging.getLogger(__name__)

class NeuronDBServerMongoDB():

    def __init__(
            self,
            db_name,
            host,
            neuron_collection='neurons',
            segment_collection='segments',
            ):

        self.db_name = db_name
        self.host = host
        self.counts = {}
        self.neurons_collection_name = neuron_collection
        self.backup_collection_name = neuron_collection + '_bak'
        self.segment_collection_name = segment_collection + '_bak'

        self.segment_map_cache = {}
        # self.segment_map_cache_neurons = set()

        try:
            self.__connect()
            self.__open_db()
            self.__open_collections()

            # collection_names = self.database.list_collection_names()
            # if neuron_collection not in collection_names:
            self.__create_neuron_collection()
            self.__create_backup_collection()
            self.__create_segment_collection()

        finally:

            self.__disconnect()

    def connect(self):
        self.__connect()
        self.__open_db()
        self.__open_collections()

    def __create_segment_collection(self):

        indexes = self.segments.index_information()

        if 'segment_id' not in indexes:
            self.segments.create_index(
                [
                    ('segment_id', DESCENDING)
                ],
                name='segment_id',
                unique=True
                )

    def __create_backup_collection(self):

        indexes = self.backup_collection.index_information()

        if 'neuron_name' not in indexes:
            self.backup_collection.create_index(
                [
                    ('neuron_name', ASCENDING)
                ],
                name='neuron_name',
                unique=True
                )

    def __create_neuron_collection(self):
        '''Creates the neuron collection, including indexes'''

        indexes = self.neurons.index_information()
        if 'neuron_name' not in indexes:
            self.neurons.create_index(
                [
                    ('neuron_name', DESCENDING)
                ],
                name='neuron_name',
                unique=True
                )
        if 'name_prefix' not in indexes:
            self.neurons.create_index(
                [
                    ('name_prefix', DESCENDING)
                ],
                name='name_prefix',
                )
        if 'tags' not in indexes:
            self.neurons.create_index(
                [
                    ('tags', ASCENDING)
                ],
                name='tags',
                )
        if 'location_tags' not in indexes:
            self.neurons.create_index(
                [
                    ('location_tags', ASCENDING)
                ],
                name='location_tags',
                )
        if 'cell_type' not in indexes:
            self.neurons.create_index(
                [
                    ('cell_type', ASCENDING)
                ],
                name='cell_type',
                )
        if 'annotator' not in indexes:
            self.neurons.create_index(
                [
                    ('annotator', ASCENDING)
                ],
                name='annotator',
                )
        if 'finished' not in indexes:
            self.neurons.create_index(
                [
                    ('finished', ASCENDING)
                ],
                name='finished',
                )
        if 'reviewed' not in indexes:
            self.neurons.create_index(
                [
                    ('reviewed', ASCENDING)
                ],
                name='reviewed',
                )
        if 'parent_segment' not in indexes:
            self.neurons.create_index(
                [
                    ('parent_segment', ASCENDING)
                ],
                name='parent_segment',
                )
        if 'soma_loc' not in indexes:
            self.neurons.create_index(
                [
                    ('z', ASCENDING),
                    ('y', ASCENDING),
                    ('x', ASCENDING),
                ],
                name='soma_loc')

    def exists_neuron(self, neuron_name, backup=False):
        collection = self.backup_collection if backup else self.neurons
        return collection.count_documents({'neuron_name': neuron_name}) > 0

    def get_neuron(self, neuron_name, with_children=True, backup=False):

        collection = self.backup_collection if backup else self.neurons

        item = collection.find_one({'neuron_name': neuron_name})
        if item is None:
            raise RuntimeError(f'Neuron {neuron_name} does not exist in db')

        prev = Neuron.from_dict(item)
        neuron = Neuron(name=neuron_name, prevNeuron=prev)

        segs = set(neuron.segments)
        blacklist_segments = set(neuron.blacklist_segments)
        children = []
        children_segments = set()
        segments_by_children = {}
        children_blacklist = set()
        for c in collection.find({'parent_segment': neuron_name}):
            segments_by_children[c['neuron_name']] = c['segments']
            child_segments = set([int(n) for n in c['segments']])

            self.__add_segment_map(child_segments, c['neuron_name'])

            children_segments |= child_segments
            children_blacklist |= set([int(n) for n in c['blacklist_segments']])
            children.append(c['neuron_name'])

        segs_without_children = segs - children_segments
        self.__add_segment_map(segs_without_children, neuron.name)

        if with_children:
            segs |= children_segments
            blacklist_segments |= children_blacklist
        else:
            segs = segs_without_children
            blacklist_segments -= children_blacklist

        neuron.segments = list(segs)
        neuron.blacklist_segments = list(blacklist_segments)
        neuron.children = list(set(children))
        neuron.children_segments = list(children_segments)
        neuron.segments_by_children = segments_by_children

        return neuron

    def find_neuron(self, query):
        res = self.neurons.find(
            query,
            limit=10000,
            )
        ret = []
        for item in res:
            ret.append(item['neuron_name'])
        return ret

    def find_neuron_filtered(self, query):
        res = self.find_neuron(query)

        query_type = query.get('cell_type', None)
        if query_type:
            filtered_res = []
            for item in res:
                if query_type != 'axon' and '.axon' in item:
                    continue
                if query_type != 'dendrite' and '.dendrite' in item:
                    continue
                filtered_res.append(item)

            res = filtered_res
        return res

    def find_neuron_with_segment_id(self, segment_id):
        item = self.segments.find_one({'segment_id': segment_id})
        if item:
            return item['neuron_name']
            # print(item)
            # name = item['neuron_name']
            # if '.' in name:
            #     return name.rsplit('.')[0]
            # else:
            #     return name
        else:
            return None

    # def find_neuron_prefix(self, prefix):
    #     res = self.neurons.find(
    #         {'name_prefix': prefix},
    #         limit=100,
    #         )
    #     ret = []
    #     for item in res:
    #         ret.append(item['neuron_name'])
    #     return ret

    def _create_neuron_name_with_prefix(self, prefix):

        assert prefix[-1] == '_'
        # assert len(prefix.split('_')) == 2, "Only one `_` is allowed in neuron name prefix to avoid ambiguity"
        prefix = prefix[:-1]

        if prefix not in self.counts:
            next_num = self.neurons.count({'name_prefix': prefix})
            self.counts[prefix] = next_num
        else:
            next_num = self.counts[prefix]

        neuron_name = '%s_%d' % (prefix, next_num)
        print("neuron_name:", neuron_name)

        # make sure this is a new name
        while self.exists_neuron(neuron_name):
            # next_num *= 2
            next_num += 1
            neuron_name = '%s_%d' % (prefix, next_num)
            print("neuron_name:", neuron_name)

            if next_num > 1024*1024:
                raise RuntimeError(
                    "next_num for prefix %s exceeded %d!!!" % (
                        prefix, 1024*1024))

        return neuron_name

    def create_neuron(self, name, no_check=False):

        if not no_check:
            assert not self.exists_neuron(name)
        return Neuron(name=name)

    def create_neuron_with_prefix(self, prefix):

        neuron_name = self._create_neuron_name_with_prefix(prefix)
        return Neuron(name=neuron_name)

    def check_neuron_mapping(self, neuron, override_assignment=False):

        if not neuron.is_finalized():
            return (False, "Neuron not finalized")

        # find children (if any) and remove overlapping segments from the parent segment
        # NOTE: will only work with a 2-level hierarchy
        neuron_name = neuron.name
        segs = set(neuron.segments)

        segs -= set(neuron.children_segments)
        neuron.segments = list(segs)

        if not override_assignment:
            for s in neuron.segments:
                mapping = self.__check_segment_belongs_to_neuron(s, neuron_name)
                if mapping is not True:
                    return (False, "Fragment %s already assigned to %s" % (s, mapping))

        neuron.checked_mapping = True
        return (True, "")

    def save_neuron(self, neuron):

        assert neuron.checked_mapping

        if neuron.prevNeuron is not None and neuron.is_substantially_modified():
            self.__save_backup(neuron.prevNeuron)

        self.__save_neuron(neuron)

        # at this point, neuron.segments list is guaranteed to not contain
        # its children segments
        self.__map_segments_to_neuron(neuron.segments, neuron.name)

    def __map_segments_to_neuron(self, segment_ids, neuron_id):
        # segment_id = int(segment_id)
        segment_ids = [int(i) for i in segment_ids]

        to_add = []
        for sid in segment_ids:
            # self.__cache_segment_map(sid)
            existing_mapping = self.__get_segment_map(sid)
            # if self.segment_map_cache[sid] == neuron_id:
            if existing_mapping == neuron_id:
                pass  # no need to update this segment
            else:
                to_add.append(sid)

        print(f'to_add: {to_add}')

        # now we need to update segments database
        # always update with the latest copy
        items = []
        for sid in to_add:
            item = self.segments.find_one({'segment_id': sid})
            if item is None:
                item = {'segment_id': sid}
            item['neuron_name'] = neuron_id
            items.append(item)

        if len(items):
            try:
                # write mapping. overwrite previous data (if any)
                self.__write(
                    self.segments,
                    ['segment_id'],
                    items
                    )
            except BulkWriteError as e:
                logger.error(e.details)
                raise

        # finally update cache
        self.__add_segment_map(segment_ids, neuron_id)
        # for sid in segment_ids:
        #     self.segment_map_cache[sid] = neuron_id

    # def __check_segment_belongs_to_neuron(self, segment_id, neuron_id):
    #     segment_id = int(segment_id)
    #     self.__cache_segment_map(segment_id)
    #     mapping = self.segment_map_cache[segment_id]
    #     if mapping == neuron_id or mapping is None or mapping == '' or mapping in neuron_id:
    #         return True
    #     else:
    #         return mapping

    def __check_segment_belongs_to_neuron(self, segment_id, neuron_id):
        segment_id = int(segment_id)
        mapping = self.__get_segment_map(segment_id)
        # check if (1) mapped to the same neuron, (2) unmapped, or (3) is mapped to parent
        if mapping == neuron_id or mapping is None or mapping in neuron_id:
            return True
        else:
            return mapping

    # def __cache_segment_map(self, segment_id):
    #     if segment_id not in self.segment_map_cache:
    #         item = self.segments.find_one({'segment_id': segment_id})
    #         if item is None:
    #             self.segment_map_cache[segment_id] = ''
    #         else:
    #             self.segment_map_cache[segment_id] = item['neuron_name']

    def __add_segment_map(self, segment_ids, neuron_id):
        for seg in segment_ids:
            self.segment_map_cache[seg] = neuron_id

    def __get_segment_map(self, segment_id):
        if segment_id not in self.segment_map_cache:
            item = self.segments.find_one({'segment_id': segment_id})
            if item is None:
                self.segment_map_cache[segment_id] = None
            else:
                self.segment_map_cache[segment_id] = item['neuron_name']
        return self.segment_map_cache[segment_id]

    def __save_neuron(self, neuron):

        logger.info("Saving neuron as %s" % neuron.name)

        item = neuron.to_json()
        # print(item)

        try:
            self.__write(
                self.neurons,
                ['neuron_name'],
                [item]
                )

        except BulkWriteError as e:
            logger.error(e.details)
            raise

    def __save_backup(self, neuron):

        neuron.name = neuron.get_backup_name()
        logger.info("Saving backup as %s" % neuron.name)

        try:
            self.__write(
                self.backup_collection,
                ['neuron_name'],
                [neuron.to_json()],
                fail_if_exists=True)

        except BulkWriteError as e:
            logger.error(e.details)
            raise

    def __write(self, collection, match_fields, docs,
                fail_if_exists=False, fail_if_not_exists=False, delete=False):
        '''Writes documents to provided mongo collection, checking for restricitons.
        Args:
            collection (``pymongo.collection``):
                The collection to write the documents into.
            match_fields (``list`` of ``string``):
                The set of fields to match to be considered the same document.
            docs (``dict`` or ``bson``):
                The documents to insert into the collection
            fail_if_exists, fail_if_not_exists, delete (``bool``):
                see write_nodes or write_edges for explanations of these flags
            '''
        assert not delete, "Delete not implemented"
        match_docs = []
        for doc in docs:
            match_doc = {}
            for field in match_fields:
                match_doc[field] = doc[field]
            match_docs.append(match_doc)

        if fail_if_exists:
            self.__write_fail_if_exists(collection, match_docs, docs)
        elif fail_if_not_exists:
            self.__write_fail_if_not_exists(collection, match_docs, docs)
        else:
            self.__write_no_flags(collection, match_docs, docs)

    def __write_no_flags(self, collection, old_docs, new_docs):
        bulk_query = [ReplaceOne(old, new, upsert=True)
                      for old, new in zip(old_docs, new_docs)]
        collection.bulk_write(bulk_query)

    def __write_fail_if_exists(self, collection, old_docs, new_docs):
        for old in old_docs:
            if collection.count_documents(old):
                raise WriteError(
                        "Found existing doc %s and fail_if_exists set to True."
                        " Aborting write for all docs." % old)
        collection.insert_many(new_docs)

    def __write_fail_if_not_exists(self, collection, old_docs, new_docs):
        for old in old_docs:
            if not collection.count_documents(old):
                raise WriteError(
                        "Did not find existing doc %s and fail_if_not_exists "
                        "set to True. Aborting write for all docs." % old)
        bulk_query = [ReplaceOne(old, new, upsert=False)
                      for old, new in zip(old_docs, new_docs)]
        result = collection.bulk_write(bulk_query)
        assert len(new_docs) == result.matched_count,\
            ("Supposed to replace %s docs, but only replaced %s"
                % (len(new_docs), result.matched_count))

    def __connect(self):
        '''Connects to Mongo client'''
        self.client = MongoClient(self.host) 

    def __open_db(self):
        '''Opens Mongo database'''
        self.database = self.client[self.db_name]

    def __open_collections(self):
        '''Opens the node, edge, and meta collections'''

        self.neurons = self.database[self.neurons_collection_name]
        self.segments = self.database[self.segment_collection_name]
        self.backup_collection = self.database[self.backup_collection_name]

    def __disconnect(self):
        '''Closes the mongo client and removes references
        to all collections and databases'''
        self.nodes = None
        self.edges = None
        self.meta = None
        self.database = None
        self.client.close()
        self.client = None

    def close(self):
        self.__disconnect()
