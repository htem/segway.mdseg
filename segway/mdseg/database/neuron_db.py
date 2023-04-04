import logging

from .neuron import Neuron

logger = logging.getLogger(__name__)

class NeuronDBServer():

    def __init__(
            self,
            db_url,
            neuron_collection='neurons',
            segment_collection='segments',
            ):

        self.neuron_collection = neuron_collection
        self.backup_collection = neuron_collection + '_bak'
        self.segment_collection = segment_collection
        self.__segment_map_cache = {}
        self.__counts = {}

        self._connect()
        # check names
        self._check_collections([neuron_collection, segment_collection])
        # todo: add backup collection

        indices = ['neuron_name', 'name_prefix', 'annotator', 'cell_type', 'parent_segment']
        unique_indices = ['neuron_name']
        self._create_index(indices, unique=unique_indices)

    def _connect(self):
        raise RuntimeError("To be implemented by derived class")

    def _check_collections(self, collections):
        raise RuntimeError("To be implemented by derived class")

    def _close(self):
        raise RuntimeError("To be implemented by derived class")

    def connect(self):
        self._connect()

    def close(self):
        self._close()

    def _get_neuron(self, neuron_name, backup=False):
        raise RuntimeError("To be implemented by derived class")

    def _exists_neuron(self, neuron_name, backup=False):
        raise RuntimeError("To be implemented by derived class")

    def _get_children_of(self, neuron_name, backup=False):
        raise RuntimeError("To be implemented by derived class")

    def _find_neuron(self, query):
        raise RuntimeError("To be implemented by derived class")

    def _get_mapped_neuron(self, segment_id):
        raise RuntimeError("To be implemented by derived class")

    def _modify_segment_map(self, segment_ids, neuron_id):
        raise RuntimeError("To be implemented by derived class")

    def _count_prefix_in_db(prefix):
        raise RuntimeError("To be implemented by derived class")

    def exists_neuron(self, neuron_name, backup=False):
        return self._exists_neuron(neuron_name, backup)

    def find_neuron(self, query):
        return self._find_neuron(query)

    def find_neuron_filtered(self, query):
        res = self._find_neuron(query)
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

    def get_neuron(self, neuron_name, with_children=True, backup=False):

        item = self._get_neuron(neuron_name, backup)
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

        for c in self._get_children_of(neuron_name):
            segments_by_children[c['neuron_name']] = c['segments']
            child_segments = set([int(n) for n in c['segments']])

            self.__update_segment_map_cache(child_segments, c['neuron_name'])

            children_segments |= child_segments
            children_blacklist |= set([int(n) for n in c['blacklist_segments']])
            children.append(c['neuron_name'])

        segs_without_children = segs - children_segments
        self.__update_segment_map_cache(segs_without_children, neuron.name)

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

    def __update_segment_map_cache(self, segment_ids, neuron_id):
        for seg in segment_ids:
            self.__segment_map_cache[seg] = neuron_id

    def __get_segment_map_cached(self, segment_id):
        if segment_id not in self.__segment_map_cache:
            nid = self._get_mapped_neuron(segment_id)
            if nid is None:
                self.__segment_map_cache[segment_id] = None
            else:
                self.__segment_map_cache[segment_id] = nid
        return self.__segment_map_cache[segment_id]

    def find_neuron_with_segment_id(self, segment_id):
        """
        Ret: str | None
            Returns nid if found, else None
        """
        return self._get_mapped_neuron(segment_id)


    def __create_neuron_name_with_prefix(self, prefix):

        assert prefix[-1] == '_'
        prefix = prefix[:-1]

        if prefix not in self.__counts:
            next_num = self._count_prefix_in_db(prefix)
            self.__counts[prefix] = next_num
        else:
            next_num = self.__counts[prefix]

        neuron_name = '%s_%d' % (prefix, next_num)
        print("neuron_name:", neuron_name)

        # make sure this is a new name
        while self._exists_neuron(neuron_name):
            next_num += 1
            neuron_name = '%s_%d' % (prefix, next_num)
            print("neuron_name1:", neuron_name)

        return neuron_name

    def create_neuron(self, name, no_check=False):

        if not no_check:
            assert not self._exists_neuron(name)
        return Neuron(name=name)

    def create_neuron_with_prefix(self, prefix):

        neuron_name = self.__create_neuron_name_with_prefix(prefix)
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
        '''Mapping segments to a named object in the sid2nid database'''
        segment_ids = [int(i) for i in segment_ids]
        # First we filter out sids that are already mapped to `neuron_id`
        to_add = []
        for sid in segment_ids:
            existing_mapping = self.__get_segment_map_cached(sid)
            if existing_mapping == neuron_id:
                pass  # no need to update this segment
            else:
                to_add.append(sid)
        print(f'to_add: {to_add}')
        # update database
        self._modify_segment_map(segment_ids, neuron_id)
        # and also cache
        self.__update_segment_map_cache(segment_ids, neuron_id)

    def __check_segment_belongs_to_neuron(self, segment_id, neuron_id):
        segment_id = int(segment_id)
        mapping = self.__get_segment_map_cached(segment_id)
        # check if (1) mapped to the same neuron, (2) unmapped, or (3) is mapped to parent
        if mapping == neuron_id or mapping is None or mapping in neuron_id:
            return True
        else:
            return mapping

    def __save_neuron(self, neuron):
        assert False, "SQL unsupported"
        logger.info("Saving neuron as %s" % neuron.name)
        item = neuron.to_json()
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
        assert False, "SQL unsupported"
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
        assert False, "SQL unsupported"
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
        assert False, "SQL unsupported"
        bulk_query = [ReplaceOne(old, new, upsert=True)
                      for old, new in zip(old_docs, new_docs)]
        collection.bulk_write(bulk_query)

    def __write_fail_if_exists(self, collection, old_docs, new_docs):
        assert False, "SQL unsupported"
        for old in old_docs:
            if collection.count_documents(old):
                raise WriteError(
                        "Found existing doc %s and fail_if_exists set to True."
                        " Aborting write for all docs." % old)
        collection.insert_many(new_docs)

    def __write_fail_if_not_exists(self, collection, old_docs, new_docs):
        assert False, "SQL unsupported"
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

    # def __connect(self):
    #     '''Connects to Mongo client'''
    #     self.client = MongoClient(self.host) 

    # def __open_collections(self):
    #     '''Opens the node, edge, and meta collections'''
    #     self.neurons = self.database[self.neuron_collection]
    #     self.segments = self.database[self.segment_collection]
    #     self.backup_collection = self.database[self.backup_collection]

    # def __disconnect(self):
    #     '''Closes the mongo client and removes references
    #     to all collections and databases'''
    #     self.nodes = None
    #     self.edges = None
    #     self.meta = None
    #     self.database = None
    #     self.client.close()
    #     self.client = None

    # def close(self):
    #     self.__disconnect()
