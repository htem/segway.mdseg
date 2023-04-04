import logging
import sqlite3
from threading import Lock, RLock
import pickle

from .neuron_db import NeuronDBServer
from .neuron import Neuron

logger = logging.getLogger(__name__)

class NeuronDBServerSQLite(NeuronDBServer):

    def __init__(
            self,
            db_url,
            neuron_collection='neurons',
            segment_collection='segments',
            ):

        self.db_url = db_url
        self.con = None
        self.cur = None
        # keep a lock for serializing mutable operations
        self.lock = Lock()

        NeuronDBServer.__init__(self, db_url, neuron_collection, segment_collection)
        # todo: add backup collection

    def _create_index(self, index_list, unique):
        for k in index_list:
            if k in unique:
                self.cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS {k} on {self.neuron_collection} ({k})')
            else:
                self.cur.execute(f'CREATE INDEX IF NOT EXISTS {k} on {self.neuron_collection} ({k})')

    def _connect(self):
        self.con = sqlite3.connect(self.db_url, check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()

    def _check_collections(self, collections):
        res = self.cur.execute("SELECT name FROM sqlite_master").fetchall()
        res = [k['name'] for k in res]
        for c in collections:
            assert c in res, f"Collection {c} not found in db"

    def _close(self):
        self.con.close()

    def _get_neuron(self, neuron_name, backup=False):
        collection = self.backup_collection if backup else self.neuron_collection
        res = self.cur.execute(f"SELECT * FROM {collection} WHERE neuron_name=?", [neuron_name]).fetchone()
        if res is None:
            return None
        return self.__unpack_entry(res)

    def _exists_neuron(self, neuron_name, backup=False):
        collection = self.backup_collection if backup else self.neuron_collection
        res = self.cur.execute(f"SELECT COUNT(1) FROM {collection} WHERE neuron_name=?", [neuron_name]).fetchone()
        return res['COUNT(1)'] == 1

    def _get_children_of(self, neuron_name, backup=False):
        collection = self.backup_collection if backup else self.neuron_collection
        res = self.cur.execute(f"SELECT * FROM {collection} WHERE parent_segment=?", [neuron_name])
        ret = []
        for item in res:
            ret.append(__class__.__unpack_entry(item))
        return ret

    @staticmethod
    def __unpack_string(data):
        if type(data) in [int, bool, str]:
            return data
        elif type(data) == bytes:
            return pickle.loads(data)
        raise RuntimeError(f'Unhandled unpacking {data} of type {type(data)}')

    @staticmethod
    def __unpack_entry(packed):
        unpacked = {}
        for k in packed.keys():
            unpacked[k] = __class__.__unpack_string(packed[k])
        return unpacked

    def _find_neuron(self, query):
        command = f"SELECT neuron_name FROM {self.neuron_collection}"
        query_data = []
        if len(query) > 0:
            command += ' WHERE'
            for k, v in query.items():
                command += ' {k}=?'
                query_data.append(v)
        res = self.cur.execute(command, query_data)
        ret = [v[0] for v in res.fetchall()]
        return ret

    def _get_mapped_neuron(self, segment_id):
        item = self.cur.execute(f"SELECT * FROM {self.segment_collection} "
                                 "WHERE segment_id=?", [segment_id]).fetchone()
        return item['neuron_name'] if item else None

    def _modify_segment_map(self, segment_ids, neuron_id):
        # todo: write more succicntly
        items = []
        for sid in segment_ids:
            items.append((sid,))
            # items.append((neuron_id, sid))
        if len(items):
            # self.cur.executemany(f"UPDATE {self.segment_collection}"
            #                      f"SET neuron_name = {neuron_id}"
            #                       "WHERE segment_id = ?", items)
            self.cur.executemany(
                f"INSERT INTO {self.segment_collection}(segment_id, neuron_name) "
                f"VALUES (?, {neuron_id}) "
                f"ON CONFLICT(segment_id) DO UPDATE SET neuron_name={neuron_id}", items
                )

    def _count_prefix_in_db(prefix):
        res = self.cur.execute(f"SELECT COUNT(*) FROM {self.neuron_collection} "
                                 "WHERE name_prefix=?", [prefix])
        return res
