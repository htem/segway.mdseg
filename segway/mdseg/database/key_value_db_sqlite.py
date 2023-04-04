import sqlite3
from threading import Lock, RLock
import pickle

import logging
logger = logging.getLogger(__name__)

from .key_value_db import KeyValueDatabase

class KeyValueDatabaseSQLite(KeyValueDatabase):
    def __init__(self, db_file, collection_name):
        self.db_file = db_file
        self.collection_name = collection_name
        self.lock = Lock()
        super().__init__()

    def connect(self):
        super().connect()
        self.con = sqlite3.connect(self.db_file, check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()

    def close(self):
        super().close()
        self.cur.close()
        self.con.close()
        self.cur = None
        self.con = None

    # def _create_index(self, index_list):
        # if db_col_name not in self.database.list_collection_names():
        #     self.superfragments.create_index(
        #         [
        #             ('id', ASCENDING)
        #         ],
        #         name='id', unique=True)
    def _create_index(self):
        with self.lock:
            self.cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS id on {self.collection_name} (id)')

    @staticmethod
    def __unpack_string(data):
        if type(data) in [int, bool, str]:
            return data
        elif type(data) is bytes:
            return pickle.loads(data)
        raise RuntimeError(f'Unhandled unpacking {data} of type {type(data)}')

    @staticmethod
    def __unpack_entry(packed):
        unpacked = {}
        for k in packed.keys():
            unpacked[k] = __class__.__unpack_string(packed[k])
        return unpacked

    # def _get(self, key_list):
    #     return [k for k in self.superfragments.find({'id': {'$in': key_list}})]
    def _get_list(self, key_list):
        """Unfound keys are silently ignored"""
        ret = []
        for k in key_list:
            res = self.cur.execute(f"SELECT * FROM {self.collection_name} "
                                     "WHERE id=?", (k,)).fetchone()
            if res is not None:
                ret.append(self.__unpack_entry(res))
        return ret

    def _get(self, key):
        res = self.cur.execute(f"SELECT * FROM {self.collection_name} "
                                 "WHERE id=?", (key,)).fetchone()
        if res is None:
            return None
        return self.__unpack_entry(res)

    def _set(self, kv_list):
        raise RuntimeError("To be implemented")
        with self.lock:
            pass
