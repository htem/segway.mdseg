
class KeyValueDatabase(object):
    def __init__(self):
        self.connect()
        self._create_index()

    def connect(self):
        pass

    def close(self):
        pass

    def _create_index(self):
        raise RuntimeError("To be implemented")

    def get(self, key_list):
        return self._get(key_list)

    def _get(self, key_list):
        raise RuntimeError("To be implemented")

    def get_list(self, key_list):
        return self._get_list(key_list)

    def _get_list(self, key_list):
        raise RuntimeError("To be implemented")

    def set(self, kv_list):
        return self._set(kv_list)

    def _set(self, kv_list):
        raise RuntimeError("To be implemented")
