import json
from .statestorage_intf import IStateStorage


class StateStorageContext(dict):
    def __init__(self, storage, data):
        """
        Instances of this class are created by StateStorage.__enter__.
        This constructor is not a part of public interface.
        """
        self._storage = storage
        self.update(data)

    def write(self):
        self._storage._backend.write(lambda f: json.dump(self, f, indent=4, sort_keys=True))


class StateStorage:
    def __init__(self, *, backend):
        self._backend = IStateStorage(backend)
        self._context = None

    def __enter__(self):
        assert self._context is None

        data = None

        def load_cb(f):
            nonlocal data
            if f:
                data = json.load(f)
            else:
                data = {}

        self._backend.open_and_read(load_cb)
        try:
            self._context = StateStorageContext(self, data)
            return self._context
        except:  # noqa: E722
            self._backend.close()
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._context is not None

        self._context = None
        self._backend.close()
        return None
