import filelock
import json
import os
import types
from atomicwrites import atomic_write
from zope.interface import implementer
from .statestorage_intf import IStateStorage


@implementer(IStateStorage)
class FileStateStorage:
    def __init__(self, path):
        self._path = path
        self._lock = filelock.FileLock(path + '.lock')
        self._state = {}

    @property
    def state(self):
        return types.MappingProxyType(self._state)

    def open(self, *, timeout):
        assert not self._lock.is_locked
        self._lock.acquire(timeout=timeout)

        try:
            if os.path.exists(self._path):
                with open(self._path) as f:
                    self._state = json.load(f)

            return self

        except:  # noqa: E722
            self._lock.release()
            raise

    def close(self):
        assert self._lock.is_locked
        self._lock.release()

    def set(self, key, value=None):
        assert self._lock.is_locked
        assert isinstance(key, str)

        if value is None:
            self._state.pop(key, None)
        else:
            self._state[key] = value

        if self._state:
            with atomic_write(self._path, overwrite=True) as f:
                json.dump(self._state, f, indent=4, sort_keys=True)
        else:
            if os.path.exists(self._path):
                os.unlink(self._path)
