import filelock
import os
from atomicwrites import atomic_write
from zope.interface import implementer
from .statestorage_intf import IStateStorage


@implementer(IStateStorage)
class FileStateStorage:
    def __init__(self, path, *, timeout=1):
        self._path = path
        self._lock = filelock.FileLock(path + '.lock')
        self._timeout = timeout

    def open_and_read(self, read_cb):
        assert not self._lock.is_locked
        self._lock.acquire(timeout=self._timeout)

        try:
            if os.path.exists(self._path):
                with open(self._path) as f:
                    read_cb(f)
            else:
                read_cb(None)

        except:  # noqa: E722
            self._lock.release()
            raise

    def close(self):
        assert self._lock.is_locked
        self._lock.release()

    def write(self, write_cb):
        assert self._lock.is_locked

        with atomic_write(self._path, overwrite=True) as f:
            write_cb(f)
