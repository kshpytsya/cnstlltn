import botocore
import boto3
import io
import json
import types
from zope.interface import implementer
from .statestorage_intf import IStateStorage


@implementer(IStateStorage)
class AwsStateStorage:
    def __init__(self, s3_bucket, s3_key, dynamodb_lock_table):
        self._s3_bucket_name = s3_bucket
        self._s3_key = s3_key
        self._state = {}
        self._opened = False

        self._s3_bucket = boto3.resource('s3').Bucket(self._s3_bucket_name)

    @property
    def state(self):
        return types.MappingProxyType(self._state)

    def open(self, *, timeout):
        assert not self._opened

        if self._s3_bucket.creation_date is None:
            raise KeyError("No such bucket: %s" % self._s3_bucket_name)

        try:
            with io.BytesIO() as f:
                try:
                    self._s3_bucket.download_fileobj(self._s3_key, f)
                    f.seek(0)
                    self._state = json.load(f)
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] != "404":
                        raise

            self._opened = True

            return self

        except:  # noqa: E722
            # self._lock.release()
            raise

    def close(self):
        assert self._opened
        self._opened = False
        # self._lock.release()

    def set(self, key, value=None):
        assert self._opened
        assert isinstance(key, str)

        if value is None:
            self._state.pop(key, None)
        else:
            self._state[key] = value

        if self._state:
            self._s3_bucket.put_object(
                Key=self._s3_key,
                Body=json.dumps(self._state, indent=4, sort_keys=True)
            )
        else:
            self._s3_bucket.delete_objects(Delete=dict(Objects=[dict(Key=self._s3_key)]))
