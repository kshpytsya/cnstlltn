import botocore
import boto3
import io
from zope.interface import implementer
from .statestorage_intf import IStateStorage


@implementer(IStateStorage)
class AwsStateStorage:
    def __init__(self, s3_bucket, s3_key, dynamodb_lock_table, timeout=10):
        self._s3_bucket_name = s3_bucket
        self._s3_key = s3_key
        self._dynamodb_lock_table = dynamodb_lock_table
        self._timeout = timeout
        self._opened = False

        self._s3_bucket = boto3.resource('s3').Bucket(self._s3_bucket_name)

    def open_and_read(self, read_cb):
        assert not self._opened

        if self._s3_bucket.creation_date is None:
            raise KeyError("No such bucket: %s" % self._s3_bucket_name)

        try:
            with io.BytesIO() as f:
                try:
                    self._s3_bucket.download_fileobj(self._s3_key, f)
                    f.seek(0)
                    read_cb(f)
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        read_cb(None)
                    else:
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

    def write(self, write_cb):
        assert self._opened

        with io.StringIO() as f:
            write_cb(f)

            self._s3_bucket.put_object(Key=self._s3_key, Body=f.getvalue().encode())
