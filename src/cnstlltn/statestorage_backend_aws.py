import botocore
import boto3
import io
from python_dynamodb_lock.python_dynamodb_lock import DynamoDBLockClient
from zope.interface import implementer
from .statestorage_intf import IStateStorage


@implementer(IStateStorage)
class AwsStateStorage:
    def __init__(self, s3_bucket, s3_key, locking=True):
        self._s3_bucket_name = s3_bucket
        self._s3_key = s3_key
        # self._timeout = timeout
        self._opened = False

        self._s3_bucket = boto3.resource('s3').Bucket(self._s3_bucket_name)
        if self._s3_bucket.creation_date is None:
            raise KeyError("No such bucket: %s" % self._s3_bucket_name)

        ddb_client = boto3.client('dynamodb')
        try:
            DynamoDBLockClient.create_dynamodb_table(ddb_client)
        except ddb_client.exceptions.ResourceInUseException:
            # the table already exists -- ok with us
            pass

        if locking:
            self._lock_client = DynamoDBLockClient(boto3.resource('dynamodb'))
            self._lock = None
        else:
            self._lock_client = None

    def open_and_read(self, read_cb):
        assert not self._opened

        if self._lock_client:
            self._lock = self._lock_client.acquire_lock("{}/{}".format(self._s3_bucket_name, self._s3_key))

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
            if self._lock_client:
                self._lock.release()

            raise

    def close(self):
        assert self._opened
        self._opened = False
        if self._lock_client:
            self._lock.release()
            self._lock_client.close()

    def write(self, write_cb):
        assert self._opened

        with io.StringIO() as f:
            write_cb(f)

            self._s3_bucket.put_object(Key=self._s3_key, Body=f.getvalue().encode())
