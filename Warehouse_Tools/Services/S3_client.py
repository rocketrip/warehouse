from boto3 import client, session
import os
import io
from pandas.core.frame import DataFrame
import pandas as pd
from botocore.exceptions import ClientError



class SimpleStorage:
    def __init__(self, bucket_name, profile_name=None, **kwargs):
        if not profile_name:
            aws_key = kwargs.get(
                "aws_access_key_id", os.environ.get('ACCESS_KEY')
            )
            aws_secret = kwargs.get(
                "aws_secret_access_key", os.environ.get('SECRET_KEY')
            )
            self.conn = client(
                service_name="s3",
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret

            )
        else:
            self.session = session.Session(
                profile_name=profile_name
            )
            self.conn = self.session.client(
                service_name="s3"
            )
        self.bucket_name = bucket_name

    def get_objects(self, delimiter="/"):
        if delimiter:
            return self.conn.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter=delimiter
            )
        return self.conn.list_objects_v2(
            Bucket=self.bucket_name,

        )

    def get_keys(self):
        try:
            return list(map(lambda x: x.get('Key'), self.get_objects().get('Contents')))
        except:
            return None

    def create_object(self, object_path, data):
        # If data is df write to CSV buffer
        if type(data) == DataFrame:
            csv_buffer = io.StringIO()
            data.to_csv(
                csv_buffer,
                encoding="latin-1",
                index=False
            )
            data = csv_buffer.getvalue()
        self.conn.put_object(
            Bucket=self.bucket_name,
            Key=object_path,
            Body=data
        )

    def delete_object(self, object_path):
        self.conn.delete_object(
            Bucket=self.bucket_name,
            Key=object_path
        )

    def delete_objects(self, object_path):
        obj_content = self.get_objects(
            object_path,
            delimiter=""
        )["Content"]
        paths = [obj["Key"] for obj in obj_content]
        for path in paths:
            self.delete_object(path)

    def generate_presigned_url(self, object_path, expiration=30):
        return self.conn.generate_presigned_url(
            ClientMethod="get_object",
            ExpiresIn=expiration,
            Params={"Bucket": self.bucket_name, "Key": object_path}
        )

    def check_existence(self, object_path):
        try:
            self.conn.head_object(
                Bucket=self.bucket_name,
                Key=object_path
            )
            return True
        except ClientError:
            return False

    def download_file(self, object_path, download_path):
        self.conn.download_file(
            Filename=download_path,
            Bucket=self.bucket_name,
            Key=object_path
        )

    def upload_file(self, object_path, file_path, remove=False):
        self.conn.upload_file(
            Filename=file_path,
            Bucket=self.bucket_name,
            Key=object_path
        )
        if remove:
            os.remove(file_path)

    def copy_object(self, src_path, dest_path, source_bucket=None):
        bucket = self.bucket_name if not source_bucket else source_bucket
        copy_source = {"Bucket": bucket, "Key": src_path}
        self.conn.copy_object(
            Bucket=self.bucket_name,
            CopySource=copy_source,
            Key=dest_path
        )

def main():
    df_test = pd.read_csv('~/Downloads/Account_test_s3.csv')
    our_ss = SimpleStorage('rt-data-warehouse')
    #our_ss.create_object('our_s3_test.csv',df_test)
    print('chill')


if __name__ == '__main__':
    main()