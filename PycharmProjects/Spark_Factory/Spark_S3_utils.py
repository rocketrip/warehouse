"""S3 Spark utility functions"""


def read_csv(spark,bucket ,csv):
    df = spark. \
        read. \
        option("delimiter", ","). \
        option("header", "true"). \
        csv(("s3a://{bucket}/{csv}").format(
        bucket= bucket,
        csv=csv)
    )
    return df


def upload_csv():
    pass


def delete_csv():
    pass