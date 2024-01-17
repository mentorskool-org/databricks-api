import boto3
from constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def get_s3_data(bucket_name, key):
    # Create the s3 obj
    s3 = boto3.client(
        service_name="s3",
        # region_name=region_name,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    ## Now read the file using get_object
    file = s3.get_object(Bucket=bucket_name, Key=key)

    ## Now fetch the content from the body
    return file["Body"].read()
