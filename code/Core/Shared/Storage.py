import boto3
from botocore.exceptions import ClientError
import uuid
from dotenv import load_dotenv
load_dotenv()

class Storage:
    """
    The Storage class provides methods to interact with S3-compatible storage,
    including storing and deleting files. It includes methods to upload a file to S3 and retrieve its public URL,
    as well as to delete a file from S3.
    The class handles potential exceptions related to S3 operations.
    """

    ENDPOINT_URL =  os.getenv("STORAGE_BUCKET")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    BUCKET_NAME = os.getenv("BUCKET_NAME")


    def __init__(self, endpoint_url = ENDPOINT_URL, aws_access_key_id = AWS_ACCESS_KEY, aws_secret_access_key = AWS_SECRET_KEY, bucket_name=BUCKET_NAME):
        """
        Initializes the Storage class with S3 configuration.

        Args:
            endpoint_url (str): The endpoint URL of the S3-compatible storage.
            aws_access_key_id (str): The access key ID for authentication.
            aws_secret_access_key (str): The secret access key for authentication.
            bucket_name (str): The name of the S3 bucket to use.
        """
        self.s3 = boto3.client('s3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self.bucket_name = bucket_name

    def store(self, file, filename):
        """
        Stores a file in S3 storage and returns its public URL.

        Args:
            file: The file to be stored.
            filename (str): The name to be used for the stored file.

        Returns:
            str: The public URL of the stored file.

        Raises:
            ClientError: If there is an error uploading the file.
        """
        try:
            # Generate a unique object name
            object_name = f"{uuid.uuid4()}-{filename}"

            # Upload the file
            self.s3.upload_fileobj(file, self.bucket_name, object_name)

            # Generate the public URL
            url = f"{self.s3.meta.endpoint_url}/{self.bucket_name}/{object_name}"

            return url
        except ClientError as e:
            print(f"An error occurred: {e}")
            raise

    def delete(self, filename):
        """
        Deletes a file from S3 storage.

        Args:
            filename (str): The name of the file to be deleted.

        Returns:
            bool: True if the file was successfully deleted, False otherwise.

        Raises:
            ClientError: If there is an error deleting the file.
        """
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=filename)
            return True
        except ClientError as e:
            print(f"An error occurred: {e}")
            return False