from minio import Minio
from minio.error import S3Error

client = Minio(
    "minio:9000",  
    access_key="AiGOMblU9UuA26XbFpQB",
    secret_key="e8s6iUWq6cZNAi7CL5FQ4DUKhuCVrt4wphSOn6eG", 
    secure=False
)

bucket_name = "bucket-21012"

if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created successfully.")
else:
    print(f"Bucket '{bucket_name}' already exists.")


file_path = "/work/input.txt"
object_name = "input.txt"

client.fput_object(bucket_name, object_name, file_path)
print(f"'{object_name}' uploaded to bucket '{bucket_name}'.")



objects = client.list_objects(bucket_name)
for obj in objects:
    print(f"Object: {obj.object_name}, Size: {obj.size} bytes")