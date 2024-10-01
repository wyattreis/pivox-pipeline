import boto3
import json
import pdal
import tiledb
import os

os.environ["PDAL_DRIVER_PATH"] = r'C:\Users\RDCRLWKR\.conda\envs\pdalplayground\Library\bin'

bucket_name = 'grid-dev-lidarscans'
pivox_name = 'Engel-Pivox/Pivox-1/scans'
scan = '20240602-2000-46.PIVOX1.laz'

in_laz = f's3://{bucket_name}/{pivox_name}/{scan}'
print(in_laz)

tiledb.default_ctx({
    "vfs.s3.region": "us-west-2" # replace with bucket region
})

tiledb.open(in_laz)

# PIPELINE = [
#     {
#         "type": "readers.tiledb",
#         "filename": in_laz,
#         "override_srs": "EPSG:32611"
#     }
# ]

# pipeline = pdal.Pipeline(json.dumps(PIPELINE))

# # # set pdal log level
# # pipeline.loglevel = 8

# # execute the pipeline
# count = pipeline.execute()
# arrays = pipeline.arrays
# metadata = pipeline.metadata
# # log = pipeline.log
# print("done")

# Set the S3 base bucket and the pivox folder
bucket_name = 'grid-dev-lidarscans'
pivox_name = 'Engel-Pivox/Pivox-1/'

# Initialize the S3 client
s3_client = boto3.client('s3')

# Set up sub directories within S3 bucket
prefix_scans = f'{pivox_name}scans/'
prefix_processed = f'{pivox_name}processed/'

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')
if 'CommonPrefixes' in response:
    print(f"Folder '{prefix_processed}' already exists in bucket '{bucket_name}'")
else:
    s3_client.put_object(Bucket=bucket_name, Key=prefix_processed)
    print(f"Folder '{prefix_processed}' created in bucket '{bucket_name}'")