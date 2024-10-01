import os
import boto3
import json
import subprocess


pipeline_file = os.path.join(os.getcwd(), 'pipeline.json')

s3 = boto3.client('s3')
# Attempt to list buckets to verify credentials
# Define the S3 bucket name and prefix
bucket_name = 'grid-dev-lidarscans'
prefix = 'Engel-Pivox/Pivox-1/scans/'

# List all objects in the S3 bucket with the specified prefix
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

for obj in response['Contents'][600:605]:
    input_file = f"s3://{bucket_name}/{obj['Key']}"
    output_file = f"E://Active Projects - HD/PiVox/Boise/output/rotated_{obj['Key']}"

    pipeline = {
        "pipeline":[
            {
                "filename": input_file,
                "type":"readers.laz"
            },
            {
                "type":"filters.transformation",
                "matrix":"0.665 0.747 0 0 -0.747 0.665 0 0 0 0 1 0 0 0 0 1"
            },
            {
                "type":"filters.transformation",
                "matrix":"1  0  0  0  0  0.998  -0.066  0  0  0.066  0.998  0  0  0  0  1"
            },
            {
                "type": "writers.las",
                "filename": output_file
            }
        ]   
    }

    # Write the pipeline configuration to a temporary file
    # pipeline_file = 'pipeline.json'
    with open(pipeline_file, 'w') as f:
        json.dump(pipeline, f)

    # Run the PDAL pipeline
        try:
            subprocess.run(['pdal', 'pipeline', pipeline_file], check=True)
        except FileNotFoundError as e:
            print(f"Error running PDAL: {e}")
        except subprocess.CalledProcessError as e:
            print(f"Error running PDAL pipeline: {e}")

        # Remove the temporary pipeline file
        os.remove(pipeline_file)