import boto3
from datetime import datetime

from pipeline_functions import  las_filter_pipeline_leveled, snowdepth_timeseries

ground_tif = "C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Data/analysis pyvox/20240702-2000-46_bareGround.tif"

# Apply a filter to select files within a specific date range
start_date = datetime(2024, 1, 25) # installed in Boise on 1/25/2024
end_date = datetime(2024, 7, 2)

# Set the S3 base bucket and the pivox folder
bucket_name = 'grid-dev-lidarscans'
pivox_name = 'Engel-Pivox/Pivox-1/'

# Initialize the S3 client
s3_client = boto3.client('s3')

# Procees the raw LAZ files from the S3 Bucket to ground points (.tif and .laz outputs)
las_filter_pipeline_leveled(bucket_name, pivox_name, s3_client, start_date, end_date)
 
# # # Create a dataframe of the elevations from the processed .tif and .laz files
scan_elev_df = snowdepth_timeseries(bucket_name, pivox_name, s3_client, ground_tif)
scan_elev_df.to_csv('scan_snowdepth_df.csv', index=False)
