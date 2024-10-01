import boto3
import pandas as pd
import numpy as np
import math
import gzip
import pdal
import json
import os
import laspy
import shutil
from os.path import join
from datetime import datetime
from pathlib import Path
from io import BytesIO
import rioxarray as rio

# Apply a filter to select files within a specific date range
start_date = datetime(2024, 2, 2)
end_date = datetime(2024, 2, 5)

# Set the S3 base bucket and the pivox folder
bucket_name = 'grid-dev-lidarscans'
pivox_name = 'Engel-Pivox/Pivox-1/'

# Initialize the S3 client
s3_client = boto3.client('s3')

ground_tif = "C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Scripts/Pivox Pipeline/output_subset_20240702_surface_crop_mean.tif"

# s3_client.download_file(bucket_name, 'Engel-Pivox/Pivox-1/leveled-laz/20240202-1301-00.PIVOX1.ROT.laz', 'C:/tmp/leveled/20240202-1301-00.PIVOX1.ROT.laz')
############################################################
def extract_date(file_path):
    date_str = file_path.split('/')[3].split('-')[0]
    return datetime.strptime(date_str, '%Y%m%d')

# Function to create and run a PDAL pipeline to extract the ground points from the leveled point clouds 
def las_filter_pipeline_leveled(bucket_name, pivox_name, s3_client, start_date, end_date):

    # Set up sub directories within S3 bucket
    prefix_leveled = f'{pivox_name}leveled-laz/'
    prefix_processed = f'{pivox_name}leveled-processed/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')
    if 'CommonPrefixes' in response:
        print(f"Folder '{prefix_processed}' already exists in bucket '{bucket_name}'")
    else:
        s3_client.put_object(Bucket=bucket_name, Key=prefix_processed)
        print(f"Folder '{prefix_processed}' created in bucket '{bucket_name}'")

    # Identify all of the scans in the S3 Bucket
    scans = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_leveled)
    scan_paths = [obj['Key'] for obj in scans.get('Contents', []) if obj['Key'].endswith('.laz')]
    scans_filtered = [file for file in scan_paths if start_date <= extract_date(file) <= end_date]
    print(f'Total Number of Files to Process: {len(scans_filtered)}')

    # Local directories for raw and results
    level_dir = os.path.join('C:/tmp', 'leveled')
    results_dir = os.path.join('C:/tmp', 'processed')
    os.makedirs(level_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    for scan in scans_filtered:
        filename = os.path.basename(scan)
        basename = os.path.basename(scan)[:-4] # remove the .las from file path name
        local_file_path = os.path.join(level_dir, filename)
        
        # Download .laz files on S3 to local file path
        s3_client.download_file(bucket_name, scan, local_file_path)
        # print(f'Downloaded: {local_file_path}')
        
        # Create output paths
        out_las = os.path.join(results_dir, f'{basename}_processed.laz')
        out_tif = os.path.join(results_dir, f'{basename}_processed.tif')

        # PDAL Pipeline to process point clouds to just the "ground" returns
        pipeline = [
            {
                "type": "readers.las", 
                "filename": local_file_path,
                "override_srs": "EPSG:32611"
            },
            {
                "type": "filters.crop",
                "bounds":"([-2.75,0],[2,5],[-7,3])" 
            },
            {
                "type": "filters.assign",
                "assignment":"Classification[:]=0",
                "value": [
                "ReturnNumber = 1 WHERE ReturnNumber < 1",
                "NumberOfReturns = 1 WHERE NumberOfReturns < 1"
                ]
            },
            {
                "type": "filters.smrf",
                "cell": 0.5,
                "slope": 0.3,
                "threshold": 0.05,
                "window": 4
            },
            { 
                "type":"filters.range",
                "limits":"Classification[2:2]"
            },
            {
                "type": "writers.las",
                "filename": out_las
            },
            # {
            #     "type": "writers.gdal",
            #     "resolution": 0.01,
            #     "output_type": "mean",
            #     "filename": out_tif
            # }
        ]

        pipeline_str = json.dumps(pipeline)
        pipeline_pdal = pdal.Pipeline(pipeline_str)
        
        try:
            pipeline_pdal.execute()
            print(f"Processed file: {filename}")

            # # Upload the results back to S3
            # output_las_key = f'{prefix_processed}{os.path.basename(out_las)}'
            # output_tif_key = f'{prefix_processed}{os.path.basename(out_tif)}'
            
            # s3_client.upload_file(out_las, bucket_name, output_las_key)
            # s3_client.upload_file(out_tif, bucket_name, output_tif_key)

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
        # finally:
        #     Path(local_file_path).unlink()
        #     Path(out_las).unlink(missing_ok=True)

############################################################
las_filter_pipeline_leveled(bucket_name, pivox_name, s3_client, start_date, end_date)

############################################################
# prefix_processed = f'{pivox_name}leveled-processed/'
# snowdepth_tif = f'{pivox_name}snowdepth-tif/'

# response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')

# # Check if the snowdepth_tif folder exists
# existing_folders = [common_prefix['Prefix'] for common_prefix in response.get('CommonPrefixes', [])]
# if snowdepth_tif in existing_folders:
#     print(f"Folder '{snowdepth_tif}' already exists in bucket '{bucket_name}'")
# else:
#     s3_client.put_object(Bucket=bucket_name, Key=snowdepth_tif)
#     print(f"Folder '{snowdepth_tif}' created in bucket '{bucket_name}'")

# # Local directories for processed tifs and snowdepth tifs
# download_dir = '/tmp/processed'
# snowdepth_dir = '/tmp/snowdepth'
# os.makedirs(download_dir, exist_ok=True)
# os.makedirs(snowdepth_dir, exist_ok=True)

# #############################################################
# # Identify all of the scans in the leveled and processed S3 Bucket
# processed = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_processed)
# leveled_processed = [obj['Key'] for obj in processed.get('Contents', []) if obj['Key'].endswith('.tif')]

# scan_elev = {}
# for scan in leveled_processed[0:2]:
#     filename = os.path.basename(scan)
#     print(filename)
#     basename, ext = os.path.splitext(filename)
#     local_file_path = os.path.join(download_dir, filename)

#     s3_client.download_file(bucket_name, scan, local_file_path)

#     date = basename[:8]
#     time = basename[9:13]

#     if (date, time) not in scan_elev:
#         scan_elev[(date, time)] = {
#             'date': date,
#             'time': time,
#             'SD_mean': np.nan,
#             'SD_std': np.nan
#         }
    
#     # Difference TIFF files and extract snow depth
#     with rio.open_rasterio(ground_tif) as dem_ground, rio.open_rasterio(local_file_path) as dem_surface:     
#         # Make sure the two DEMs are aligned
#         if dem_ground.rio.crs != dem_surface.rio.crs:
#             dem_surface = dem_surface.rio.reproject(dem_ground.rio.crs)
        
#         dem_surface_resampled = dem_surface.rio.reproject_match(dem_ground)

#         print(dem_ground)
#         print(dem_surface)

        
        
#             # Replace -9999 with NAN in both DEMs
#             dem_ground = dem_ground.where(dem_ground != -9999, np.nan)
#             dem_ground.rio.write_nodata(np.nan, inplace=True)
#             dem_surface_resampled = dem_surface_resampled.where(dem_surface_resampled != -9999, np.nan)
#             dem_surface_resampled.rio.write_nodata(np.nan, inplace=True)

#             # Calculate Snow Depth
#             dem_snowDepth = dem_surface_resampled - dem_ground
#             scan_elev[(date, time)]['laz_elev_mean'] = dem_snowDepth.mean(skipna=True).values.item()
#             scan_elev[(date, time)]['laz_elev_std'] = dem_snowDepth.std(skipna=True).values.item()

#             # Save snow depth to a temporary TIF file for uploading
#             snow_depth_tif = os.path.join(download_dir, f'{basename}_snow_depth.tif')
#             dem_snowDepth.rio.to_raster(snow_depth_tif)
            
#         # Upload the snow depth TIFs back to S3
#         output_tif_key = f'{snowdepth_tif}{os.path.basename(snow_depth_tif)}'
#         s3_client.upload_file(snow_depth_tif, bucket_name, output_tif_key)

#     print(f"Snow Depth added from file: {os.path.basename(snow_depth_tif)}")

#     Path(local_file_path).unlink()
#     if filename.endswith('.tif'):
#         Path(snow_depth_tif).unlink()

# # Create a DataFrame with the data
# scan_elev_df = pd.DataFrame(scan_elev.values())
# scan_elev_df['datetime'] = pd.to_datetime(scan_elev_df['date'] + scan_elev_df['time'], format='%Y%m%d%H%M')
# scan_elev_df.drop(columns=['date', 'time'], inplace=True)
# columns = ['datetime'] + [col for col in scan_elev_df.columns if col != 'datetime']
# scan_elev_df = scan_elev_df[columns]