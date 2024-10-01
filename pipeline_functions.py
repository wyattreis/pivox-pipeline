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

# Function to extract date from the file path and convert it to a datetime object
def extract_date(file_path):
    date_str = file_path.split('/')[3].split('-')[0]
    return datetime.strptime(date_str, '%Y%m%d')

# Function to rotate the pivox point cloud based on the telemetry file
def rotation_matrix(s3_client,bucket_name, pivox_name, start_date, end_date):
    # Specify file paths
    prefix_telemetry =f'{pivox_name}telemetry/'
    t_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_telemetry)
    
    # Specify the X and Y rotation columns in the CSVs
    columns_to_keep = [1, 4, 9, 12]

    # Create a list of all CSV files in the folder, skip the first since the station wasn't in Boise
    csv_files = [obj['Key'] for obj in t_files['Contents'] if obj['Key'].endswith('.gz')][1:]
    columns_to_keep = [1, 4, 9, 12]
    dataframes = []
    for csv in csv_files:
        obj = s3_client.get_object(Bucket=bucket_name, Key=csv)
        try:
            gzip_file_content = obj['Body'].read()
            with gzip.GzipFile(fileobj=BytesIO(gzip_file_content)) as gzipfile:
                try:
                    df = pd.read_csv(gzipfile, encoding='utf-8', header= None)
                    filtered_df = df.iloc[:, columns_to_keep]
                    dataframes.append(filtered_df)
                    # print(f"Processed {csv} with utf-8 encoding")
                except UnicodeDecodeError:
                    # If UTF-8 fails, try another encoding like ISO-8859-1
                    gzipfile.seek(0)  # Reset file pointer to the beginning
                    df = pd.read_csv(gzipfile, encoding='ISO-8859-1', header= None, on_bad_lines='skip')
                    filtered_df = df.iloc[:, columns_to_keep]
                    dataframes.append(filtered_df)
                    # print(f"Processed {csv} with ISO-8859-1 encoding")
        except Exception as e:
            print(f"Error processing file {csv}: {e}")

    new_column_names = ['date', 'time', 'X', 'Y']
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.columns = new_column_names[:combined_df.shape[1]]
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    filtered_df = combined_df[(combined_df['date'] >= start_date) & (combined_df['date'] <= end_date)] # Date of Boise site install

    # Remove the outliers using the full period of interest
    def remove_outliers(df, columns):
        for column in columns:
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
        return df

    clean_cols = ['X', 'Y']
    df_clean = remove_outliers(filtered_df, clean_cols)
    df_clean = df_clean.dropna()

    # Determine the mean rotation angles in the X and Y during the POI
    X_mean = df_clean['X'].mean()
    Y_mean = df_clean['Y'].mean()
    print(f'Mean PiVox rotation angles: X={X_mean}; Y={Y_mean}')

    # Convert the mean angles in degrees to radians for both the X and Y
    Xtheta_rad = math.radians(X_mean)
    Ytheta_rad = math.radians(Y_mean)

    # Calculate the cosine and sine of the angle and create the PDAL rotation Matrix for X and Y
    X_COStheta = math.cos(Xtheta_rad)
    X_SINtheta = math.sin(Xtheta_rad)
    Y_COStheta = math.cos(Ytheta_rad)
    Y_SINtheta = math.sin(Ytheta_rad)
    Xmatrix = f"1 0 0 0 0 {X_COStheta} {-X_SINtheta} 0 0 {X_SINtheta} {X_COStheta} 0 0 0 0 1"
    Ymatrix = f"{Y_COStheta} 0 {Y_SINtheta} 0 0 1 0 0 {-Y_SINtheta} 0 {Y_COStheta} 0 0 0 0 1"
    # print(f"X Matrix: {Xmatrix}")
    # print(f"Y Matrix: {Ymatrix}")
    return Xmatrix, Ymatrix

# Function to create and run a PDAL pipeline to extract the ground points
def las_filter_pipeline_dwnld(bucket_name, pivox_name, s3_client, start_date, end_date):

    # Set up sub directories within S3 bucket
    prefix_scans = f'{pivox_name}leveled-laz/'
    prefix_processed = f'{pivox_name}processed/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')
    if 'CommonPrefixes' in response:
        print(f"Folder '{prefix_processed}' already exists in bucket '{bucket_name}'")
    else:
        s3_client.put_object(Bucket=bucket_name, Key=prefix_processed)
        print(f"Folder '{prefix_processed}' created in bucket '{bucket_name}'")

    # Identify all of the scans in the S3 Bucket
    scans = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_scans)
    scan_paths = [obj['Key'] for obj in scans.get('Contents', [])]
    scans_filtered = [file for file in scan_paths if start_date <= extract_date(file) <= end_date]
    print(f'Total Number of Files to Process: {len(scans_filtered)}')

    # Local directories for raw and results
    raw_dir = '/tmp/raw'
    results_dir = '/tmp/processed'
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    Xmatrix, Ymatrix = rotation_matrix(s3_client,bucket_name, pivox_name, start_date, end_date)

    for scan in scans_filtered:
        filename = os.path.basename(scan)
        local_file_path = None
        out_las = None
        if filename.endswith(('.laz', '.las')):
            print(f'Processing file: {filename}')
            basename = os.path.basename(scan)[:-4] # remove the .las from file path name
            local_file_path = os.path.join(raw_dir, basename)
            out_las = os.path.join(results_dir, f'{basename}_processed.laz')
            # Download .laz and .las files on S3 to local file path
            s3_client.download_file(bucket_name, scan, local_file_path)
            # print(f'Downloaded: {local_file_path}')
            
        elif filename.endswith('.gz'):
            print(f'Processing file: {filename}')
            basename = os.path.basename(scan)[:-7] # remove the .las.gz from file path name
            local_file_path = os.path.join(raw_dir, basename)
            out_las = os.path.join(results_dir, f'{basename}_processed.laz')

            obj = s3_client.get_object(Bucket=bucket_name, Key=scan)
            with gzip.GzipFile(fileobj=obj['Body']) as gzipfile:
                with open(local_file_path, "wb") as f:
                    f.write(gzipfile.read())
            # print(f'Downloaded and unzipped: {local_file_path}')

        # PDAL Pipeline to process point clouds to just the "ground" returns
        pipeline = [
            {
                "type": "readers.las", 
                "filename": local_file_path,
                "override_srs": "EPSG:32611"
            },
            # {
            #     "type": "filters.ferry",
            #     "dimensions": "=>tempY"
            # },
            # {
            #     "type": "filters.assign",
            #     "value": [
            #         "tempY=Y",
            #         "Y=Z",
            #         "Z=tempY"
            #         ]
            # },
            # {
            #     "type": "filters.transformation",
            #     "matrix": Xmatrix
            # },
            # {
            #     "type": "filters.transformation",
            #     "matrix": Ymatrix
            # },
            # {
            #     "type": "filters.assign",
            #     "assignment":"Classification[:]=0",
            #     "value": [
            #     "ReturnNumber = 1 WHERE ReturnNumber < 1",
            #     "NumberOfReturns = 1 WHERE NumberOfReturns < 1"
            #     ]
            # },
            # {
            #     "type": "filters.smrf",
            #     "cell": 0.5,
            #     "slope": 0.3,
            #     "threshold": 0.05,
            #     "window": 4
            # },
            # { 
            #     "type":"filters.range",
            #     "limits":"Classification[2:2]"
            # },

            {
                "type": "writers.las",
                "filename": out_las
            }
        ]

        pipeline_str = json.dumps(pipeline)
        pipeline_pdal = pdal.Pipeline(pipeline_str)
        
        try:
            pipeline_pdal.execute()
            print(f"Processed file: {filename}")

            # Upload the results back to S3
            output_las_key = f'{prefix_processed}{os.path.basename(out_las)}'
            # output_tif_key = f'{prefix_processed}{os.path.basename(out_tif)}'
            
            s3_client.upload_file(out_las, bucket_name, output_las_key)
            # s3_client.upload_file(out_tif, bucket_name, output_tif_key)

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
        finally:
            Path(local_file_path).unlink()
            Path(out_las).unlink(missing_ok=True)

# Function to create and run a PDAL pipeline to extract the ground points from the leveled point clouds 
def las_filter_pipeline_leveled(bucket_name, pivox_name, s3_client, start_date, end_date):

    # Set up sub directories within S3 bucket
    prefix_scans = f'{pivox_name}leveled-laz/'
    prefix_processed = f'{pivox_name}leveled-processed/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')
    if 'CommonPrefixes' in response:
        print(f"Folder '{prefix_processed}' already exists in bucket '{bucket_name}'")
    else:
        s3_client.put_object(Bucket=bucket_name, Key=prefix_processed)
        print(f"Folder '{prefix_processed}' created in bucket '{bucket_name}'")

    # Identify all of the scans in the S3 Bucket
    scans = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_scans)
    scan_paths = [obj['Key'] for obj in scans.get('Contents', []) if obj['Key'].endswith('.laz')]
    scans_filtered = [file for file in scan_paths if start_date <= extract_date(file) <= end_date]
    print(f'Total Number of Files to Process: {len(scans_filtered)}')

    # Local directories for raw and results
    raw_dir = '/tmp/raw'
    results_dir = '/tmp/processed'
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    for scan in scans_filtered:
        filename = os.path.basename(scan)
        local_file_path = None
        out_las = None
        if filename.endswith(('.laz', '.las')):
            print(f'Processing file: {filename}')
            basename = os.path.basename(scan)[:-4] # remove the .las from file path name
            local_file_path = os.path.join(raw_dir, basename)
            out_las = os.path.join(results_dir, f'{basename}_processed.laz')
            out_tif = os.path.join(results_dir, f'{basename}_processed.tif')
            # Download .laz and .las files on S3 to local file path
            s3_client.download_file(bucket_name, scan, local_file_path)
            # print(f'Downloaded: {local_file_path}')
            
        elif filename.endswith('.gz'):
            # print(f'Processing file: {filename}')
            basename = os.path.basename(scan)[:-7] # remove the .las.gz from file path name
            local_file_path = os.path.join(raw_dir, basename)
            out_las = os.path.join(results_dir, f'{basename}_processed.laz')
            out_tif = os.path.join(results_dir, f'{basename}_processed.tif')

            obj = s3_client.get_object(Bucket=bucket_name, Key=scan)
            with gzip.GzipFile(fileobj=obj['Body']) as gzipfile:
                with open(local_file_path, "wb") as f:
                    f.write(gzipfile.read())
            # print(f'Downloaded and unzipped: {local_file_path}')

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
            {
                "type": "writers.gdal",
                "resolution": 0.01,
                "output_type": "mean",
                "filename": out_tif
            }
        ]

        pipeline_str = json.dumps(pipeline)
        pipeline_pdal = pdal.Pipeline(pipeline_str)
        
        try:
            pipeline_pdal.execute()
            print(f"Processed file: {filename}")

            # Upload the results back to S3
            output_las_key = f'{prefix_processed}{os.path.basename(out_las)}'
            output_tif_key = f'{prefix_processed}{os.path.basename(out_tif)}'
            
            s3_client.upload_file(out_las, bucket_name, output_las_key)
            s3_client.upload_file(out_tif, bucket_name, output_tif_key)

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
        finally:
            Path(local_file_path).unlink()
            Path(out_las).unlink(missing_ok=True)

# Function to create a timeseries of gground heights from the sensor and standard deviation
def snowdepth_timeseries(bucket_name, pivox_name, s3_client, ground_tif):
    prefix_processed = f'{pivox_name}leveled-processed/'
    snowdepth_tif = f'{pivox_name}snowdepth-tif/'

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=pivox_name, Delimiter='/')

    # Check if the snowdepth_tif folder exists
    existing_folders = [common_prefix['Prefix'] for common_prefix in response.get('CommonPrefixes', [])]
    if snowdepth_tif in existing_folders:
        print(f"Folder '{snowdepth_tif}' already exists in bucket '{bucket_name}'")
    else:
        s3_client.put_object(Bucket=bucket_name, Key=snowdepth_tif)
        print(f"Folder '{snowdepth_tif}' created in bucket '{bucket_name}'")


    # Identify all of the scans in the S3 Bucket
    processed = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_processed)
    processed_paths = [obj['Key'] for obj in processed.get('Contents', []) if obj['Key'].endswith('.tif')]

    # Local directories for raw and results
    download_dir = '/tmp/processed'
    os.makedirs(download_dir, exist_ok=True)

    scan_elev = {}
    for scan in processed_paths:
        filename = os.path.basename(scan)
        basename, ext = os.path.splitext(filename)
        local_file_path = os.path.join(download_dir, filename)

        s3_client.download_file(bucket_name, scan, local_file_path)

        date = basename[:8]
        time = basename[9:13]

        if (date, time) not in scan_elev:
            scan_elev[(date, time)] = {
                'date': date,
                'time': time,
                'SD_mean': np.nan,
                'SD_std': np.nan
            }
        
        # Difference TIFF files and extract snow depth
        if filename.endswith('.tif'):
            tif_fp = os.path.join(download_dir, filename)

            with rio.open_rasterio(ground_tif) as dem_ground, rio.open_rasterio(tif_fp) as dem_surface:
            
                # Make sure the two DEMs are aligned
                if dem_ground.rio.crs != dem_surface.rio.crs:
                    dem_surface = dem_surface.rio.reproject(dem_ground.rio.crs)
                
                dem_surface_resampled = dem_surface.rio.reproject_match(dem_ground)

                # Replace -9999 with NAN in both DEMs
                dem_ground = dem_ground.where(dem_ground != -9999, np.nan)
                dem_ground.rio.write_nodata(np.nan, inplace=True)
                dem_surface_resampled = dem_surface_resampled.where(dem_surface_resampled != -9999, np.nan)
                dem_surface_resampled.rio.write_nodata(np.nan, inplace=True)

                # Calculate Snow Depth
                dem_snowDepth = dem_surface_resampled - dem_ground
                scan_elev[(date, time)]['laz_elev_mean'] = dem_snowDepth.mean(skipna=True).values.item()
                scan_elev[(date, time)]['laz_elev_std'] = dem_snowDepth.std(skipna=True).values.item()

                # Save snow depth to a temporary TIF file for uploading
                snow_depth_tif = os.path.join(download_dir, f'{basename}_snow_depth.tif')
                dem_snowDepth.rio.to_raster(snow_depth_tif)
                
            # Upload the snow depth TIFs back to S3
            output_tif_key = f'{snowdepth_tif}{os.path.basename(snow_depth_tif)}'
            s3_client.upload_file(snow_depth_tif, bucket_name, output_tif_key)

        print(f"Snow Depth added from file: {os.path.basename(snow_depth_tif)}")

        Path(local_file_path).unlink()
        if filename.endswith('.tif'):
            Path(snow_depth_tif).unlink()

    # Create a DataFrame with the data
    scan_elev_df = pd.DataFrame(scan_elev.values())
    scan_elev_df['datetime'] = pd.to_datetime(scan_elev_df['date'] + scan_elev_df['time'], format='%Y%m%d%H%M')
    scan_elev_df.drop(columns=['date', 'time'], inplace=True)
    columns = ['datetime'] + [col for col in scan_elev_df.columns if col != 'datetime']
    scan_elev_df = scan_elev_df[columns]

    # # Add the elevation difference between each scan
    # for col in ['laz_elev_mean']:  #'tif_elev_mean',
    #     scan_elev_df[f'{col[:8]}_diff'] = scan_elev_df[col].diff()

    # try:
    #     shutil.rmtree('/tmp/')
    #     print(f"Deleted download directory: {'/tmp/'}")
    # except Exception as e:
    #     print(f"Error deleting download directory: {e}")

    return scan_elev_df