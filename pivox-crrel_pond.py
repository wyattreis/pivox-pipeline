import boto3
from datetime import datetime
import numpy as np
from pipeline_functions import  *

# Apply a filter to select files within a specific date range
start_date = datetime(2023, 11, 29)
end_date = datetime(2023, 12, 1)

# Set the S3 base bucket and the pivox folder
bucket_name = 'grid-dev-lidarscans'
pivox_name = 'Engel-Pivox/Pivox-2/'

# Initialize the S3 client
s3_client = boto3.client('s3')

#--------------------------------------------------------------------------------------------
# Determine the mean rotation angles in the X and Y during the POI
Y_mean = 6.8299
X_mean = 1.729
print(f'Mean Pivox rotation angles: X={X_mean}; Y={Y_mean}')

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

#--------------------------------------------------------------------------------------------

las_filter_pipeline_dwnld(bucket_name, pivox_name, s3_client, start_date, end_date, Xmatrix, Ymatrix)