import pandas as pd
import math
import boto3
import io
import gzip

# Define the S3 bucket name and telemetry files prefix
# bucket_name = 'grid-dev-lidarscans'
# pivox_name = 'Engel-Pivox/Pivox-1/'

def rotation_matrix(bucket_name, pivox_name):
    # Initialize S3 resource
    s3 = boto3.resource('s3')

    # Initialize the S3 client
    s3_client = boto3.client('s3')
    prefix_tele = pivox_name + 'telemetry/'
    t_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_tele)

    # Create a list of all CSV files in the folder, skip the first since the station wasn't in Boise
    csv_files = [obj['Key'] for obj in t_files['Contents'] if obj['Key'].endswith('.gz')][1:]
    columns_to_keep = [1, 4, 9, 12]
    dataframes = []
    for file_key in csv_files:
        obj = s3.Object(bucket_name, file_key)
        try:
            with obj.get()["Body"] as s3_body:
                with gzip.GzipFile(fileobj=io.BytesIO(s3_body.read())) as gzip_file:
                    # Try different encodings or handle errors
                    try:
                        df = pd.read_csv(gzip_file, encoding='utf-8', header= None)
                        columns_to_keep = [1, 4, 9, 12]
                        filtered_df = df.iloc[:, columns_to_keep]
                        dataframes.append(filtered_df)
                        # print(filtered_df)
                    except UnicodeDecodeError:
                        # If UTF-8 fails, try another encoding like ISO-8859-1
                        gzip_file.seek(0)  # Reset file pointer to the beginning
                        df = pd.read_csv(gzip_file, encoding='ISO-8859-1', header= None, on_bad_lines='skip')
                        filtered_df = df.iloc[:, columns_to_keep]
                        dataframes.append(filtered_df)
                        # print(filtered_df)

        except Exception as e:
            print(f"Error processing file {file_key}: {e}")

    new_column_names = ['date', 'time', 'X', 'Y']
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.columns = new_column_names[:combined_df.shape[1]]
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    filtered_df = combined_df[combined_df['date'] >= '2024-01-25'] # Date of Boise site install

    # Remove the outliers using the full dataset 
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

    # Determine the mean rotation angles in the X and Y 
    X_mean = df_clean['X'].mean()
    Y_mean = df_clean['Y'].mean()

    # Set the angle in degrees from the TELEMETRY.CSV file - using the mean from the full season
    Xtheta_deg = X_mean
    Ytheta_deg = Y_mean

    # Convert the angle in degrees to radians for both the X and Y
    Xtheta_rad = math.radians(Xtheta_deg)
    Ytheta_rad = math.radians(Ytheta_deg)

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