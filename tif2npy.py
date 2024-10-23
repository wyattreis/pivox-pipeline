import rasterio
import numpy as np
import os

# Directory and raster files
raster_dir = r'C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Data/raw pyvox/'
raster_files = ["20240128-1200-46.PIVOX1.ROT_processed_sd.tif", 
                "20240406-2000-46.PIVOX1.ROT_processed_sd.tif", 
                "20240702-2000-46.PIVOX1.ROT_processed_sd.tif"]

# Dictionary to hold data from all rasters
data_dict = {}

# Collect flattened elevation data for each raster
for i, raster_file in enumerate(raster_files):
    # Extract the date part of the filename (the first 13 characters)
    date_part = os.path.basename(raster_file)[:13]
    
    # Open the raster file and read data
    with rasterio.open(f'{raster_dir}{raster_file}') as dataset:
        elevation_data = dataset.read(1)
        elevation_data = np.ma.masked_equal(elevation_data, dataset.nodata)
    
    # Flatten the array
    elevation_data_flat = elevation_data.compressed()
    
    # Add the flattened data to the dictionary
    data_dict[date_part] = elevation_data_flat

# Save the dictionary as a .npy file
np.save(r'C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Data/elevation_data_all.npy', data_dict)