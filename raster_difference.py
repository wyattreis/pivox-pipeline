import rioxarray
import numpy as np

dir = "C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Scripts/Pivox Pipeline"

dem1 = rioxarray.open_rasterio("C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Scripts/Pivox Pipeline/output_subset_20240702_surface_mean.tif")
dem2 = rioxarray.open_rasterio("C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Scripts/Pivox Pipeline/output_subset_20240501_surface_mean.tif")

# Ensure DEMs have the same CRS (Coordinate Reference System)
if dem1.rio.crs != dem2.rio.crs:
    dem2 = dem2.rio.reproject(dem1.rio.crs)

# Reproject or resample dem2 to match the shape, resolution, and extent of dem1
# Using dem1 as the reference
dem2_resampled = dem2.rio.reproject_match(dem1)

dem1 = dem1.where(dem1 != -9999, np.nan)
dem1.rio.write_nodata(np.nan, inplace=True)
dem2_resampled = dem2_resampled.where(dem2_resampled != -9999, np.nan)
dem2_resampled.rio.write_nodata(np.nan, inplace=True)

# Subtract the two DEMs
dem_difference = dem2_resampled - dem1

SD_mean = dem_difference.mean(skipna=True).values.item()
SD_std = dem_difference.std(skipna=True).values.item()

print(SD_mean)
print(SD_std)
#dem_difference.rio.to_raster("C:/Users/RDCRLWKR/Documents/FileCloud/My Files/Active Projects/Snow Working Group/Pivox/Technical/Scripts/Pivox Pipeline/dem_difference_full.tif")