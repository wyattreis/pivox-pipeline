# Reads in las/laz file and assigns srs
reader = {
    "type": "readers.las", 
    "filename": file_path,
    "override_srs": "EPSG:32611"
}
# Create an empty temporary Y classification
ferry = {
    "type": "filters.ferry",
    "dimensions": "=>tempY"
}
# Swap the Y and Z so Z is in the vertical direction
assign_axis = {
    "type": "filters.assign",
    "value": [
        "tempY=Y",
        "Y=Z",
        "Z=tempY"
        ]
}
# Rotate about the x-axis
rotateX = {
    "type": "filters.transformation",
    "matrix": Xmatrix
}
# Rotate about the y-axis
rotateY = {
    "type": "filters.transformation",
    "matrix": Ymatrix
}
# Assign classification, return number, and number of returns
assign_class ={
    "type": "filters.assign",
    "assignment":"Classification[:]=0",
    "value": [
        "ReturnNumber = 1 WHERE ReturnNumber < 1",
        "NumberOfReturns = 1 WHERE NumberOfReturns < 1"
    ]
},
# SMRF classifier for ground
smrf_classifier ={
    "type": "filters.smrf",
    "cell": 0.5,
    "slope": 0.3,
    "threshold": 0.05,
    "window": 4
},
# Select ground points only
smrf_selecter = { 
    "type":"filters.range",
    "limits":"Classification[2:2]"
},
# Write las file
las_writer = {
    "type": "writers.las",
    "filename": "output_YZswapped_rotated.laz"
}