"""
Takes zipped NED data and converts the IMG files to geoTIFF files.

args: None.
results: ZIP and TIF files.
	
"""

#Import gdal
from osgeo import gdal
from osgeo import osr
import sys
import subprocess
import re
import os
import glob

def raster2tiff(src_filename,dst_filename):
	""" Takes source raster and converts to TIFF """
	#Open existing dataset
	# src_filename = r"C:\ned_data\imgn34w119_1.img"
	src_ds = gdal.Open( src_filename )

	#Open output format driver, see gdal_translate --formats for list
	format = "GTiff"
	# format = "XYZ" # For output to ascii text
	driver = gdal.GetDriverByName( format )

	# Output to new format
	# dst_filename = r"C:\ned_data\test_34w119n.tif"
	# dst_filename = r"C:\ned_data\test_34w119n.txt"
	dst_ds = driver.CreateCopy( dst_filename, src_ds, 0)

	# Fix metadata (doesn't get copied right from IMG file)
	srs = osr.SpatialReference()
	srs.SetWellKnownGeogCS( 'NAD83' )
	dst_ds.SetProjection( srs.ExportToWkt() )

	#Properly close the datasets to flush to disk
	dst_ds = None
	src_ds = None
	
#########################


# Change current working directory 
path = os.path.abspath(sys.argv[1])
print "\nChange directory to %s\n" % path
os.chdir(path)

# Get list of zip files
zip_list = glob.glob('*.zip')

for zip_filename in zip_list:
	
	# UNZIP
	subprocess.call("7za e " + zip_filename + " -y -r *.img")

	# Get IMG and TIF filenames
	img_filename = glob.glob('*.img')[0]
	tif_filename = re.sub(r'.img',r'.tif',img_filename)
	
	# Convert to TIF
	raster2tiff(img_filename,tif_filename)
	print "\nConverted %s to %s!" % (img_filename, tif_filename)
	
	# Delete IMG
	os.remove(img_filename)

print "\nAll files converted!"


