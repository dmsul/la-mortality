#! /usr/bin/bash

python get_zip_utm.py
python clean_blockgroup_data.py
python zip4_to_block2000.py
stata xwalk_zip_zip4_tmp.do

python zips_aermod.py
python sample_dummies.py

stata map_aermod_coverage.do
stata reg_main.do
