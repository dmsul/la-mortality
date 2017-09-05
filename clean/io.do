global home: env USERPROFILE    // For windows
if "$home" == "" global home ~  // For not windows

global project_root_from_home "research/poll-house"
global root_dir = "$home" + "/" + "$project_root_from_home"

global DATA_PATH = "$root_dir" + "/" + "data"
global src_path = "$DATA_PATH/src"

global out_month = "1510"
global OUT_PATH = "$root_dir" + "/out/" + "$out_month"


* Global Files
*-------
global firms_geocodes $DATA_PATH/geocode/cleaned_winners

global house_sample_dta $DATA_PATH/house_sample
global firm_panel_path $DATA_PATH/firm_panel
global firms_static_path $DATA_PATH/firms_static

global patzip_sample_path $DATA_PATH/patient_sample


/* XXX Everything below here should be deprecated! */


* Project parameters
*-------------------
global ppb_to_micgperm3 = 1.883146457307684
global SOUTH_CA_LAT = 37.12


* Main data
*------------

* Source: aermod/meteorological, other
global scaqmd_metsites_csv ../src/scaqmd_metsites.csv

* Source: House
