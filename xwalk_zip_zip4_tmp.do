/*
The file `data_fake.dta` has several variables that should have ZIP+4's.
However, the last 4 digits of these variables are "0000" so they won't merge to
any of the real datasets.

This file creates a simple workaround crosswalk to replace the ZIP + "0000"s
with actual, workable ZIP+4's.

*/
clear all
use ../data/zip4
keep zip4
gen zip = substr(zip4, 1, 5) + "0000"
bys zip: keep if _n == 1
save ../data/tmp_xwalk_zip_zip4, replace
