capture log close
log using Y:\Shares\CMS\Sullivan\Logs\reg_kernels, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off


*** UNIFORM, 2km
run methods

global CHEMS tria5_nox
global exposure_pre_var tria5_nox_pre

verify_out_path
data_prep

global OUT_NAME "reg_unif2"          // Filename for results
global X tria5_nox_diff tria5_nox_pre    // X's of interest
reg_loops                                  // Change globals to affect regressions


*** TRIANGLE, 5km
clear all
run methods

global CHEMS tria5_nox
global exposure_pre_var tria5_nox_pre

verify_out_path
data_prep

global OUT_NAME "reg_tria5"          // Filename for results
global X tria5_nox_diff tria5_nox_pre    // X's of interest
reg_loops                                  // Change globals to affect regressions

cap log close
