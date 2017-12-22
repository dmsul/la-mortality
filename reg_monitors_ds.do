capture log close
* log using Y:\Shares\CMS\Sullivan\Logs\reg_monitors_ds, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

run methods // Import functions, globals, etc.

global CHEMS invd15_nox
global exposure_pre_var invd15_nox_pre

verify_out_path
data_prep

** Regressions

* Basic Specification
global OUT_NAME "reg_monitors_ds"          // Filename for results
global X invd15_nox_diff invd15_nox_pre    // X's of interest
reg_loops                                  // Change globals to affect regressions

cap log close
