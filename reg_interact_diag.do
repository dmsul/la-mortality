*capture log close
*log using Y:\Shares\CMS\Sullivan\Logs\reg_main_log_6_25_17, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

run methods // Import functions, globals, etc.

global interact_diagnoses /// Health outcomes to examine
    ami_ever alzh_ever copd_ever diabetes_ever hip_fracture_ever ///
    stroke_tia_ever cancer_any_ever asthma_ever hypert 

global outcomes death_date


verify_out_path
data_prep

** Regressions

* Basic Specification
global OUT_NAME "regs_interact_diag"
global X aer_nox_diff aer_nox_diff_d* aer_nox_pre aer_nox_pre_d*        // X's of interest

reg_loops

cap log close
