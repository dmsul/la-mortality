capture log close
*log using Y:\Shares\CMS\Sullivan\Logs\reg_toxics_log, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

run methods // Import functions, globals, etc.

global CHEMS nox co rog

verify_out_path
data_prep

** Regressions

global outcomes /// Health outcomes to examine
    death_date


* All pollutants at once
global OUT_NAME "regs_toxic_hotzone$HOTZONE_ONLY"
global X aer_*_diff aer_*_pre        // X's of interest
global pre_var aer_*_pre             // variable(s) to condition sample on

reg_loops

* One pollutant at a time
global OUT_NAME "regs_toxic_single_chem_hotzone$HOTZONE_ONLY"

foreach chem in $CHEMS {
    global X aer_`chem'_diff aer_`chem'_pre
    global pre_var aer_`chem'_pre             // variable(s) to condition sample on
    di "Main: `outcome' `timespan'"
    reg_loops `append'
    local append append
}

cap log close
