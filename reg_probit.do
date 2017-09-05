*capture log close
*log using Y:\Shares\CMS\Sullivan\Logs\reg_main_log_6_25_17, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

run methods // Import functions, globals, etc.
** Switch to probit
global reg_command probit


verify_out_path
data_prep

** Regressions

* Basic Specification
global OUT_NAME "regs_probit"          // Filename for results
global X aermod_diff aermod_pre        // X's of interest
reg_loops                              // Change globals to affect regressions

* Interact aermod_diff with age bins
global OUT_NAME "regs_interact_age_probit"
global X c.aermod_diff#i.agebins aermod_pre
reg_loops

cap log close
