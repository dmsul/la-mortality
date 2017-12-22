*capture log close
*log using Y:\Shares\CMS\Sullivan\Logs\reg_main_log_6_25_17, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

run methods // Import functions, globals, etc.

global INV_DIST_CHEMS NO2 O3 pm25

verify_out_path
data_prep

prog drop main_reg
prog def main_reg
    args outcome timespan replace

    * Number of years lived after treatment
    cap drop outcome_years_after_treat
    gen outcome_years_after_treat = (`outcome' - date("1/1/2001", "MDY")) / 365
    cap gen death_years_after_treat = (death_date- date("1/1/2001", "MDY")) / 365

    * Gen Y var
    cap drop outcome_within_limit
    gen outcome_within_limit = outcome_years_after_treat <= `timespan'

    * Create AERMOD variables based on timespan
    cap drop aer_*_pre
    cap drop aer_*_diff
    local aermod_diff_band = min(`timespan', 5)  // Max AERMOD diff is 5 years
    foreach chem in $CHEMS {
        gen aer_`chem'_pre = `chem'_pre_`aermod_diff_band'
        gen aer_`chem'_diff = `chem'_post_`aermod_diff_band' - aer_`chem'_pre
    }

    * Create monitor variables based on timespan
    cap drop *invd_pre
    cap drop *invd_diff
    cap drop *nm_pre
    cap drop *nm_diff
    local aermod_diff_band = min(`timespan', 5)  // Max AERMOD diff is 5 years
    foreach chem in $INV_DIST_CHEMS {
        foreach metric in invd nm {
            di "making `chem' `metric'"
            gen `chem'_`metric'_pre = `chem'_`metric'_pre_2
            gen `chem'_`metric'_diff = `chem'_`metric'_post_`aermod_diff_band' - `chem'_`metric'_pre
        }
    }

    /*
    cap drop aer_nox_*_d_*
    foreach diag in $interact_diagnoses {
        gen tmp_years_after_treat = (`diag' - date("1/1/2001", "MDY")) / 365
        gen tmp_diag_within_limit = tmp_years_after_treat <= `timespan'

        cap drop aermod_pre_d_`diag'
        cap drop aermod_diff_d_`diag'
        gen aer_nox_pre_d_`diag' = aer_nox_pre * tmp_diag_within_limit
        gen aer_nox_diff_d_`diag' = aer_nox_diff * tmp_diag_within_limit

        drop tmp*
    }
    */

    cap drop aer_pre_min
    local exposure_pre_var : word 1 of $X
    egen aer_pre_min = rowmin(`exposure_pre_var')    // for check in `sample'

    * Sample restriction
    local min_move_year = 1999
    local max_move_year = 2000 + `timespan'
    cap drop sample
    gen sample = ///
        outcome_years_after_treat > 0 & ///  Didn't have 'outcome' before treatment
        startyear_geo_movein < `min_move_year' & ///    Moved in before 1999
        stayer_thru_year >= `max_move_year' & /// Didn't move out too soon
        enter_sample_year <= 2000 & ///      Observed in sample before treatment
        aer_pre_min > 0 & aer_pre_min < . & ///Non-zero pollution exposure
        age_in_2000 >= 65 //                 At least 65 before treatment

    if $HOTZONE_ONLY {
        replace sample = sample * hotzone
    }

    if "`outcome'" != "death_date" {
        replace sample = sample * (death_years_after_treat > `timespan')
    }

    if 0 {
        foreach chem in $INV_DIST_CHEMS {
            foreach metric in invd nm {
                cap drop `chem'_`metric'_pre_bins
                cap drop `chem'_`metric'_pre_bin_*
                xtile `chem'_`metric'_pre_bins = `chem'_`metric'_pre if sample, n(10)
                tab `chem'_`metric'_pre_bins, missing
                tab `chem'_`metric'_pre_bins, gen(`chem'_`metric'_pre_bin_)
                drop `chem'_`metric'_pre_bin_1
            }
        }
    }

    *** Regression ***

    $reg_command outcome_within_limit $X $W if sample, cluster(blkgrp) // a(tract)

    * Diagnostics
    cap drop in_reg
    gen in_reg = e(sample)
    tab sample in_reg
    count if sample & bg_pct_9th_to_12th == .
    count if outcome_years_after_treat > 0
    count if startyear_geo_movein < 1999
    count if stayer_thru_year >= `max_move_year'
    count if enter_sample_year <= 2000
    count if aer_pre_min > 0
    count if age_in_2000 >= 65
    count if hotzone == 1


    * Format text for `outreg2`
    if "`outcome'" == "death_date" {
        local outcome_label "Mort."
    }
    else {
        local outcome_label = subinstr("`outcome'", "_ever", "", .)
    }
    local outcome_label = "`outcome_label' (`timespan'-yr)"

    summ outcome_within_limit if in_reg
    local outcome_mean = `r(mean)'

    * Get sample means for treatment variables
    cap unab full_x_list : $X
    if _rc == 0 {
        local tmp "`full_x_list'"
        local X_num: list sizeof local(tmp)
        local pre_var : word 1 of `tmp'
        local post_var : word 2 of `tmp'
    }
    else {
        local pre_var aer_nox_pre
        local post_var aer_nox_diff
    }
    summ `pre_var' if in_reg
    local pre_label = "Aermod pre mean"
    local pre_mean = r(mean)

    summ `post_var' if in_reg
    local post_label = "Aermod diff mean"
    local diff_mean = r(mean)

    summ aer_*_pre if in_reg
    summ aer_*_diff if in_reg

    outreg2 using "${OUT_PATH}/${OUT_NAME}.xls", excel `replace' ///
        ctitle("`outcome_label'") ///
        addstat("Outcome mean", `outcome_mean', ///
                "`pre_label'", `pre_mean', ///
                "`post_label'", `diff_mean')
end

** Regressions

* Basic Specification
local append
global OUT_NAME "reg_monitors_nk"
foreach metric in invd nm {
    foreach poll in pm25 NO2 O3 {
        global X `poll'_`metric'_pre `poll'_`metric'_diff 
        reg_loops `append'
        local append append
    }
}

foreach metric in invd nm {
    global X
    foreach poll in pm25 NO2 O3 {
        global X $X `poll'_`metric'_pre `poll'_`metric'_diff 
    }
    reg_loops `append'
    local append append
}

cap log close
