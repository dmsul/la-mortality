*capture log close
*log using Y:\Shares\CMS\Sullivan\Logs\reg_main_log_6_25_17, text replace

/*
Regress "heath outcome realized w/in X years of pollution shock" on change in
pollution exposure as measured by AERMOD.
*/
clear all
set more off

global FAKEDATA = 1

if $FAKEDATA {
    global BENE_DATA ../data/data_fake                          // Core Medicare data
    global ZIPS_AERMOD ../data/zips_aermod                      // Core AERMOD data
    global ZIPS_AERMOD_SYMM_BYQ ../data/zips_aermod_symmetric_byq.dta   // Derived from AERMOD
    global ZIPS_BLOCK2000 ../data/zip4s_block2000.dta           // X-walk, zip4->block2000
    global BLOCKGROUP_INFO ../data/blockgroup_2000              // Demographic info
    global OUT_PATH ..\out                                      // Folder for output
}
else {
    global BENE_DATA Y:\Shares\CMS\Sullivan\Data\data_real                          // Core Medicare data
    global ZIPS_AERMOD Y:\Shares\CMS\Sullivan\Data\zips_aermod                      // Core AERMOD data
    global ZIPS_AERMOD_SYMM_BYQ Y:\Shares\CMS\Sullivan\Data\zips_aermod_symmetric_byq.dta   // Derived from AERMOD
    global ZIPS_BLOCK2000 Y:\Shares\CMS\Sullivan\Data\zip4s_block2000.dta           // X-walk, zip4->block2000
    global BLOCKGROUP_INFO Y:\Shares\CMS\Sullivan\Data\blockgroup_2000              // Demographic info
    global OUT_PATH Y:\Shares\CMS\Sullivan\Results                                  // Folder for output
}

global outopt bdec(5) sdec(5) bfmt(f) br asterisk(se) 


qui {
prog def verify_out_path
    /* Create output folder if needed */
    cap confirm file "$OUT_PATH/nul"
    if _rc {
        di as err "Creating $OUT_PATH"
        !mkdir $OUT_PATH
    }
end


prog def _gen_aermod_pre_post
    * Create multi-year averages of AERMOD exposure centered around 2000/2001 shock.

    * If file already exists, do nothing
    cap confirm file $ZIPS_AERMOD_SYMM_BYQ
    if !_rc & 1 {
        exit
    }

    di "Creating $ZIPS_AERMOD_SYMM_BYQ"
    preserve
    clear

    use $ZIPS_AERMOD

    forval q=1/4 {
        gen aermod_pre_1_q`q' = aermod_2000q`q'
        gen aermod_post_1_q`q' = aermod_2001q`q'

        egen aermod_pre_3_q`q' = rowmean(aermod_1998q`q' aermod_1999q`q' aermod_2000q`q')
        egen aermod_post_3_q`q' = rowmean(aermod_2001q`q'* aermod_2002q`q'* aermod_2003q`q'*) 

        egen aermod_pre_5_q`q' = rowmean(aermod_1996q`q' aermod_1997q`q' aermod_1998q`q' aermod_1999q`q' aermod_2000q`q')
        egen aermod_post_5_q`q' = rowmean(aermod_2001q`q' aermod_2002q`q' aermod_2003q`q' aermod_2004q`q' aermod_2005q`q')
    }

    forval y=1994/2006 {
        forval q=1/4 {
            drop aermod_`y'q`q'
        }
    }

    tostring zip4, replace

    save $ZIPS_AERMOD_SYMM_BYQ, replace
    restore
end

prog def data_prep
    use $BENE_DATA
    drop state* county*

    * Get last year person lives in ZIP+4 given in `startyear_geo_movein`
    // Calculate first year in sample
    gen enter_sample_year = .
    forval year=2013(-1)1999 {
        replace enter_sample_year = `year' if zip4_`year' != "."
    }
    // Calculate zip that corresponds to `startyear_geo_movein`
    gen enter_zip = ""
    forval year=2013(-1)1999 {
        replace enter_zip = zip4_`year' if ///
            (enter_sample_year == `year') | (`year' == 1999 & enter_sample_year <= 1999)
    }
    // Calculate last year of residence in `startyear_geo_movein` zip
    gen death_year = year(death_date)
    gen stayer_thru_year = .
    forval year=1999/2013 {
        replace stayer_thru_year = `year' if (zip4_`year' == enter_zip) & (`year' <= death_year)
    }
    forval year=1999/2013 {  // Want to count people who died as "stayer"
        replace stayer_thru_year = 2013 if ///
            `year' == death_year & zip4_`year' == enter_zip
    }
    gen tmp = stayer_thru_year < death_year | stayer_thru_year == 2013
    tab death_year stayer_thru_year
    assert tmp
    drop death_year tmp

    * For merging with other datafiles
    if $FAKEDATA {
        gen zip = enter_zip
        merge m:1 zip using ../data/tmp_xwalk_zip_zip4, keep(1 3) nogen
        drop zip
    }
    else {
        gen zip4 = enter_zip
    }

    * Merge in Aermod pre/post averages
    _gen_aermod_pre_post    // Gen file of pre/post aermod averages
    merge m:1 zip4 using $ZIPS_AERMOD_SYMM_BYQ, keep(1 3) nogen

    * Merge in Block Group/Tract
    merge m:1 zip4 using $ZIPS_BLOCK2000, keep(1 3) nogen
    gen blkgrp = substr(block2000, 1, 12)
    gen tract = substr(blkgrp, 1, 11)
    drop block2000
    gen bg = blkgrp
    merge m:1 bg using $BLOCKGROUP_INFO, keep(1 3) nogen
    drop bg

    * Age bins
    gen age_in_2000 = (date("1/1/2001", "MDY") - bene_birth_dt) / 365
    egen agebins = cut(age_in_2000), at(0, 65, 67, 70, 73, 76, 80, 85, 90, 2000)
    qui tab agebins, gen(agebin_)
    qui levelsof agebins
    local i = 1
    foreach var in `r(levels)' {
        ren agebin_`i' agebin_`var'
        local i = `i' + 1
    }
    drop agebin_0

    * New cancer variable
    egen cancer_any_ever = rowmin(cancer*ever)
end


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
    local aermod_diff_band = min(`timespan', 5)  // Max AERMOD diff is 5 years
    forval q=1/4 {
        cap drop aermod_pre_q`q'
        cap drop aermod_diff_q`q'
        gen aermod_pre_q`q' = aermod_pre_`aermod_diff_band'_q`q'
        gen aermod_diff_q`q' = aermod_post_`aermod_diff_band'_q`q' - aermod_pre_q`q'
    }
    cap drop aermod_pre_mean
    egen aermod_pre_mean = rowmean(aermod_pre_q*)

    * Sample restriction
    local max_move_year = 2000 + `timespan'
    cap drop sample
    gen sample = ///
        outcome_years_after_treat > 0 & ///  Didn't have 'outcome' before treatment
        startyear_geo_movein < 1999 & ///    Moved in before 1999
        stayer_thru_year >= `max_move_year' & /// Didn't move out too soon
        enter_sample_year <= 2000 & ///      Observed in sample before treatment
        aermod_pre_mean > 0 & aermod_pre_mean < . & ///Non-zero pollution exposure
        age_in_2000 >= 65 //                 At least 65 before treatment

    if "`outcome'" != "death_date" {
        replace sample = sample * (death_years_after_treat > `timespan')
    }

    *** Regression ***

    reg outcome_within_limit $X $W if sample, cluster(blkgrp) // a(tract)

    * Diagnostics
    cap drop in_reg
    gen in_reg = e(sample)
    tab sample in_reg
    count if sample & bg_pct_9th_to_12th == .
    count if outcome_years_after_treat > 0
    count if startyear_geo_movein < 1999
    count if stayer_thru_year >= 2002
    count if enter_sample_year <= 2000
    count if aermod_pre_mean > 0
    count if age_in_2000 >= 65


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
    summ aermod_pre_q* if in_reg
    summ aermod_diff_q* if in_reg
    summ aermod_pre_mean if in_reg
    local pre_mean = r(mean)

    outreg2 using "${OUT_PATH}/${OUT_NAME}.xls", excel `replace' ///
        ctitle("`outcome_label'") ///
        addstat("Outcome mean", `outcome_mean', ///
                "Aermod_pre mean", `pre_mean')
end

}


verify_out_path
data_prep

** Regressions

local outcomes /// Health outcomes to examine
    death_date ami_ever alzh_ever copd_ever diabetes_ever hip_fracture_ever ///
    stroke_tia_ever cancer_lung_ever cancer_any_ever asthma_ever ///
    majordepression_ever migraine_ever hypert anxiety
local timespans 1 3 5 10  // Time horizon for outcomes (e.g., 3-year mortality)

* Basic Specification
global OUT_NAME "regs_by_quarter"
global X aermod_diff_q* aermod_pre_q*        // X's of interest
global W ///                           // Other controls
    agebin_67-agebin_90 male ///
    bg_pct_8th_or_less bg_pct_9th_to_12th bg_pct_some_coll bg_pct_assoc_degree ///
    bg_pct_bach_degree bg_pct_grad_degree ///
    bg_pct_black bg_pct_hispanic ///
    bg_pct_renter_occ bg_pct_vacant bg_med_house_value ///
    bg_med_hh_inc

local replace replace
foreach outcome in `outcomes' {
    foreach timespan in `timespans' {
        di "Main: `outcome' `timespan'"
        main_reg `outcome' `timespan' `replace'
        local replace
    }
}

cap log close
