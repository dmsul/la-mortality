global FAKEDATA = 1

*** File Paths
if $FAKEDATA {
    global BENE_DATA ../data/data_fake                          // Core Medicare data
    global ZIPS_AERMOD ../data/zips_aermod                      // Core AERMOD data
    global ZIPS_AERMOD_SYMM_ROOT ../data/zips_aermod_symmetric   // Derived from AERMOD
    global ZIPS_BLOCK2000 ../data/zip4s_block2000.dta           // X-walk, zip4->block2000
    global ZIPS_HOTZONE_FLAG ../data/zips_coast_hotzone_flag.dta
    global BLOCKGROUP_INFO ../data/blockgroup_2000              // Demographic info
    global OUT_PATH ..\out                                      // Folder for output
}
else {
    global BENE_DATA Y:\Shares\CMS\Sullivan\Data\data_real                          // Core Medicare data
    global ZIPS_AERMOD Y:\Shares\CMS\Sullivan\Data\zips_aermod                      // Core AERMOD data
    global ZIPS_AERMOD_SYMM_ROOT Y:\Shares\CMS\Sullivan\Data\zips_aermod_symmetric   // Derived from AERMOD
    global ZIPS_BLOCK2000 Y:\Shares\CMS\Sullivan\Data\zip4s_block2000.dta           // X-walk, zip4->block2000
    global ZIPS_HOTZONE_FLAG Y:\Shares\CMS\zips_coast_hotzone_flag.dta
    global BLOCKGROUP_INFO Y:\Shares\CMS\Sullivan\Data\blockgroup_2000              // Demographic info
    global OUT_PATH Y:\Shares\CMS\Sullivan\Results                                  // Folder for output
}


** Default Regression Specifications

* global CHEMS nox napthalene ammonia benzene lead nickel arsenic cadmium formaldehyde chromium asbestos co rog sox tsp
* global CHEMS nox co rog sox tsp
global CHEMS nox

global interact_diagnoses

global HOTZONE_ONLY = 0

global reg_command reg

global timespans 1 3 5 10  // Time horizon for outcomes (e.g., 3-year mortality)
global outcomes /// Health outcomes to examine
    death_date ///
    ami_ever alzh_ever copd_ever diabetes_ever hip_fracture_ever ///
    stroke_tia_ever cancer_lung_ever cancer_any_ever asthma_ever ///
    majordepression_ever migraine_ever hypert anxiety
global X aermod_diff aermod_pre        // X's of interest
global W ///                           // Other controls
    agebin_67-agebin_90 male ///
    bg_pct_8th_or_less bg_pct_9th_to_12th bg_pct_some_coll bg_pct_assoc_degree ///
    bg_pct_bach_degree bg_pct_grad_degree ///
    bg_pct_black bg_pct_hispanic ///
    bg_pct_renter_occ bg_pct_vacant bg_med_house_value ///
    bg_med_hh_inc

global pre_var aer_nox_pre

global outopt bdec(5) sdec(5) bfmt(f) br asterisk(se) 


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

    foreach chem in $CHEMS {
        local filepath "${ZIPS_AERMOD_SYMM_ROOT}_`chem'.dta"
        * If file already exists, do nothing
        cap confirm file `filepath'
        if !_rc & 1 {
            continue
        }

        di "Creating `filepath'"
        preserve
        clear

        use ${ZIPS_AERMOD}_`chem'

        if "`chem'" == "nox" {
            egen aermod_`chem'_pre_1 = rowmean(aermod_`chem'_2000*)
            egen aermod_`chem'_pre_3 = rowmean(aermod_`chem'_1998* ///
                                               aermod_`chem'_1999* ///
                                               aermod_`chem'_2000*)
            egen aermod_`chem'_pre_5 = rowmean(aermod_`chem'_1996* ///
                                               aermod_`chem'_1997* ///
                                               aermod_`chem'_1998* ///
                                               aermod_`chem'_1999* ///
                                               aermod_`chem'_2000*)
        }
        else {
            egen aermod_`chem'_pre_1 = rowmean(aermod_`chem'_2000)
            egen aermod_`chem'_pre_3 = rowmean(aermod_`chem'_2000)
            egen aermod_`chem'_pre_5 = rowmean(aermod_`chem'_2000)
        }

        egen aermod_`chem'_post_1 = rowmean(aermod_`chem'_2001*)
        egen aermod_`chem'_post_3 = rowmean(aermod_`chem'_2001* ///
                                            aermod_`chem'_2002* ///
                                            aermod_`chem'_2003*)
        egen aermod_`chem'_post_5 = rowmean(aermod_`chem'_2001* ///
                                            aermod_`chem'_2002* ///
                                            aermod_`chem'_2003* ///
                                            aermod_`chem'_2004* ///
                                            aermod_`chem'_2005*)

        if "`chem'" == "nox" {
            cap drop aermod*q*
        }
        else {
            cap drop aermod_`chem'_2*
            cap drop aermod_`chem'_19*
        }

        tostring zip4, replace

        save `filepath', replace
        restore
    }
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
    foreach chem in $CHEMS {
        local filepath "${ZIPS_AERMOD_SYMM_ROOT}_`chem'.dta"
        di "Merging `chem'"
        merge m:1 zip4 using `filepath', keep(1 3) nogen
    }

    * Merge in Block Group/Tract
    di "Merging Block ID"
    merge m:1 zip4 using $ZIPS_BLOCK2000, keep(1 3) nogen
    gen blkgrp = substr(block2000, 1, 12)
    gen tract = substr(blkgrp, 1, 11)
    drop block2000
    gen bg = blkgrp
    di "Merging Block Group info"
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

    * Merge in "Hotzone" flag
    di "Merging 'Hotzone' flag"
    merge m:1 zip4 using $ZIPS_HOTZONE_FLAG, keep(1 3) nogen


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
    cap drop aer_*_pre
    cap drop aer_*_diff
    local aermod_diff_band = min(`timespan', 5)  // Max AERMOD diff is 5 years
    foreach chem in $CHEMS {
        gen aer_`chem'_pre = aermod_`chem'_pre_`aermod_diff_band'
        gen aer_`chem'_diff = aermod_`chem'_post_`aermod_diff_band' - aer_`chem'_pre
    }

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


    cap drop aer_pre_max
    egen aer_pre_max = rowmin($pre_var)    // for check in `sample'

    * Sample restriction
    local max_move_year = 2000 + `timespan'
    cap drop sample
    gen sample = ///
        outcome_years_after_treat > 0 & ///  Didn't have 'outcome' before treatment
        startyear_geo_movein < 1999 & ///    Moved in before 1999
        stayer_thru_year >= `max_move_year' & /// Didn't move out too soon
        enter_sample_year <= 2000 & ///      Observed in sample before treatment
        aer_pre_max > 0 & aer_pre_max < . & ///Non-zero pollution exposure
        age_in_2000 >= 65 //                 At least 65 before treatment

    if $HOTZONE_ONLY {
        replace sample = sample * hotzone
    }

    if "`outcome'" != "death_date" {
        replace sample = sample * (death_years_after_treat > `timespan')
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
    count if aer_pre_max > 0
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
    unab full_x_list : $X
    local tmp "`full_x_list'"
    local X_num: list sizeof local(tmp)
    if `X_num' == 2 {
        local pre_var : word 2 of `tmp'
        local post_var : word 1 of `tmp'
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

prog def reg_loops
    args rep
    if "`rep'" == "" {
        local replace replace
    }
    else {
        local replace
    }

    foreach outcome in $outcomes {
        foreach timespan in $timespans {
            di "Main: `outcome' `timespan'"
            main_reg `outcome' `timespan' `replace'
            local replace
        }
    }
end

