clear all
set more off

global BENE_DATA ../data/data_fake                          // Core Medicare data
global ZIPS_GEODATA ../data/zip4                            // Lat/lon, Census
global ZIPS_AERMOD ../data/zips_aermod                      // Core AERMOD data
global ZIPS_AERMOD_SYMM ../data/zips_aermod_symmetric.dta   // Derived from AERMOD
global OUT_PATH ..\out                                      // Folder for output

global SAVE 0       // Switch for over-writing past results
global FAKEDATA 1   // Switch for using fake data (ZIP + "0000" fix)


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
    cap confirm file $ZIPS_AERMOD_SYMM   // If file already exists, do nothing
    if !_rc & 1 {
        exit
    }
    di "Creating $ZIPS_AERMOD_SYMM"
    preserve
    clear

    use $ZIPS_AERMOD
    egen aermod_pre_1 = rowmean(aermod_2000*)
    egen aermod_post_1 = rowmean(aermod_2001*)

    egen aermod_pre_3 = rowmean(aermod_1998* aermod_1999* aermod_2000*)
    egen aermod_post_3 = rowmean(aermod_2001* aermod_2002* aermod_2003*)

    egen aermod_pre_5 = rowmean(aermod_1996* aermod_1997* aermod_1998* aermod_1999* aermod_2000*)
    egen aermod_post_5 = rowmean(aermod_2001* aermod_2002* aermod_2003* aermod_2004* aermod_2005*)

    drop aermod*q*

    tostring zip4, replace

    save $ZIPS_AERMOD_SYMM, replace
    restore
end

prog def data_prep
    use $BENE_DATA
    drop state* county*
    keep if startyear_geo_movein <= 1999

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
        replace stayer_thru_year = `year' if zip4_`year' == enter_zip | ///
            (`year' > death_year & zip4_`year' == ".")  // Want to count people who died as "stayer"
    }
    drop death_year

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
    merge m:1 zip4 using $ZIPS_AERMOD_SYMM, keep(1 3) nogen

    * Merge in Block Group/Tract
    merge m:1 zip4 using $ZIPS_GEODATA, keep(1 3) keepusing(blkgrp) nogen
    gen tract = substr(blkgrp, 1, 11)

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
end


prog def main_reg
    args outcome timespan replace

    * Number of years lived after treatment
    cap drop outcome_years_after_treat
    gen outcome_years_after_treat = (`outcome' - date("1/1/2001", "MDY")) / 365

    * Sample restriction
    cap drop sample
    gen sample = ///
        outcome_years_after_treat > 0 & ///  Didn't have 'outcome' before treatment
        stayer_thru_year >= 2002 & ///       Didn't move before 2002
        enter_sample_year <= 2000 & ///      Observed in sample before treatment
        age_in_2000 >= 65 //                 At least 65 before treatment


    * Gen Y var
    cap drop outcome_within_limit
    gen outcome_within_limit = outcome_years_after_treat <= `timespan'

    * Create AERMOD variables based on timespan
    cap drop aermod_pre
    cap drop aermod_diff
    local aermod_diff_band = min(`timespan', 5)  // Max AERMOD diff is 5 years
    gen aermod_pre = aermod_pre_`aermod_diff_band'
    gen aermod_diff = aermod_pre - aermod_post_`aermod_diff_band'

    *** Regression ***
    global X aermod_diff aermod_pre        // X's of interest
    global W agebin_67-agebin_90            // Other Controls

    reg outcome_within_limit $X $W if sample, cluster(blkgrp) // a(tract)

    * Format text for `outreg2`
    if "`outcome'" == "death_date" {
        local outcome_label "Mort."
    }
    else {
        local outcome_label = subinstr("`outcome'", "_ever", "", .)
    }
    local outcome_label = "`outcome_label' (`timespan'-yr)"

    qui summ outcome_within_limit
    local outcome_mean = `r(mean)'

    if $SAVE {
        global OUT_NAME "regs_main"
        outreg2 using $OUT_PATH\$OUT_NAME.xls, excel `replace' ///
            ctitle("`outcome_label'") addstat("Outcome mean", `outcome_mean')
    }

end

}


verify_out_path
data_prep

* Main regression loop
// Health outcomes to examine
local outcomes death_date ami_ever 
// Time horizon for outcomes (e.g., 3-year mortality)
local timespans 1 3 5 10

foreach outcome in `outcomes' {
    foreach timespan in `timespans' {
        main_reg `outcome' `timespan' `replace'
        local replace
    }
}
