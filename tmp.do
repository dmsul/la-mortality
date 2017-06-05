clear all
set more off

global BENE_DATA ../data/data_fake
global ZIPS_AERMOD_PRE ../data/zip5s_fake_aermod_pre
global ZIPS_AERMOD ../data/zips_aermod
global ZIPS_AERMOD_SYMM ../data/zips_aermod_symmetric


prog def symm_aermod_vars
    use $ZIPS_AERMOD
    egen aermod_pre_1 = rowmean(aermod_2000*)
    egen aermod_post_1 = rowmean(aermod_2001*)

    egen aermod_pre_3 = rowmean(aermod_1998* aermod_1999* aermod_2000*)
    egen aermod_post_3 = rowmean(aermod_2001* aermod_2002* aermod_2003*)

    egen aermod_pre_5 = rowmean(aermod_1996* aermod_1997* aermod_1998* aermod_1999* aermod_2000*)
    egen aermod_post_5 = rowmean(aermod_2001* aermod_2002* aermod_2003* aermod_2004* aermod_2005*)

    drop aermod*q*

    * Just for the fake data
    if 1 {
        tostring zip4, replace
        replace zip4 = substr(zip4, 1, 5) + "0000"
        collapse (mean) aermod_pre* aermod_post*, by(zip4)
    }

    save $ZIPS_AERMOD_SYMM, replace
end


prog def load_data_and_basic_clean
    use $BENE_DATA
    drop state* county*
    keep if startyear_geo_movein <= 1999
end

//symm_aermod_vars

load_data_and_basic_clean

** Is in pre-2000 house thru date
* Calculate first year in sample
gen enter_sample_year = .
forval year=2013(-1)1999 {
    replace enter_sample_year = `year' if zip4_`year' != "."
}
* Calculate zip that corresponds to `startyear_geo_movein`
gen enter_zip = ""
forval year=2013(-1)1999 {
    replace enter_zip = zip4_`year' if ///
        (enter_sample_year == `year') | (`year' == 1999 & enter_sample_year <= 1999)
}
* Calculate last year of residence in `startyear_geo_movein` zip
gen stay_thru = .
gen death_year = year(death_date)
forval year=1999/2013 {
    replace stay_thru = `year' if zip4_`year' == enter_zip | ///
        (`year' > death_year & zip4_`year' == ".")  // Want to count people who died as "stayer"
}
drop death_year


** Practice Reg

gen death_delay = (death_date - date("1/1/2001", "MDY")) / 365


gen age_in_2000 = (date("1/1/2001", "MDY") - bene_birth_dt) / 365
gen age2 = age_in_2000^2

gen zip4 = enter_zip
merge m:1 zip4 using $ZIPS_AERMOD_PRE, keep(1 3) nogen
merge m:1 zip4 using $ZIPS_AERMOD_SYMM, keep(1 3) nogen


gen sample = death_delay > 0 & ///
             stay_thru >= 2002 & ///
             enter_sample_year <= 2000 & ///
             age_in_2000 >= 65

gen died_1year = death_delay <= 5
local band 5

gen aermod_diff_`band' = aermod_pre_`band' - aermod_post_`band'

reg died_1year aermod_diff_`band' aermod_pre_`band' age_in_2000 age2 if sample, robust
