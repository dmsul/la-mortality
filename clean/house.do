/*
This file creates a panel of house sales with hedonics in Southern California
from the cleaned Stata files developed by the Taubman Center from raw DataQuick
data.

Notes:
-"history-CA-San Luis Obispo" has no year variable
-Units (condos) with same zip4, coded at "zip4" level, but different (x,y).
    See 32211 & 32202 Shoreview Dr, Westlake Village, CA 91361-4231
*/

set more off 
set trace off
clear all

run clean/io.do


*  I/O
*--------
global DATA_PATH $DATA_PATH

* DataQuick
if inlist(c(hostname), "ThinkPad-PC", "Daniel-PC") {
    global DATAQUICK_PATH "d:\data\dataquick"
}
else {
    global DATAQUICK_PATH $DATA_PATH
}

global IN_SALES_SRC_STEM $DATAQUICK_PATH/history-CA/history-CA
global IN_ASSESS_SRC_STEM $DATAQUICK_PATH/Assessor-CA/Assessor-CA


* These files are created from empty
global MID_SALES $DATA_PATH/tmp_c1h_sales
global MID_ASSESS $DATA_PATH/tmp_c1h_assessor
* These are convenience files to break up long run times
global tmp_rawDQpanel $DATA_PATH/tmp_c1h_DQpanel_raw
global tmp_cleanDQpanel $DATA_PATH/tmp_c1h_DQpanel_clean

* Out
global house_sample_dta $house_sample_dta
global people_dta $DATA_PATH/houses_people
global places_dta $DATA_PATH/houses_places

* Parameters
*----------
quiet{
global LASTYEAR 2011
global HGRID_SIZE 100

/*
global COUNTIES `" "Los Angeles" "San Luis Obispo" "Kern" ///
        "San Bernardino" "Santa Barbara" "Ventura" "Orange" ///
        "Riverside" "San Diego" "Imperial" "'
*/
global COUNTIES `" "Los Angeles" "San Bernardino" "Orange" "Riverside" "'

global SALE_VARS year ///
	sr_property_id sr_unique_id ///	// ID's
	sa_site_house_nbr sa_site_dir sa_site_street_name /// // Address 1
	sa_site_suf sa_site_city sa_site_zip sa_site_unit_val /// // Address 2
	sa_x_coord sa_y_coord sa_geo_qlty sa_census_*  ///  // Geocodes
	sr_buyer sr_seller ///	// People involved
	origination_loan estimated_interest_rate_1 /// // Loan info
	use_code_std ///
	sr_val_transfer /// // Actual price
	mm_muni_name mm_fips_muni_code ///
	bad_history_trans bad_address dup_flag /// // Bad Data flags
        close_repeat_sale poss_correction  ///
	sa_site_mail_same /// Id's owner or renter occupied.
	sr_date_trans ///
	transfer ///
	sr_tran_type /// 
	partial_sale ///
	distress_indicator ///
	corporation_buyer /// 
	corporation_seller

/* 
    sr_tran_type codes 
        1 Resale, 2 Refi or Equity, 3 Subdivision, 4 Construction, 5 Timeshare
    Corp buyer codes
        0 non-corp, 1 corp, 2 trust or estate, 3 gov't or housing agency
*/

global HEDONICS ///
	sa_property_id sa_site_zip sa_census_tract ///
        sa_census_block_group mm_fips_muni_code ///
	sa_sqft sa_nbr_rms sa_nbr_bath sa_nbr_bedrms sa_nbr_stories ///
        sa_lotsize sa_bldg_sqft sa_yr_blt ///	// Basic X's
	assr_year sa_val_assd sa_val_assd_land sa_val_assd_imprv ///
        sa_construction_qlty sa_cool_code sa_heat_code ///
        sa_roof_code sa_structure_nbr ///
        sa_yr_blt_effect   ///	// Date of major construction
        bad_assessor use_code_std sa_x_coord sa_y_coord sa_geo_qlty_code
	

prog drop _all
} // End quiet around globals

* Routines
*----------
quiet{
prog def main
    capture confirm file $tmp_rawDQpanel.dta
    if _rc == 601 {
        build_DataQuickpanel
        save $tmp_rawDQpanel, replace
    }

    capture confirm file $tmp_cleanDQpanel.dta
    if _rc == 601 {
        cap use $tmp_rawDQpanel
        basic_cleaning
        save $tmp_cleanDQpanel, replace
    }

    cap use $tmp_cleanDQpanel
    gen_hgrids

    * Clean dtypes, save
    recast long hgrid utm_east utm_north
    compress
    label data "From new DataQuick data; quarterly freq"
    save $house_sample_dta, replace
end

prog def build_DataQuickpanel
        build_sales_panel
        build_assess_panel
        merge_sales_assess
end

prog def build_sales_panel
    clear
    save "$MID_SALES", replace empty

    * Build from county/year files
    qui foreach cnty in $COUNTIES {
        noi di "`cnty'" _continue
        if inlist("`cnty'","Los Angeles","Orange","Riverside", ///
                                 "San Bernardino","San Diego") {
            forval year=1990/$LASTYEAR {
                if `year'!=$LASTYEAR noi di "." _continue
                else noi di "."
                _raw_file_prep `"`cnty'"' `year'
            }
        }
        else {
            noi di "..."
            _raw_file_prep `cnty'
        }
    }

    * Drop 90-day repeats
    bys sr_prop badsale (sr_date): gen daydiff = sr_date - sr_date[_n-1]
    drop if daydiff <= 90
    drop daydiff

    //_drop_duplicate_sales

    save $MID_SALES, replace
end
prog def _raw_file_prep
    args cnty year
    
    * Read data
    if "`year'"!="" use "$IN_SALES_SRC_STEM-`cnty'_`year'", clear
    else use "$IN_SALES_SRC_STEM-`cnty'", clear
    
    * Variable selection			
    cap gen int year = year(sr_date_tr)
    keep if inrange(year,1990,$LASTYEAR)
    keep $SALE_VARS
    
    * Flag non-standard sales
    /* Keep only 
        (0) Non-low priced sales 
        (1) arms-length transactions
        (2) non-transfers, 
        (3) re-sales, 
        (4) non-distressed/non-foreclosures, 
        (5) non-corporation buyers
    */	
    cap gen byte badsale = 0
    replace badsale = 0 if badsale==.
    replace badsale = 1 if  ///
                    sr_val_t<=15000 | ///
                    bad_history_tran!=0 | ///
                    transfer==1 | ///
                    sr_tran_type!=1 | ///
                    distress!=. | ///
                    corporation_buy == 3
    drop transfer bad_history_tran sr_tran_type distress
    
    * Single-family homes only
    keep if use_code_std==1
    drop use_code_std
    
    * Drop possible corrections
    drop if poss_corr==1
    drop poss_corr
    
    * Drop the bad sales (for now)
    drop if badsale==1
    
    * If multi-year, save year then append to master
    * Else, just append to master
    local filename = subinstr("`cnty'"," ","",.)
    if "`year'"!="" & "`year'"!="$LASTYEAR" {
        save midtemp_`filename'_`year', replace
    }
    else if "`year'"=="$LASTYEAR" {
        noi di as err "Doing the last year!"
        local T_1 = $LASTYEAR - 1
        forval y = 1990/`T_1' {
            append using midtemp_`filename'_`y'
            rm midtemp_`filename'_`y'.dta
        }
    }
    if inlist("`year'","","$LASTYEAR") {
        append using $MID_SALES
        compress
        save $MID_SALES, replace
    }
end
prog def _drop_duplicate_sales
    * Duplicates mostly taken care of by "badsale"
    * and (later) collapse to year level

    gen flag = dup!=0
    bys sr_prop: egen has_dup = max(flag)
    bys sr_prop: gen T = _N
    order T dup
    sort sr_prop sr_date_tr
    tab dup badsale
    browse badsale T dup sr_prop sa_x_coord sr_date sr_val_t ///
             sr_buyer sr_seller origination_loan ///
             if has_dup==1 & has_good==1
end
prog def build_assess_panel
    clear
    save $MID_ASSESS, replace empty

    qui foreach cnty in $COUNTIES  {
        use "$IN_ASSESS_SRC_STEM-`cnty'", clear
        keep $HEDONICS
        keep if use_code_std==1
        ren sa_property_id sr_property_id
        
        append using $MID_ASSESS
        save $MID_ASSESS, replace
    }	
end
prog def merge_sales_assess
    use $MID_SALES, clear
            
    merge m:1 sr_property_id using $MID_ASSESS
    assert (badsale==. & _merge==2) | (badsale==0 & _merge==3)
    drop _merge

end

prog def basic_cleaning
    * Drop missing geocodes (only two obs)
    drop if inlist(sa_x,0,.)	

    * Rename variables
    ren sa_nbr_rms rooms
    ren sa_nbr_bath baths
    ren sa_nbr_bed beds
    ren sa_nbr_stories stories
    ren sa_lotsize lotsize
     
    ren mm_muni_name county
    ren mm_fips_muni_code county_fips

    ren sa_x x
    ren sa_y_c y

    * Should be in negative degrees
    replace x = - x 

    * Remove variable name prefixes
    foreach var of varlist sa_* sr_* {
        local newvar = substr("`var'", 4, .)
        ren `var' `newvar'
    }
    foreach var of varlist site_* {
        local newvar = substr("`var'", 6, .)
        ren `var' `newvar'
    }

    * Recast zip code
    destring zip, replace

    * Flag properties that are mainly land (by ass'd values in 2011)
    gen landfrac = val_assd_land / (val_assd_land + val_assd_imp)
    gen byte only_land = landfrac > .95 if landfrac<.
    * drop if only_land == 1    // This is more likely lots that have since been demolished
    drop landfrac val_assd_land

    * Replace improbable assessor data with missing
    replace sqft = . if sqft < 200 
    replace beds = . if beds==0
    replace bath = . if bath==0
    replace rooms = . if rooms==0
    replace stories = . if stories==0
    replace lotsize = . if lotsize < 100

    * Create block group variable
    tostring census_tract census_block_group, replace
    forval i=1/5 {
        replace census_tract = "0" + census_tract if length(census_tract) < 6
    }
    gen bg = string(county_fips) + census_tract + census_block_group
    replace bg = "060" + bg
    assert length(bg) == 12
    drop census_block_group
    rename census_tract tract

    * Collapse to larger time unit
    _collapse_to_time "quarter"

    * Drop variables that are constant across all obs
    foreach var of varlist _all {
        qui summ `var'
        if "`r(sd)'"=="0" {
            di "Variable `var' is constant, dropping."
            drop `var'
        }
    }

    * Ln-price only for valid sales
    local cpiyear = 2014
    merge m:1 year quarter using $DATA_PATH/src/cpi_quarterly, ///
          keep(1 3) keepusing(cpi_to`cpiyear')
    assert _merge == 3 if year != .
    drop _merge
    gen p = val_transfer
    gen p_real = p * cpi_to`cpiyear'
    gen lnp = ln(p_real) 
    drop cpi_to`cpiyear' val_transfer
    lab var p_real "Real sale price (`cpiyear'$)"

    * Save separate data files for buyers/sellers, places
    /* These are long strings, don't want them floating around the main sales
     * data file. */
    // People
    preserve
    drop if lnp == . // Sales only (duh)
    keep property_id year quarter buyer seller
    save $people_dta, replace
    restore
    // Places
    preserve
    keep property_id county city
    duplicates drop
    save $places_dta, replace
    restore

    * Drop memory-heavy variables (will want these later)
    drop county city buyer seller house_nbr dir street_name unit_val suf
end
prog def _collapse_to_time
    args subyear
    if "`subyear'" == "quarter" {
        gen month = month(date_transfer)
        egen quarter = cut(month), at(1,4,7,10,13) icodes
        replace quarter = quarter + 1
        * Gen t for running quarter
        qui summ year
        gen tq = quarter + 4*(year - r(min))
        * Keep only one sale per quarter
        bys prop tq (val_t): keep if _n==_N
        drop month
        label var tq "Period index, quarter"
        label var quarter "Quarter of year"
    }
    else if "`subyear'" == "year" {
        bys prop year (val_t): keep if _n == _N
    }
end

prog def gen_hgrids

     _get_UTM

    keep prop utm_east utm_north x y
    bys prop: keep if _n==1

    ren utm_east own_utm_east
    ren utm_north own_utm_north

    * Bin by UTM
    gen utm_east = round(own_utm_east, $HGRID_SIZE)
    gen utm_north = round(own_utm_north, $HGRID_SIZE)
    egen hgrid = group(utm_east utm_north)

    * Save grid/utm info for merge later
    keep property_id hgrid *_east *_north
    tempfile gridid
    save `gridid'

    * Write grid info to CSV
    keep hgrid utm_east utm_north
    duplicates drop
    summ hgrid
    bys hgrid: gen N = _N
    assert N==1
    drop N

    * Merge hgrids back to main data
    use $tmp_cleanDQpanel, clear
    merge m:1 property_id using `gridid', assert(1 3) keep(2 3) nogen

    label var own_utm_east "UTM, zone 11N, east"
    label var own_utm_north "UTM, zone 11N, north"
    label var utm_east "${HGRID_SIZE}m grid UTM, zone 11N, east"
    label var utm_north "${HGRID_SIZE}m grid UTM, zone 11N, north"
end
prog def _get_UTM
    local tmp_houses_xy $DATA_PATH/tmp_c1h_xy.csv
    local tmp_houses_utm $DATA_PATH/tmp_c1h_utm.dta

    * Make unique property-XY table
    preserve
    keep property_id x y
    duplicates drop
    // Check uniqueness
    bys property_id: gen temp = _N
    assert temp == 1
    drop temp
    outsheet using `tmp_houses_xy', comma replace
    restore
    * Convert
    cap rm `tmp_houses_utm'
    !python -m py4stata.xy_to_utm `tmp_houses_xy' `tmp_houses_utm'

    merge m:1 property_id using `tmp_houses_utm', assert(3) nogen

end
} // end quiet around program defs

main

