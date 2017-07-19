/* Creates monitor panel by hour, day, month, quarter, and year.  */

set trace off
set more off
clear all

run clean/io


* I/O
*-----
global monitor_src_path $src_path/monitor

* Monitor metadata
global mon_location_xls $monitor_src_path/location.xls
* Monitor data
global mon_src_pm10_xls $monitor_src_path/pm10StdDaily20120709.xls
global IN_TSP_STEM $monitor_src_path/dlypm // {9,0}.dat

* Temp files for this script
global metadata_clean $DATA_PATH/tmp_c1mon_metadata
global tsp_clean $DATA_PATH/tmp_c1mon_clean_tsp
global pm10_clean $DATA_PATH/tmp_c1mon_clean_pm10
global MID_hourly $DATA_PATH/tmp_c1mon_hours
global MID_toUTM $DATA_PATH/tmp_c1mon_UTM.csv
global MID_fromUTM $DATA_PATH/tmp_c1mon_UTM.dta

* Output (paths from io.do)
global monitor_hour $DATA_PATH/monitor_hour
global monitor_day_stem $DATA_PATH/monitor_day
global monitor_day_allchem $DATA_PATH/monitor_day
global monitor_month $DATA_PATH/monitor_month
global monitor_quarter_dta $DATA_PATH/monitor_quarter
global monitor_year $DATA_PATH/monitor_year


* Parameters 
*------------
global MIN_READS = 8    //Min number of readings to count a whole day
global chems nox no no2 ozone co coh ltsc
global KEEP_METADATA x y utm_east utm_north countyname

prog def main
    clean_metadata
    clean_tsps
    clean_pm10
    clean_chems_hourly
    clean_chems_daily
    clean_chems_mqy
end

prog def clean_metadata
    clear
    import excel using $mon_location_xls, first case(l)
    keep site latitude longitude utmeast utmnorth utmzone countyname
    drop if site==.
    duplicates drop

    ren utmz utm_zone
    ren utmn utm_north1
    ren utme utm_east1
    ren latitude y
    ren longitude x

    destring utm_zone, replace

    /* Their geocodes (correctly) span two UTM zones. I never use anything but zone
    * 11, so reproject from lat/lon to get everything in UTM zone 11. */
    preserve
    keep site y x
    duplicates drop
    // Check uniqueness
    bys site: gen temp = _N
    assert temp == 1
    drop temp
    outsheet using $MID_toUTM, comma replace
    restore
    cap rm $MID_fromUTM
    !python -m py4stata.xy_to_utm $MID_toUTM $MID_fromUTM

    merge m:1 site using $MID_fromUTM, ///
        keepusing(utm_east utm_north) assert(3) nogen

    * Compare their utm to mine [1/13/2015: we're exact up to rounding]
    foreach direction in east north {
        gen diff_`direction' = utm_`direction' - utm_`direction'1 if utm_zone == 11
        assert abs(diff_`direction') <= 1 if diff_`direction' != .
        drop diff_`direction'
    }
    drop utm_east1 utm_north1 utm_zone

    compress
    save $metadata_clean, replace
end

prog def clean_tsps
    cap confirm file $tsp_clean.dta
    if _rc == 0 exit

    clear
    tempfile base_tsp
    save `base_tsp', emptyok

    foreach y in 9 0 {
        infix   str basin_lt 1 ///
                str site 2-5 ///
                year 6-9 ///
                month 10-11 ///
                day 12-13 ///
                pm10cl 14-21 ///
                pm10dich 22-29 ///
                pm10k 30-37 ///
                pm10nh4 38-45 ///
                pm10no3 46-53 ///
                pm10pb 54-61 ///
                pm10so4 62-69 ///
                pm10totc 70-77 ///
                pmcrs 78-85 ///
                pmfine 86-93 ///
                tsp 94-101 ///
                tsppb 102-109 ///
                tspso4 110-117 ///
                tspno3 118-125 ///
                cohmx2h 126-133 ///
                cohav24 134-141 ///
                ltscmx1h 142-149 ///
                ltscav24 150-157 ///
                dayofwk 158  ///
                using $IN_TSP_STEM`y'.dat, clear
                
        destring site, replace force
        drop if site==.
        
        gen num_date = date(string(month)  + "/" + string(day) + "/" + string(year), "MDY")
            
        append using `base_tsp'
        save `base_tsp', replace
    }

    keep site num_date tsp

    save $tsp_clean, replace
end
prog def clean_pm10
    cap confirm file $pm10_clean.dta
    if _rc == 0 exit

    clear
    tempfile base_pm
    save `base_pm', emptyok

    forval y=1990/2011 {
        import excel $mon_src_pm10_xls, clear firstrow case(lower) sheet("`y'")
        append using `base_pm'
        save `base_pm', replace
    }

    collapse (mean) pm10 = value, by(site date)
    ren date num_date

    save $pm10_clean, replace
end

prog def clean_chems_hourly
    foreach chem in $chems {    

        tempfile base
        clear
        save `base', empty replace
        
        foreach timeblock in "19901994" "19951999" "20002004" "20052009" "20102011" {
            di as err "Processing `chem', `timeblock'"
            if "`chem'"=="nox" & "`timeblock'"!="20102011" local delim ","
            else local delim "|"
        
            insheet using $monitor_src_path/`chem'`timeblock'.txt, clear delim("`delim'")
            
            drop if inlist(site, 3220, 3221) // Meta data is wrong for (one of) these guys.
            
            * Sig figs ????
            if "`chem'"=="coh" replace obs = round(obs,.1)
            replace obs = . if obs < 0
            * Date vars
            if "`chem'"=="nox" & "`timeblock'"!="20102011" gen num_date = date(date,"MDY")
            else gen num_date = date(date,"YMD")
            drop date
            * Within chemical save
            append using `base'
            save `base', replace        
        }
        rename obs `chem'

        * Save individual chemical's hourly
        save ${monitor_hour}_`chem', replace
    }
end

prog def clean_chems_daily
    local daily_mid $DATA_PATH\tmp_c1mon_middaily.dta
    cap confirm file `daily_mid'
    if _rc == 601 {
        _daily_core
        save `daily_mid', replace
    }
    else cap use `daily_mid'
    _daily_metadata
    compress
    save $monitor_day_allchem, replace
end
prog def _daily_core
    /* Prep for collapse to Daily */

    * Keep days with at least MIN_OBS readings
    foreach chem in $chems {
        di "Process daily for `chem'"
        use ${monitor_hour}_`chem'
        rename start_hour hour
        bys site num_date: egen nomiss_count_`chem' = count(`chem')
        replace `chem' = -99 if nomiss_count_`chem'<$MIN_READS
        drop nomiss_count_`chem'
        * Create duration weighting forward (see Schlenker, Walker (2013 wp))
        bys site (num_date hour): replace `chem' = `chem'[_n-1] ///
                    if (`chem'==.) & (abs(num_date - num_date[_n-1])<=1) & (`chem'!=-99)
        replace `chem' = . if `chem' == -99
        * Collapse to daily
        local maxcollapse `chem'_max = `chem'
        collapse (mean) `chem' (max) `maxcollapse', by(site num_date)
        save ${monitor_day_stem}_`chem', replace
    }

    * Combine all
    di "Combine all daily"
    clear
    foreach chem in $chems {
        di "Combine daily for `chem'"
        if `=_N' == 0 use ${monitor_day_stem}_`chem'
        else merge 1:1 site num_date using ${monitor_day_stem}_`chem', nogen
    }
    cap merge 1:1 site num_date using $tsp_clean, nogen
    cap merge 1:1 site num_date using $pm10_clean, nogen
    // Saved by calling function
end
prog def _daily_metadata

    merge m:1 site using $metadata_clean, keepusing($KEEP_METADATA) keep(1 3) assert(2 3) nogen

    gen year = year(num_date)
    gen month = month(num_date)
    gen day = day(num_date)
    drop num_date

    * Drop sites with only missing obs
    foreach chem in $chems {
        bys site: egen count_`chem' = count(`chem')
    }
    egen count_all = rowmax(count_*)
    bys site: egen maxcount = max(count_all)
    drop if maxcount==0
    drop *count*
end

prog def clean_chems_mqy
    global VARLIST $chems `chemmax'

    use $monitor_day_allchem, clear

    * Month
    collapse (mean) $VARLIST, by(site year month)

    fillin site year month
    drop _fillin

    merge m:1 site using $metadata_clean, ///
        keepusing($KEEP_METADATA) keep(1 3) assert(2 3) nogen

    save $monitor_month, replace

    * Quarter
    use $monitor_day_allchem, clear
    gen quarter = floor((month-1)/3) + 1
    tab month quarter
    collapse (mean) $VARLIST, by(site year quarter)
    fillin site year quarter
    drop _fillin

    merge m:1 site using $metadata_clean, ///
        keepusing($KEEP_METADATA) keep(1 3) assert(2 3) nogen

    save $monitor_quarter_dta, replace

    * Year
    cap use $monitor_quarter_dta
    collapse (mean) $VARLIST, by(site year)
    fillin site year
    drop _fillin

    merge m:1 site using $metadata_clean, ///
        keepusing($KEEP_METADATA) keep(1 3) assert(2 3) nogen

    save $monitor_year, replace
end

main
