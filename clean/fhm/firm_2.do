quiet{
set trace off
set more off
clear all

run clean/io


* I/O
*-----
global src_path $src_path
global fhm_path $src_path/fhm

* In
global fromFHM_InitAlloc $fhm_path/InitAlloc
global fromFHM_xy_csv $fhm_path/fromFHM-xy.csv
global fromFHM_utm_csv $fhm_path/fromFHM-utm.csv
global fhm_electric_dta $fhm_path/electric
global fhm_allyrs6_dta $fhm_path/allyrs6

global firms_geocodes $firms_geocodes

global county_pop_dta $src_path/county_pop_1900-90
global carb_county_code_xwalk_dta $src_path/carb_county_code_xwalk

* temp
global tmp_new_utm $DATA_PATH/tmp_c1_utm.dta
global tmp_new_xy $DATA_PATH/tmp_c1_newxy.dta

* Out
global firm_panel_dta $firm_panel_path
global firms_static_dta $firms_static_path


* Subs
*----
prog def main
    prep_init_alloc
    prep_fhm_geocodes
    basic_cleaning
    set_geocodes
    set_metsite
    set_pop1990
    make_firmid

    compress
    sort ufacid year
    save $firm_panel_dta, replace

    * Write static
    keep ufacid facid firm_id electric year x y utm_east utm_north noxt ///
        county pop1990 metsite_*
    reshape wide noxt, i(ufacid) j(year)
    sort ufacid
    save $firms_static_dta, replace
end

prog def prep_init_alloc
    use $fromFHM_InitAlloc, clear
    rename fac_id facid
    gen year = real(substr(string(ExpDat), 1, 4))
    collapse (sum) permits=InitAlloc, by(facid year)
    compress
    replace permits = permits / 2000  // Convert units to tons
    save $DATA_PATH/tmp_c1f_permits, replace
end

prog def prep_fhm_geocodes
    * FHM's lat-long data
    insheet using $fromFHM_xy_csv, clear

    keep ufacid lat lon
    ren lat fhm_y
    ren lon fhm_x

    save $DATA_PATH/tmp_c1_fhm_xy, replace

    * FHM's original UTM data
    insheet using $fromFHM_utm_csv, clear
    duplicates drop

    ren facilityname locationname

    drop utmeast utmnorth
    ren east_m fhm_utm_east
    ren north_m fhm_utm_north
    ren facilityid facid

    foreach var of varlist fhm_utm* {
        replace `var' = . if `var'==0
    }

    drop locationzipp

    foreach var in name street city zip {
        ren location`var' f`var'
        cap replace f`var' = trim(f`var')
    }
    drop location*

    * For facility ID merge
    keep facid fhm_utm*
    duplicates tag facid, gen(tag)
    assert tag==0
    drop tag

    save $DATA_PATH/tmp_c1_fhm_utm, replace
end

prog def basic_cleaning
    /* Loads main data and restricts sample. */

    use $fhm_allyrs6_dta

    keep if RECLAIM1 == 1
    drop RECLAIM1

    merge m:1 facid ab using $fhm_electric_dta, keep(1 3) keepus(r2009)
    gen electric = r2009 == 1
    drop r2009 _merge

    * Set noxt values
    /*  Default noxt from CARB is taken from NEI, which forward fills missings.
        NEI sucks, forward fills are definitely terrible. Drop them.
        We're keeping zeros because [1/13/15] firm extinction?  */
    bys ufacid (year): gen truenox = nox if nox!=nox[_n-1] | _n==1 | nox==0
    drop noxt
    // Use RECLAIM values if we have them
    replace truenox = RECLAIMemiSum if RECLAIMemiSum!=.
    ren truenox noxt

    * Keep only firms with non-missing emissions 1997, 2003
    gen crit_year = inrange(year, 1997, 2003) & noxt != .
    bys ufacid: egen tot_crit = total(crit_year)
    foreach yr in 1997 1998 {
        gen is_`yr' = year == `yr' & noxt != .
        bys ufacid: egen has_`yr' = max(is_`yr')
    }
    keep if tot_crit == 7 ///
            | (tot_crit == 6 & has_1997 == 1) ///
            | (tot_crit == 5 & has_1997 ==1 & has_1998 == 1)

    * Drop firms who are mostly 0 emissions
    gen crit_is_0 = noxt == 0 & crit_year == 1
    bys ufacid: egen tot_crit0 = total(crit_is_0)
    tab tot_crit0
    drop if tot_crit0 >= 6 | ((tot_crit0 / tot_crit) > .75)
    drop tot_crit0

    keep if inrange(year, 1995, 2005)

    order ufacid year electric noxt
    drop nonaOZ1-crit_is_0 RECLAIM*

    * Merge permit info
    merge 1:1 facid year using $DATA_PATH/tmp_c1f_permits, keep(1 3) nogen

end

prog def set_geocodes
    /* Takes data in memory and adds geocodes */

    merge m:1 ufacid using $firms_geocodes, keep(1 3)
    ren _merge _mine

    /* As far as I can tell, FHM's xy's are the lowest-quality geocodes. I
     * don't use them for anything, so just leave them out except for checking
     * on how bad they are.

    merge m:1 ufacid using $DATA_PATH/tmp_c1_fhm_xy, keep(1 3)
    ren _merge _fhm

    vincenty lat lon fhm_y fhm_x, vin(dist) inkm
    */

    * Convert to UTM
    ren lat y
    ren lon x

    _py_xy_to_utm

    merge m:1 ufacid using $tmp_new_utm
    assert _merge == _mine
    drop _merge
    compress

    * Merge in FHM UTM
    merge m:1 facid using $DATA_PATH/tmp_c1_fhm_utm, keep(1 3) nogen
    gen utm_dist = sqrt((utm_east - fhm_utm_east)^2 + (utm_north - fhm_utm_north)^2)/1000

    ren utm_east my_east
    ren utm_north my_north

    * Use FHM's UTM if mine are missing
    foreach direction in east north {
        gen utm_`direction' = my_`direction'
        replace utm_`direction' = fhm_utm_`direction' if utm_`direction' == .
    }

    /* Eyeball check comparing FHM UTM and mine. Mine are better, but still
     * only street addresses. These midpoints get closer to stacks. 11/18/14 */
    foreach id in 19_SC_SC_18763 19_SC_SC_800075 {
        replace utm_east = (my_east + fhm_utm_east)/2 if ufacid == "`id'"
        replace utm_north = (my_north + fhm_utm_north)/2 if ufacid == "`id'"
    }

    /* This setup hits all but a few firms, most of whom are Navy bases.
     * Just leave them out for now. 11/18/14 */
    gen hasgeo = utm_east !=.
    tab electric hasgeo
    drop if hasgeo == 0
    drop hasgeo

    * Fill in XY when I use FHM's UTMS
    _py_utm_to_xy
    merge m:1 ufacid using $tmp_new_xy, update assert(4 5) nogen

    drop my_east my_north fhm_utm_east fhm_utm_north utm_dist _mine
    cap drop dist fhm_x fhm_y

    *Round UTM to nearest meter
    foreach var in utm_east utm_north {
        replace `var' = round(`var')
    }
end
prog def _py_xy_to_utm
    * Get unique ufacid-xy table
    preserve
    keep y x ufacid
    drop if x == .
    duplicates drop
    // Check uniqueness
    bys ufacid: gen temp = _N
    assert temp == 1
    drop temp
    * Convert via python
    local file_to_py $DATA_PATH/tmp_c1_xy_for_utm.csv
    outsheet using `file_to_py', comma replace
    restore
    cap rm $tmp_new_utm
    !python py4stata/xy_to_utm.py `file_to_py' $tmp_new_utm
end
prog def _py_utm_to_xy
    * Get unique ufacid-utm table
    preserve
    keep utm_east utm_north ufacid
    duplicates drop
    // Check uniqueness
    bys ufacid: gen temp = _N
    assert temp == 1
    drop temp
    local file_to_py $DATA_PATH/tmp_c1_utm_to_xy.csv
    outsheet using `file_to_py', comma replace
    restore
    * Convert via python
    cap rm $tmp_new_xy
    !python py4stata/xy_to_utm.py `file_to_py' $tmp_new_xy --inverse
end

prog def set_metsite
    /* Finds nearest metsite via python and merges it in. */

    local firm_utm_csv $DATA_PATH/tmp_c1_firmutm.csv
    local metsites_dta $DATA_PATH/metsites.dta
    local metsite_matches_dta $DATA_PATH/metsite_matches.dta

    preserve
    * Prep firm's utm list
    keep utm_east utm_north
    duplicates drop
    outsheet using `firm_utm_csv', replace comma

    * Prep metsite data
    insheet using $DATA_PATH/src/scaqmd_metsites.csv, clear

    ren code metsite_code
    ren z metsite_z
    gen utm_east = utm_ekm*1000
    gen utm_north = utm_nkm*1000
    gen metsite_year = "09"
    replace metsite_year = "07" if metsite_code=="rivr"

    keep metsite_code utm_east utm_north metsite_year metsite_z

    save `metsites_dta', replace
    restore

    cap rm `metsite_matches_dta'  // Jury-rigged exit code check on util.Neighbors
    !python -m py4stata.nearestneighbor `firm_utm_csv' `metsites_dta' ///
                                        `metsite_matches_dta' metsite_code

    merge m:1 utm_east utm_north using `metsite_matches_dta', assert(3) nogen

    merge m:1 metsite_code using `metsites_dta', keep(1 3)
    assert _merge==3
    drop _merge

    compress
end

prog def set_pop1990
    preserve
    use $county_pop_dta, clear
    gen county = regexs(1) if regexm(name,"CA (.*) County")
    drop if county==""
    keep county pop1990
    tempfile countypop
    save `countypop'

    // XXX This is a file with no source
    use $carb_county_code_xwalk_dta, clear
    ren county co
    ren countyname county
    tempfile countyxwalk
    save `countyxwalk'

    restore

    merge m:1 co using `countyxwalk', keep(1 3)
    // XXX This is a temporary fix
    replace county = "Los Angeles" if _merge == 1
    assert county != ""
    drop _merge

    merge m:1 county using `countypop', keep(1 3)
    assert _merge == 3
    drop _merge
end

prog def make_firmid
    /* Create new numeric firm id's
       Below 100 are electric firms, above 100, non-elec's */
    egen firm_id = group(ufacid) if electric == 1
    egen temp = group(ufacid) if electric == 0
    replace temp = temp + 100 if electric == 0
    replace firm_id = temp if electric == 0
    drop temp
    // Double check one-to-one mapping
    preserve
    bys ufacid firm_id: keep if _n == 1
    qui tab ufacid
    assert `=_N' == r(r)
    restore
    // Make an id that lumps all non-electrics into one "firm"
    /* This used to be an easier way to lump firms together during
       batch runs of aermod, but it's probably obsolete */
    gen Tfirm_id = firm_id * electric
end

} // End quiet


* Main
*------
main
