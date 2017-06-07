clear all
set more off

use ../data/ZIP4.dta

merge 1:1 zip4 using ../data/zips_aermod_pre.dta
 
twoway (scatter lat lon if _merge == 3 & aermod_pre > 0, msize(vtiny)) ///
       (scatter lat lon if _merge == 3 & aermod_pre == 0, msize(vtiny)) ///
       (scatter lat lon if _merge == 1, msize(vtiny))
