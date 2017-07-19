/*
XXX !!! Data documentation is WRONG for files before 1996. From 1996 on, all data
and docs match, but some variables are different/missing before 1996 but the
pre-96 docs conform to post-96 data.

Docs for 1996 are also wrong, e.g., 'payer' instead of 'pay-cat'.

*/
set more off
quiet {
set trace off
clear all

run clean/io

* I/0
*--------
* In
global PDD_ROOT /homes/data/oshpd/oshpdpdd

* Out
global patzip_sample_quarter_dta $patzip_sample_path

// TODO: Figure this crap out
global RAW_PANEL $patzip_sample_quarter_dta
global CLEANER_PANEL $patzip_sample_quarter_dta


*************************
**            Parameters
*************************

global Year_0 = 1996
global Year_T = 2005


/* Notes on variables
    adm_src (admission source, from 1995 on):
        Each digit codes something about how the patient got to the hospital.
        1st, where admitted from (home, nursing home, newborn, another dept)
        2nd, which hospital that was
        3rd, whether they came through 'your' ER.
        Using 1999 state-wide data:
        132 - From home, not through ER (41%)
        131 - From home, ER (34%)
        712 - Newborn (13%)
        522 - From another hospital's acute care (3%)
        512 - From this hospital's acute care (3%)

    adm_type (from 1995 on):
        When was the admission arranged? (1) Schedule >24 hrs, (2) unscheduled <24 hrs,
        (3) infant, (4) unknown. All others missing.

    disp (disposition, from 1995 on):
        Where did the patient go upon discharge?
        01 - Routine (home)
           - Within this hospital
           - To another hospital
           - Nursing or care home
           - Prison
           - Against advice
        11 - Died

    pay_cat (who's paying):
        1 - Medicare
        2 - Medi-cal (medicaid?)
        3 - private coverage
        4 - Worker's comp
        5 - County welfare (e.g. CHIP)
        6 - Other gov't
        7 - Other indigent (e.g., charity care)
        8 - Self pay
        9 - Other pay (including no payment required, research)
        0 - Not reported or error

    pay_type (type of plan)
        (1) HMO, (2) PPO, (3) Fee-for-service, all others; (0) Weird stuff
    pay_plan (code for name of specific insurance plan)
*/
                        
**************************

/* Legacy, so I don't have to look up these differences later */
prog def _clean_for_zipXgroup_match
    * Correct groups to match Census mappings
    replace agecat5 = 2 if agecat5==1
end

* Main
prog def main
    /* Create patient panel-ish data */
    clear
    save $DATA_PATH/temp, replace emptyok
    foreach year of numlist $Year_0/$Year_T {
        _read_data `year'
        _restrict_yearly_data
        gen_diagnosis_flags
        disposition_flags

        compress
        append using $DATA_PATH/temp
        save $DATA_PATH/temp, replace
    }

    _overall_cleaning

    save $RAW_PANEL, replace
end

prog def _read_data
    args year
    di as err "PROCESSING `year'"
    local last2digits = substr("`year'", -2, 2)
    use $PDD_ROOT/`year'/public`last2digits', clear
end

prog def _restrict_yearly_data
    global keepVarsFromSrc ///
        oshpd_id                                         /// Hosp id
        adm_qtr adm_yr /// Time
        adm_src                                          /// Admission source
        adm_typ                                          /// Admit scheduled
        disp                                             /// Disposition @ disg
        age_yrs agecat* sex ethncty race patzip patcnty  /// Patient demogs
        los                                              /// Length of stay 
        typ_care                                         /// Treatment info
        mdc drg diag_p odiag*                            /// Diag info
        proc_p                                           /// Procedure code
        charge pay*                                      //  Payment

    keep $keepVarsFromSrc
    keep if inlist(patcnty, "19", "30", "33", "36") //LA, Orange, Riverside, San Bernard
    destring patzip, replace i("*XYZ")
    ren patzip zip
    ren patcnty cnty
    ren adm_qtr quarter
    ren adm_yr year
    drop if zip==.
    cap rename payer pay_cat

    foreach var in year quarter cnty adm_src pay_cat pay_type ///
                    pay_plan mdc ethncty race agecat5 agecat20 charge drg {
        cap destring `var', replace i("*OXPD")
    }

    keep if inrange(year, $Year_0, $Year_T)
end
prog def _overall_cleaning
    * Charges
    replace charge = . if charge==0 // Not reported, e.g., capitated pay
    replace charge = 0 if charge==1 // Actually $0
    //replace charge = . if charge==9999999 // Charges over 7 digits
    * Sex
    gen byte female = sex=="2" if inlist(sex,"1","2")
    drop sex
    * Make white hispanics own group (like in (old)clean2 file)
    replace race = 0 if race==1 & ethnc==1
    drop ethn
end

prog def gen_diagnosis_flags
    quiet {
    /* Birth vars */

    * Low birth weight
    _loop_diag_codes, diag(dl1_lowbwt) ///
        icd9_list(`" "76400", "76401", "76402", "76403", "76404", "76405", "76406", "76407", "76408""') 
    _loop_diag_codes, diag(dl1_lowbwt) append ///
        icd9_list(`" "76410", "76411", "76412", "76413", "76414", "76415", "76416", "76417", "76418""') 
    _loop_diag_codes, diag(dl1_lowbwt) append ///
        icd9_list(`" "76420", "76421", "76422", "76423", "76424", "76425", "76426", "76427", "76428""') 
    _loop_diag_codes, diag(dl1_lowbwt) append ///
        icd9_list(`" "76490", "76491", "76492", "76493", "76494", "76495", "76496", "76497", "76498""') 
    _loop_diag_codes, diag(dl1_lowbwt) append ///
        icd9_list(`" "76500", "76501", "76502", "76503", "76504", "76505", "76506", "76507", "76508""') 
    _loop_diag_codes, diag(dl1_lowbwt) append ///
        icd9_list(`" "76510", "76511", "76512", "76513", "76514", "76515", "76516", "76517", "76518""') 
    /*
    foreach cat in 41 42 49 50 51 {
        _loop_diag_codes, diag(dl1_lowbwt) append ///
            icd9_list(`" "76`i'0", "76`i'1", "76`i'2", "76`i'3", "76`i'4", "76`i'5", "76`i'6", "76`i'7", "76`i'8""') 
    }
    */
   
    // Get 4 digit diagnosis codes
    foreach codevar of varlist diag_p odiag* {
        replace `codevar' = substr(`codevar',1,4)
    }

    * Prematurity
    _loop_diag_codes, diag(dl0_prem_extreme) icd9_list(`""7650""')
    _loop_diag_codes, diag(dl0_prem_1) icd9_list(`""7651""')
    gen byte dl1_prembirth = inlist(1, dl0_prem_extreme, dl0_prem_1)

    gen dl1_infdie_t = drg == 385
    replace dl1_prembirth = inlist(drg, 386, 387, 388)
    gen dl1_infprob = inlist(drg, 389, 390)  // With problems
    drop dl0_prem_1

    // Get 3 digit diagnosis codes (AFTER birth stuff above)
    foreach codevar of varlist diag_p odiag* {
        replace `codevar' = substr(`codevar',1,3)
    }

    * Flag all births
    _loop_diag_codes, diag(dl2_birth) ///
        icd9_list(`""V29", "V30", "V31", "V32", "V33", "V34", "V35""')
    _loop_diag_codes, diag(dl2_birth) append icd9_list(`""V36", "V37", "V38", "V39""')
    replace dl2_birth = 1 if inrange(drg, 385, 391)
    replace dl2_birth = 1 if inlist(1, dl1_prembirth, dl1_lowbwt)

    /* Respiratory */

    * Specific conditions
    _loop_diag_codes, diag(dl0_asthma) icd9_list(`" "493" "')
    _loop_diag_codes, diag(dl0_COPD) icd9_list(`"  "493", "494" "') 

    _loop_diag_codes, diag(dl0_pneumonia) ///
        icd9_list(`" "480", "481", "482", "483", "484", "485", "486" "')
    _loop_diag_codes, diag(dl0_influenza) icd9_list(`" "487" "')

    * All acute respiratory
    _loop_diag_codes, diag(dl1_acuteresp) ///
                    icd9_list(`" "460", "461", "462", "463", "464", "465", "466" "')
    _loop_diag_codes, diag(dl1_acuteresp) append ///
        icd9_list(`" "470", "471", "472", "473", "474", "475", "476", "477", "478" "')
    _loop_diag_codes, diag(dl1_acuteresp) append ///
        icd9_list(`" "500", "501", "502", "503", "504", "505", "506", "507", "508" "')
    _loop_diag_codes, diag(dl1_acuteresp) append icd9_list(`" "514" "')
    replace dl1_acuteresp = 1 if dl0_COPD == 1

    * Other respiratory (only to include in level-2 'respiratory' variable)
    _loop_diag_codes, diag(dl1_othresp) icd9_list(`" "516", "517", "518", "519" "')
    _loop_diag_codes, diag(dl1_othresp) append ///
        icd9_list(`" "490", "491", "492", "495", "496" "')
    _loop_diag_codes, diag(dl1_othresp) append ///
        icd9_list(`" "510", "511", "512", "513", "515" "')

    gen byte dl2_respiratory = inlist(1,                                        ///
                                      dl0_COPD, dl0_pneumonia, dl0_influenza,   ///
                                      dl1_acuteresp, dl1_othresp)
    
    /* Heart disease */
    _loop_diag_codes, diag(dl1_heart) icd9_list(`"410, 429"') range

    /* Placebos */
    _loop_diag_codes, diag(dl1_stroke) icd9_list(`"430, 438"') range
    _loop_diag_codes, diag(dl1_fracture) icd9_list(`"800, 829"') range
    _loop_diag_codes, diag(dl1_appendicitis) icd9_list(`"540, 543"') range

    * Clean up diagnoses
    drop diag_p* odiag*

    * Restrict sample
    keep if dl2_birth == 1 | ///
            dl2_respiratory == 1 | ///
            dl1_heart == 1 | ///
            dl1_stroke == 1 | ///
            dl1_fracture == 1 | ///
            dl1_appendicitis == 1
    } // End quiet
end
prog def _loop_diag_codes
    syntax [anything], diag(string) icd9_list(string) [append] [range]
    if "`append'" == "" {
        gen byte `diag' = 0
    }
    foreach codevar of varlist diag_p odiag* {
        if "`range'" == "" {
            replace `diag' = 1 if inlist(`codevar', `icd9_list')
        }
        else {
            replace `diag' = 1 if inrange(real(`codevar'), `icd9_list')
        }
    }
end

prog def disposition_flags
    gen byte disp_died = disp=="11"
    gen byte disp_transfer = inlist(disp, "02", "03", "04", "05", "06")
    drop disp
end

/* Legacy, when I only did the main diagnosis code (I think) */
prog def old_std_panel_cleaning
    cap use $RAW_PANEL

    keep year quarter age* female race zip cnty ///
      mdc drg charge los diag_p odiag1 disp

    * Single out some diagnoses (DRG)
    /*
        ** Respiratory (MDC 4)
        *088 - Chronic obstructive pulmonary disease
        *089,090,091 - Simple pneumonia, (age >17) X (w/ cc)
        079,080,081 - Resp infections & inflammations (age > 17) X (w/cc)
        096,097,098 - Bronchitis & asthma
        082 - Resp neoplasms
        475 - pulmonary system diagnosis w.  

        075,076,077 - Major (and other) respiratory OR procedures
        482,483 - Tracheostomy

        ** Nervous system (MDC 1)
        *014 - Specific cerebrovascular disorders except tia

        ** Circulatory (MDC 5)
        *127 - Heart failure & shock
        *143 - Chest pain
        121,122,123* - Circ disorders w/ AMI (123 died)
        124,125 - Circ disorders w/o AMI
        130,131 - Peripheral circulatory
        132,133 - atherosclerosis

        ** Digestive (MDC 6)
        174,175 - GI hemorrhage
        *182-184 - Esophagitis, msc
        148,149 - Major bowel procedures

        ** Muscuoloskelital (MDC 8)
        209* - Major joint & limb reattachment procedures
        243 - Medical back problems

        ** (MDC 10)
        *296-298 - Nutritional & misc metabolic

        ** (MDC 11)
        *320-322 - Kidney and urinary tract infections

        ** (MDC 13)
        *357-359 - Uterine and adnexa procesures 

        ** (MDC 18)
        *416-417 - Septicemia

        ** Mental disease (MDC 19)
        *430 - Psychoses

        ** Neonatal (MDC 15)
        *391 - Normal newborn
        *389 - Full-term w/ major problems
        *390 - Neonate with other sig problems
        387 - Premature w/ major problems
        388 - Premature w/o major problems
        
        386 - Extreme prematurity or resp distress syndrome
        
        385 - Neonate, died or transferred 
    */

    /*
    keep if inlist(drg, 88, 89, 90, 91, 79, 80, 81, 96, 97, 98, 14, 127, 143) | ///
            inlist(drg, 121, 122, 123, 182, 183, 184, 296, 297, 298, 320) | ///
            inlist(drg, 321, 322, 357, 358, 359, 416, 417, 430, 389, 390) | ///
            inlist(drg, 391, 387, 388, 386, 385)
    */

    * Re-categorize diagnoses
    gen diag3 = substr(diap_p,1,3)
    
    gen dlabel = ""
    /* Acute respiratory infections */
    replace dlabel = "pharyngitis" if diag3 == "462"
    replace dlabel = "tonsillitis" if diag3 == "463"
    replace dlabel = "laryngitisAcute" if diag3 == "464" // Mostly croup 
    replace dlabel = "uppRespInf" if diag3 == "465"
    replace dlabel = "bronchitisAcute" if diag3 == "466" // Chest cold
    /* Other diseases of upper respiratory trace */
    replace dlabel = "deviatedseptum" if diag3 == "470"
    replace dlabel = "nasalpolyp" if diag3 == "471"
    replace dlabel = "pharyngitisChron" if diag3 == "472"
    replace dlabel = "sinusitisChron" if diag3 == "473"
    replace dlabel = "tonsilChron" if diag3 == "474"
    replace dlabel = "peritonsilabscess" if diag3 == "475"
    replace dlabel = "laryngitisChron" if diag3 == "476"
    replace dlabel = "allergicrhinitis" if diag3 == "477"
    replace dlabel = "othUppResp" if diag3 == "478"     // Prob drop
    /* Pneumonia and influenza */
    replace dlabel = "pneumonia" if inrange(real(diag3), 480, 486)
    replace dlabel = "influenza" if inrange(real(diag3), 487, 488)
    /* COPD */
    replace dlabel = "bronchitisOth" if diag3 == "490"
    replace dlabel = "bronchitisChron" if diag3 == "491"
    replace dlabel = "emphysema" if diag3 == "492"
    replace dlabel = "asthma" if diag3 == "493"
    replace dlabel = "bronchiectasis" if diag3 == "494"
    replace dlabel = "hypersPneum" if diag3 == "495"
    replace dlabel = "othCOPD" if diag3 == "496"
    /* Pneumoconioses and oth lung diesease due to external agents (500-508) 
        (Black Lung) */
    replace dlabel = "blacklung" if inrange(real(diag3), 500, 508)
    /* Other diseases of respiratory system (510-519) */
    replace dlabel = "empyema" if diag3 == "510"    // Only lungs??
    replace dlabel = "pleurisy" if diag3 == "511"
    replace dlabel = "pneumothorax" if diag3 == "512" // Collapsed lung
    replace dlabel = "othothResp" if inrange(real(diag3), 513, 517) /// // Prob drop
                                     | (diag3 == "518" ///
                                        & substr(diag3,1,4) == "5188" ) ///
                                     | diag3 == "519"
    replace dlabel = "respfailure" if substr(diag3,1,4) == "5188"
    /**** Pregnancy, childbirth ****/
    /* Ectopic (630-633) */
    /* Other pregnancy with abortive outcome (634-639) */
    /* Complications (640-649) */
    /* Normal Delivery (650-659) */
    replace dlabel = "normaldelivery" if diag3 == "650"
    replace dlabel = "multiplegestation" if diag3 == "651"
    replace dlabel = "malposition" if diag3 == "652"
    replace dlabel = "disproportion" if diag3 == "653"


    /* Old codes */
    replace dlabel = "copd" if drg==88
    replace dlabel = "pneum" if inrange(drg,89,91)
    replace dlabel = "respInf" if inrange(drg,79,81)

    replace dlabel = "newborn" if drg==391
    replace dlabel = "newborn_prob" if inlist(drg,389,390)
    replace dlabel = "prem" if drg==388
    replace dlabel = "prem_prob" if drg==387
    replace dlabel = "newborn_diedoT" if drg==385

    replace dlabel = "mental" if mdc==19
    replace dlabel = "infection" if mdc==18
    replace dlabel = "uterine" if mdc==13
    replace dlabel = "kidney" if mdc==11
    replace dlabel = "nutrition" if mdc==10
    replace dlabel = "mskel" if mdc==8
    replace dlabel = "nervous" if mdc ==1
    replace dlabel = "circ" if mdc==5
    replace dlabel = "digest" if mdc==6

    drop if dlabel==""
    */
    save $CLEANER_PANEL, replace
end

} // End quiet

*------------
main

