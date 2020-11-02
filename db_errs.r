

######################################
#
get_db_error_report_by_case_error_Residency_canadian<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "" & nchar(ResidenceCountry) > 0) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is unspecified, yet ResidenceCountry is not blank.")%>% collect() %>% 
    mutate(typ = "Residency" )
}
get_db_error_report_by_case_error_Residency_canadian_but_not<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "Canadian resident" & !(str_to_lower( ResidenceCountry )==str_to_lower("CANADA"))) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is Canadian, yet ResidenceCountry is not.")%>% collect() %>% 
    mutate(typ = "Residency" )
}
get_db_error_report_by_case_error_Age_Age_group<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & (tolower(AgeGroup10) == tolower("Unknown") | tolower(AgeGroup20) == tolower("Unknown") )) %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Agegroup is listed as unkown.")%>% collect() %>% 
    mutate(typ = "Age" )
}
get_db_error_report_by_case_error_Age_Age_units<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & AgeUnit == "") %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Age units are not.") %>% collect() %>% 
    mutate(typ = "Age" )
}


get_db_error_report_by_case_Sym<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(tolower(Asymptomatic) == "yes" ) %>% 
    select(PHACID, Asymptomatic, matches("^Sym")) %>% 
    collect() %>% 
    pivot_longer(matches("^Sym")) %>% 
    filter(! is.na(value)) %>% 
    filter(value != "" & value != ".", tolower(value) != tolower("No") & tolower(value) != tolower("Unknown")) %>% 
    mutate(err = paste0("'",name,"' is '", value, "'") ) %>% 
    group_by(PHACID) %>% 
    summarise(err = paste0(err, collapse = " and") ) %>% 
    mutate(err = paste0("Asymptomatic is 'yes' but",  err, collapse = " and ") ) %>% 
    select(PHACID, err) %>% 
    mutate(typ = "symptoms" )
}

get_db_error_report_by_case_Hosp<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    select(PHACID, Hosp, HospStartDate, HospEndDate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(Hosp) == "yes") %>% 
    filter(! is.na(HospStartDate) | ! is.na(HospEndDate)) %>% 
    select(PHACID) %>% 
    mutate(err = paste0("A hospital date is listed but Hosp is not Yes") ) %>% 
    select(PHACID, err) %>% collect() %>% 
    mutate(typ = "Hosp" )
}

get_db_error_report_by_case_ICU<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    select(PHACID, ICU, ICUStartDate, ICUEndDate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(ICU) == "yes") %>% 
    filter(! is.na(ICUStartDate) | ! is.na(ICUEndDate)) %>% 
    select(PHACID) %>% 
    mutate(err = paste0("A ICU date is listed but ICU is not Yes") ) %>% 
    select(PHACID, err) %>% collect() %>%
    mutate(typ = "ICU" )
}


get_db_error_report_by_case_Isolation<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    select(PHACID, Isolation, IsolationStartDate, IsolationEndDate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(Isolation) == "yes") %>% 
    filter(! is.na(IsolationStartDate) | ! is.na(IsolationEndDate) ) %>% 
    select(PHACID) %>% 
    mutate(err = paste0("A Isolation date is listed but Isolation is not Yes") ) %>% 
    select(PHACID, err) %>% collect() %>% 
    mutate(typ = "Isolation" )
}


get_db_error_report_by_case_vent<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!tolower(MechanicalVent) == "yes") %>% 
    filter(! is.na(VentStartDate) | ! is.na(VentEndDate) ) %>% 
    select(PHACID) %>% 
    collect() %>% 
    mutate(err = paste0("A vent date is listed but MechanicalVent is not Yes") ) %>% 
    select(PHACID, err) %>% 
    mutate(typ = "vent" )
}
get_db_error_report_by_case_Reovered<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    select(PHACID, Disposition, RecoveryDate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(Disposition) == "recovered") %>% 
    filter(! is.na(RecoveryDate) ) %>% 
    select(PHACID, RecoveryDate, Disposition) %>% 
    mutate(err = paste0("Recovery date of '",RecoveryDate,"' is listed but Disposition is '", Disposition, "'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Recovered" )
}
get_db_error_report_by_case_Death<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    select(PHACID, Disposition, DeathDate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(Disposition) == "deceased") %>% 
    filter(! is.na(DeathDate) ) %>% 
    select(PHACID, DeathDate, Disposition) %>% 
    mutate(err = paste0("Death date of '",DeathDate,"' is listed but Disposition is '", Disposition, "'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Death" )
}



get_db_error_report_by_case_travel<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  tmp_df <- 
    a_tbl %>% 
    filter((!tolower(Travel) == "yes")) %>% 
    select(PHACID, matches("^travel")) %>% collect() 
  
  tmp_df %>% 
    mutate_all(as.character) %>% 
    pivot_longer(matches("^travel")) %>% 
    filter(name != "Travel") %>% 
    filter(!is.na(value)) %>% 
    filter(value != "") %>% 
    mutate(err = paste0("'",name,"' is '", value, "'") ) %>% 
    select(PHACID, err)  %>%
    group_by(PHACID) %>%
    summarise(err = paste0(err, collapse = " and ")) %>% 
    mutate(err = paste0("travel is not yes but ", err) ) %>% 
    mutate(typ = "Travel" )
}

get_db_error_report_by_case_close<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  tmp_df <- 
    a_tbl %>% 
    filter((!tolower(CloseContactCase) == "yes")) %>% 
    select(PHACID, matches("^CloseContactCase")) %>% collect() 
  
  tmp_df %>% 
    mutate_all(as.character) %>% 
    pivot_longer(matches("^CloseContactCase")) %>% 
    filter(name != "CloseContactCase") %>% 
    filter(!is.na(value) &  value != "") %>% 
    mutate(err = paste0("'",name,"' is '", value, "'") ) %>% 
    select(PHACID, err)  %>%
    group_by(PHACID) %>%
    summarise(err = paste0(err, collapse = " and ")) %>% 
    mutate(err = paste0("CloseContactCase is not yes but ", err) ) %>% 
    mutate(typ = "Close Contact" )
}

#get_db_error_report_by_case_date_diff <- function()




get_db_error_report_by_case_Dead_and_alive<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  #' Returns error report related to cases both dead and alive
  #' 
  a_tbl %>% 
    filter( (! is.na(DeathDate)) &  (! is.na(RecoveryDate)) ) %>% 
    select(PHACID, DeathDate, RecoveryDate) %>% 
    mutate(err = paste0("Both Death date '",DeathDate,"' and recovery date '",RecoveryDate,"' listed.") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "DeadorAlive" )
}

get_db_error_report_by_case_dead_before_onset<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter( (! is.na(DeathDate)) &  (! is.na(OnsetDate)) ) %>% 
    filter( DeathDate < OnsetDate) %>% 
    select(PHACID, DeathDate, OnsetDate) %>% 
    mutate(err = paste0("Dead before Onset '",DeathDate,"' < '",OnsetDate,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Early Dead" )
}


get_db_error_report_by_case_recovered_before_onset<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter( (! is.na(RecoveryDate)) &  (! is.na(OnsetDate)) ) %>% 
    filter( RecoveryDate < OnsetDate) %>% 
    select(PHACID, RecoveryDate, OnsetDate) %>% 
    mutate(err = paste0("Recovered before Onset '",RecoveryDate,"' < '",OnsetDate,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Early Recovered" )
}


get_db_error_report_by_case_teasting_CloseContact_disagree<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  #' Generates Error List on DB
  a_tbl %>% 
    filter( TestingReason == "Contact of a case" &  (!CloseContactCase ==  "Yes")) %>% 
    select(PHACID, TestingReason, CloseContactCase) %>% 
    mutate(err = paste0("TestingReason '",TestingReason,"' disagrees with  CloseContactCase '",CloseContactCase,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Contact Testing Reason" )
}

get_db_error_report_by_case_COVIDDeath_Disposition_disagree<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  #' Generates Error List on DB
  a_tbl %>% 
    select(PHACID, COVIDDeath, Disposition) %>% 
    mutate(COVIDDeath = clean_str(COVIDDeath), 
           Disposition = clean_str(Disposition)) %>% #count(COVIDDeath)
    filter(COVIDDeath == YES_STR & Disposition != "Deceased") %>% 
    mutate(err = paste0("COVIDDeath '",COVIDDeath,"' disagrees with  Disposition '",Disposition,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Covid Death but not Deceased" )
}


get_db_error_report_by_case_PHAC_report_before_episode<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  a_tbl %>% 
    mutate(., EpisodeDate = make_EpisodeDate(.)) %>% 
    mutate(., EpisodeType = make_EpisodeType(.)) %>% 
    filter( (! is.na(EpisodeDate)) &  (! is.na(PHACReportedDate)) ) %>% 
    filter( PHACReportedDate < EpisodeDate) %>% 
    mutate(delta_days = PHACReportedDate - EpisodeDate) %>% 
    select(PHACID, PHACReportedDate, EpisodeDate, EpisodeType, delta_days, Classification) %>% 
    mutate(err = paste0("PHACReported before EpisodeDate '",PHACReportedDate,"' < '",EpisodeDate,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Early PHACReported" )
}


get_db_error_report_by_case_future_dates<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  
  tmp <- 
    a_tbl %>% 
    select(matches("Date")) %>%
    select_if(is.Date) 
  
  col_nms <- colnames(tmp)
  #select(PHACReportedDate, ReportedDate) %>% 
  #mutate(a = PHACReportedDate > Sys.Date()) %>% select(a) %>% table()
  tmp %>%   mutate_all(function(x){x > Sys.Date()}) %>%
    bind_cols(a_tbl %>% select(PHACID), .) %>% 
    pivot_longer(cols = matches("Date"), values_drop_n = T) %>% 
    filter(value == T) %>% 
    mutate(err = paste0(name , " is in the future? spooky!") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Future Date" )
  
}
get_db_error_report_by_case_still_sick<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  a_tbl %>% 
    select(Classification, Disposition, PHACID, OnsetDate, ReportedDate) %>% 
    filter(Classification %in% c("Confirmed", "Probable")) %>%
    filter(! Disposition %in% c("Deceased", "Recovered")) %>%
    filter(OnsetDate <= Sys.Date() - 30*5) %>% 
    mutate(err = paste0("Onset Date ",OnsetDate," is ",Sys.Date()- OnsetDate," days ago....Thats a lot, maybe verify this with the PT?") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Sick Long time" )
}

get_db_error_report_by_case_still_sick_reported<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  a_tbl %>% 
    select(Classification, Disposition, PHACID, OnsetDate, ReportedDate) %>% 
    filter(Classification %in% c("Confirmed", "Probable")) %>%
    filter(! Disposition %in% c("Deceased", "Recovered")) %>%
    filter(ReportedDate <= Sys.Date() - 30*5) %>% 
    mutate(err = paste0("ReportedDate ",ReportedDate," is ",Sys.Date()- ReportedDate," days ago....Thats a lot, maybe verify this with the PT?") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Reported Long time ago." )
}



###########################################
#
get_db_error_report_by_case <- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con)){
  
  errs <- 
    bind_rows(
      get_db_error_report_by_case_error_Residency_canadian(a_tbl = a_tbl),
      get_db_error_report_by_case_error_Residency_canadian_but_not(a_tbl = a_tbl),
      get_db_error_report_by_case_error_Age_Age_group(a_tbl = a_tbl),
      get_db_error_report_by_case_error_Age_Age_units(a_tbl = a_tbl),
      get_db_error_report_by_case_Sym(a_tbl = a_tbl), 
      get_db_error_report_by_case_Hosp(a_tbl = a_tbl),
      get_db_error_report_by_case_ICU(a_tbl = a_tbl), 
      get_db_error_report_by_case_Isolation(a_tbl = a_tbl), 
      get_db_error_report_by_case_vent(a_tbl = a_tbl),
      get_db_error_report_by_case_Reovered(a_tbl = a_tbl),
      get_db_error_report_by_case_Death(a_tbl = a_tbl),
      get_db_error_report_by_case_Dead_and_alive(a_tbl = a_tbl),
      get_db_error_report_by_case_dead_before_onset(a_tbl = a_tbl),
      get_db_error_report_by_case_recovered_before_onset(a_tbl = a_tbl),
      get_db_error_report_by_case_travel(a_tbl = a_tbl),
      get_db_error_report_by_case_teasting_CloseContact_disagree(a_tbl = a_tbl),
      get_db_error_report_by_case_COVIDDeath_Disposition_disagree(a_tbl = a_tbl),
      get_db_error_report_by_case_PHAC_report_before_episode(a_tbl = a_tbl),
      get_db_error_report_by_case_future_dates(a_tbl = a_tbl),
      get_db_error_report_by_case_still_sick(a_tbl = a_tbl),
      get_db_error_report_by_case_still_sick_reported(a_tbl = a_tbl),
      
    ) %>% arrange(PHACID)
  
  ids <- unique(errs$PHACID)
  
  ret_val <-   
    a_tbl %>% 
    filter(PHACID %in% ids) %>% 
    select(PHACID, PT,    PTCaseID, Classification, PHACReportedDate ) %>% 
    left_join(errs, by = "PHACID") %>% 
    arrange(desc(PHACReportedDate), PHACID, typ) %>% 
    select(PHACID, PT, PTCaseID, typ, Classification,PHACReportedDate , err)
  
  return(ret_val)
}

