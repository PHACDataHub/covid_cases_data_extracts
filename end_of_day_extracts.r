


rm(list=ls())
gc()

#options(java.parameters = "-Xmx8000m")

library(tidyverse)
#library(rio)
library(janitor)
library(stringr)
library(keyring)
library(DBI)
library(odbc)
library(dbplyr)
library(scales)
library(svglite)
library(docstring)
library(writexl)
library(readxl)
#library(xlsx)
library(lubridate)
#library(RDCOMClient)

source("make_graphs.r")


#DIR_OF_DB = file.path("~", "..", "Desktop") 
#NAME_DB = "COVID-19_v2.accdb"
DIR_OF_DB = "~"
DIR_OF_DB_2_BACK_UP = file.path("~", "..", "Desktop")
NAME_DB = "Case.xlsx"
NAME_DB_2_BACK_UP = "COVID-19_v2.accdb"

#DIR_OF_INPUT = "//Ncr-a_irbv2s/irbv2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/DATA AND ANALYSIS/DATABASE/R"
#DIR_OF_OUTPUT = "//Ncr-a_irbv2s/irbv2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/DATA AND ANALYSIS/DATABASE/R/output"
#DIR_OF_OUTPUT = "~"
DIR_OF_INPUT = getwd()
END_OF_DAY_REPORT_INPUT = "end_of_day_reports_input.xlsx"

YES_STR = "YES"
NO_STR = "NO"
UNKNOWN_STR = "UNKNOWN"
#BLANK_VALUE_STR = "BLANK"
BLANK_VALUE_STR = ""
NOT_STATED_STR = "Not stated"
#DEF_TYPE_DB = "MS_Access"
DEF_TYPE_DB = "xlsx"



####################################
#
get_covid_cases_db_con <- function(
  db_type = DEF_TYPE_DB, 
  db_dir = DIR_OF_DB,
  db_name = NAME_DB,
  db_full_nm = path.expand(file.path(db_dir, db_name)),
  db_pwd = keyring::key_get("COVID-19")
)
{
  #' Get a connection fo the data base
  #' 
  #' @param db_type type of DB SQLite MSAccess Postgres etc....
  #' @param db_dir for MS Access ans SQLite
  #' @param db_name for MS Access ans SQLite
  #' @param db_full_nm for MS Access ans SQLite
  #' @param db_pwd password for the database
  #' 
  con <- NULL
  if (db_type == "MS_Access"){
    print("getting Con")
    con <- dbConnect(odbc::odbc(),
                     .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", 
                                                 db_full_nm, 
                                                 ";PWD=", db_pwd))
  }else if (db_type == "xlsx"){
    #TODO : ODBC is having issues on Jenne compuiter so we pretend its a DB connection
    return(db_full_nm)
    # con <- dbConnect(odbc::odbc(),
    #                  .connection_string = paste0("Driver={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=",db_full_nm,";ReadOnly=0;"))
    
  }
  else
  {
    warning(paste0("Unknown db_type='", db_type, "'. Returning NULL from get_covid_cases_db_con"))
  }
  return(con)  
}

##########################
# CHECK ... DO WE HAVE A CONNECTION
get_covid_cases_db_con()


########################################
#
non_missing_cols <- function(df, cols_order, 
                             vector_init_mode = "double",
                             vector_transform = as_date,
                             bad_value = 0)
{
  #' For each row in the datafram return a value from a hieraghy of columns 
  #' 
  #' @param df a data frame
  #' @param cols_order vector of strings with column names in them
  #' @param vector_init_mode type of vector to be returning
  #' @param vector_transform function to call at the end to transform the return vector into
  #' @param bad_value represents a bad value in the DB also NA does this
  #' 
  #' 
  n_row <- df %>% tally() %>% pull()
  ret_val <- vector(mode = vector_init_mode, 
                    length = n_row)
  
  # ret_val<- 
  # lapply(cols_order, function(col){
  #   ifelse( ret_val != bad_value & (!is.na(ret_val)), ret_val, df %>% pull(col))
  # }
  #)
  for (col in cols_order){
    ret_val <- ifelse( ret_val != bad_value & (!is.na(ret_val)), ret_val, df %>% pull(col))
  }
  
  ret_val <- vector_transform(ret_val)
  return(ret_val)
}


########################################
#
non_missing_cols_str <- function(df, cols_order, strs_for_col,
                                 vector_init_mode = "character",
                                 vector_transform = as.character,
                                 bad_value = ""){
  #' For each row in the datafram return a value from a hieraghy of columns, but returns the string passed in instead
  #' 
  #' @param df a data frame
  #' @param cols_order vector of strings with column names in them
  #' @param strs_for_col vector of strings must be of same lenght as cols_order
  #' @param vector_init_mode type of vector to be returning
  #' @param vector_transform function to call at the end to transform the return vector into
  #' @param bad_value represents a bad value in the DB also NA does this
  #' 
  #' 
  n_row <- df %>% tally() %>% pull()
  ret_val <- vector(mode = vector_init_mode, 
                    length = n_row)
  
  for (icol in 1:length(cols_order)){
    col_nm = cols_order[[icol]]
    col_val = strs_for_col[[icol]]
    curr_col <- df %>% pull(col_nm) %>% as.character()
    ret_val <- ifelse( ret_val != bad_value & (!is.na(ret_val)), ret_val, 
                       ifelse(curr_col != bad_value & (!is.na(curr_col)), 
                              col_val, 
                              bad_value
                       )   
                       
    )
  }
  
  ret_val <- vector_transform(ret_val)
  return(ret_val)
}


########################################
#
clean_yes_no <- function(x, 
                         other_replace = UNKNOWN_STR, 
                         bad_value_replace = BLANK_VALUE_STR,
                         bad_value = "", 
                         good_vals = c(YES_STR,NO_STR)){
  #' makes sure all values in a vector are one of a list of values
  #' 
  #' @param x a vector
  #' @param error_replace a vector
  #' @param x a vector
  clean_Categorical(x = x, 
                    other_replace = other_replace,
                    bad_value_replace = bad_value_replace,
                    bad_value = bad_value,
                    good_vals = good_vals)
  
}


clean_Categorical_top_n <-function(x, n = 1){
  #' makes sure all values in a vector are one of a list of values
  #' 
  #' @param x a vector
  #' @param n number of options to keep
  #' @param ... passed to clean_Categorical
  x <- clean_str(x)
  t <- x %>% table() %>% sort(decreasing = T)
  t <- t[1:n]
  
  clean_Categorical(x, good_vals = names(t)) #%>% table() %>% sort(decreasing = T)
}

########################################
#
clean_Categorical <- function(x, 
                         other_replace = UNKNOWN_STR, 
                         bad_value_replace = BLANK_VALUE_STR,
                         bad_value = "", 
                         good_vals){
  #' makes sure all values in a vector are one of a list of values
  #' 
  #' @param x a vector
  #' @param error_replace a vector
  #' @param x a vector
  x <- clean_str(x)
  good_vals <- clean_str(good_vals)
  bad_value <- clean_str(bad_value)
  bad_value_replace <- clean_str(bad_value_replace)
  other_replace <- clean_str(other_replace)
  
  x[is.na(x)] <- bad_value_replace
  x[x == bad_value] <- bad_value_replace
  
  #x <- toupper(x)
  
  x[!(x %in% c(good_vals, bad_value_replace))] <- other_replace
  
  return(x)
}



########################################
# 
clean_str <- function(x, 
                      pattern = "[[:punct:]]", 
                      replacement = " ", 
                      NA_replace = "", 
                      capitalize_function = str_to_title){
  #' By default this replactes all punctuation, and trims spaces so there is only one
  #' then sets title case
  #' 
  x %>% str_replace_all(pattern = pattern, replacement = replacement) %>%
    str_replace_all(pattern = "[ ]+", replacement = " ") %>%
    ifelse(is.na(.), NA_replace, .) %>% 
    #str_to_lower() %>% 
    capitalize_function() %>% 
    trimws() #%>% table()
}


clean_st_CONSTANTS <- function(){
  YES_STR <<- clean_str(YES_STR)
  NO_STR<<- clean_str(NO_STR)
  UNKNOWN_STR<<- clean_str(UNKNOWN_STR)
  BLANK_VALUE_STR<<- clean_str(BLANK_VALUE_STR) 
  NOT_STATED_STR<<- clean_str(NOT_STATED_STR)
}
clean_st_CONSTANTS()



  
  
########################################
#  
number_2_phacid <-function(x, pattern = " ", replacement = "-"){
  #' Convert an number string to a phac ID with the hypen
  #'
  #'
  #'
  x %>% str_replace_all(pattern = pattern, replacement = replacement) 
  #insert_str(x = x, insert_str = "-", afterPos = 2)
}



########################################
# 
insert_str <- function(x, insert_str = "-", afterPos){
  #' insert one string into another
  #' 
  paste0(substr(x, 1, afterPos), insert_str,  substr(x, afterPos + 1, stop = nchar(x))) 
}




get_db_tbl_IN_MEM_CACHE <- list()
get_db_tbl <- function(con = get_covid_cases_db_con(),
                       tlb_nm = "case"){
  #' gets a raw tabie from a DB connections
  #' this will have a memmory cache to avoid multiple feteches 
  #'
  if (is.null(get_db_tbl_IN_MEM_CACHE[[tlb_nm]])){
    print(paste0("Fetching and memmory caching '",tlb_nm,"'."))
    tic <- Sys.time()
    if (class(con) == "character"){
      #TODO : ODBC is having issues on Jenne compuiter so we pretend its a DB connection
      get_db_tbl_IN_MEM_CACHE[[tlb_nm]]  <<-  readxl::read_xlsx(path = con, sheet = stringr::str_to_title(tlb_nm) )
    }else{
      get_db_tbl_IN_MEM_CACHE[[tlb_nm]]  <<-  tbl(con, tlb_nm) %>% collect() 
    }
    
    toc <- Sys.time()
    print(paste0("It took ", format(toc-tic) ," to cache the table '",tlb_nm,"'."))
  }
  return(get_db_tbl_IN_MEM_CACHE[[tlb_nm]])
}


get_flat_case_tbl <- function(con = get_covid_cases_db_con(), flatten = F){
  #' get the wide and flat cases table forom the DB
  #' for the relational form it widens it.
  #'
  #'
  ret_df = get_db_tbl(con = con, tlb_nm = "case")
  if (flatten == T){
    #TODO:
    print("warning flatten not implemented yet in get_flat_case_tbl!, this is for Dbs with relational stuffs.....")
    return(NULL)
  }
  df_dts <-
    bind_cols(
  ret_df %>% select(matches("Date", ignore.case = F)) %>% #count(HospStartDate)
    select_if(is.character) %>%
    mutate_all(function(x){as.Date(as.numeric(x),origin = "1899-12-30")})
,
  ret_df %>% select(matches("Date", ignore.case = F)) %>% #count(HospStartDate)
    select_if(is.logical) %>%
    mutate_all(function(x){as.Date(as.numeric(x),origin = "1899-12-30")})

    )
  
  ret_df <- bind_cols(
  ret_df[! colnames(ret_df) %in% colnames(df_dts)] ,
  df_dts
  )
    
    
  return(ret_df)
}

########################
#chache the table
get_flat_case_tbl() #%>% select(matches("Date", ignore.case = F))




#get_reports_column_list()
get_reports_column_list <- function(in_dir = DIR_OF_INPUT, 
                                    fn = END_OF_DAY_REPORT_INPUT){
  #' Return a long table of report name and columns needed in that report, in the prefered order 
  #'
  #'
  #return(rio::import(file.path(in_dir, fn), setclass = "tibble"))
  return(readxl::read_xlsx(file.path(in_dir, fn)))
}


get_reports_dir_locations <- function(in_dir = DIR_OF_INPUT, 
                                    fn = END_OF_DAY_REPORT_INPUT#, 
                                    #report_filter
                                    ){
  #' Return a table of report name and directories to put the report into 
  #'
  #'
  #return(rio::import(file.path(in_dir, fn), setclass = "tibble"))
  return(readxl::read_xlsx(file.path(in_dir, fn), sheet = "report_locations"))
}


get_reports_lbls <- function(in_dir = DIR_OF_INPUT, 
                                      fn = END_OF_DAY_REPORT_INPUT#, 
                                      #report_filter
){
  #' Return a table of report name and labels
  #'
  #'
  #return(rio::import(file.path(in_dir, fn), setclass = "tibble"))
  return(readxl::read_xlsx(file.path(in_dir, fn), sheet = "lbls"))
}


get_cols_2_clean <- function(in_dir = DIR_OF_INPUT, 
                             fn = END_OF_DAY_REPORT_INPUT, 
                             report_filter
){
  #' Return a table of report name and directories to put the report into 
  #'
  #'
  #return(rio::import(file.path(in_dir, fn), setclass = "tibble"))
  readxl::read_xlsx(file.path(in_dir, fn), sheet = "input") %>% 
    filter(report == report_filter) %>% 
    mutate(clean_str = clean_yes_no(clean_str)) %>% 
    filter(clean_str == YES_STR) %>%
    pull(col_nm)
}





get_HCDaily_cols <- function(report_filter = "HCDaily", 
                             ...){
  #' Return Columns needed for the 'HCDaily' report 
  #'
  #'
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}
get_HCDaily_dir <- function(report_filter = "HCDaily", 
                            ...){
  #' Return directory needed for the 'HCDaily' report 
  #'
  #'
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}
get_export_should_write <- function(report_filter, 
                            ...){

  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% 
    pivot_longer(., colnames(.)) %>% 
    filter(name == as.character( lubridate::wday(Sys.Date(), label = T))) %>%
    pull(value) %>% 
    trimws() %>% nchar() > 0

}


get_StatsCan_cols <- function(report_filter = "STATCAN", 
                              ...){
  #' Return Columns needed for the 'STATCAN' report 
  #'
  #'
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}
get_StatsCan_dir <- function(report_filter = "STATCAN", 
                            ...){
  #' Return directory needed for the 'STATCAN' report 
  #'
  #'
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}





get_WHO_cols <- function(report_filter = "WHO", 
                         ...){
  #' Return Columns needed for the 'WHO' report 
  #'
  #'
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}
get_WHO_dir <- function(report_filter = "WHO", 
                            ...){
  #' Return directory needed for the 'WHO' report 
  #'
  #'
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}



get_case_data_domestic_epi_cols <- function(report_filter = "qry_allcases", 
                                            ...){
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}
get_case_data_domestic_epi_dir <- function(report_filter = "qry_allcases", 
                            ...){
  #' Return directory needed for the 'qry_allcases' report 
  #'
  #'
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}
get_case_db_errs_dir <- function(report_filter = "db_errs", 
                                           ...){
  #' Return directory needed for the 'qry_allcases' report 
  #'
  #'
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}


get_domestic_survelance_cols <- function(report_filter = "Domestic surveillance", 
                                         ...){
  #' Return Columns needed for the 'Domestic surveillance' or modelng report
  #'
  #'
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}

get_domestic_survelance_dir <- function(report_filter = "Domestic surveillance", 
                                           ...){
  #' Return directory needed for the 'Domestic surveillance' report 
  #'
  #'
  get_generic_report_dir(report_filter = report_filter, ...)
}
get_generic_report_dir <- function(report_filter, ...){
  #' Return directory needed for the any report
  #'
  #'  
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(dir)
}



make_hospstatus <- function(df){
  #' return vector indicating hospital status
  #'
  #'@param df must have all of the following columns, ICU, MechanicalVent, Hosp, ICUStartDate, HospStartDate
  df %>% 
    select(c("ICU", "MechanicalVent", "Hosp", "ICUStartDate", "HospStartDate", "VentStartDate")) %>% 
    mutate_at(vars(any_of(c("ICU", "MechanicalVent", "Hosp"))), clean_yes_no ) %>% 
    mutate(hospstatus = 
             if_else( ICU == YES_STR | !is.na(ICUStartDate) | MechanicalVent == YES_STR | !is.na(VentStartDate) , 
                      "Hospitalized - ICU",
                      if_else( Hosp == YES_STR | !is.na(HospStartDate), 
                               "Hospitalized - non-ICU",
                               if_else( toupper(Hosp) == NO_STR | !is.na(HospStartDate), 
                                        "Not hospitalized",
                                        UNKNOWN_STR
                               )
                      )
             )
           
    ) %>% pull(hospstatus)
}




make_Hospitalized <- function(df){
  #' return vector indicating hospitalized state, either yes or no or unknown
  #'
  #'@param df must have all of the following columns, ICU, MechanicalVent, Hosp, ICUStartDate, HospStartDate
  #'
  
  df %>% select(c("ICU", "MechanicalVent", "Hosp", "ICUStartDate", "HospStartDate",  "VentStartDate")) %>% 
    mutate_at(vars(any_of(c("ICU", "MechanicalVent", "Hosp"))), clean_yes_no ) %>% 
    mutate(Hospitalized = ifelse(ICU == YES_STR | !is.na(ICUStartDate) | MechanicalVent == YES_STR | !is.na(VentStartDate) |  Hosp == YES_STR| !is.na(HospStartDate) | Hosp == YES_STR, 
                             YES_STR, 
                             ifelse(Hosp == NO_STR , NO_STR, UNKNOWN_STR))) %>% pull(Hospitalized)
}


make_IntensiveCareUnit <- function(df){
  #' return vector indicating if they are or were in the ICU
  #'
  #' @param df must have all of the following columns,"ICU", "MechanicalVent",  "ICUStartDate", "VentStartDate"
  #'

  df %>% select(c("ICU", "MechanicalVent",  "ICUStartDate", "VentStartDate")) %>% 
    mutate_at(vars(any_of(c("ICU", "MechanicalVent"))), clean_yes_no ) %>% 
    mutate(IntensiveCareUnit = ifelse(ICU == YES_STR | !is.na(ICUStartDate) | MechanicalVent == YES_STR | !is.na(VentStartDate), 
                                 YES_STR, 
                                 ifelse(ICU == NO_STR , NO_STR, UNKNOWN_STR))) %>% pull(IntensiveCareUnit)
}

make_IndigenousGroup_BOOL <- function(df){
  #' return vector indicating if they are indigenous
  #'
  #' @param df must have all of the following columns,IndigenousGroup
  #'
  #'
  df$IndigenousGroup %>% #table()
    gsub(pattern = "[^[:alnum:]]", replacement = " ", x = .) %>% 
    gsub(pattern = "[ ]+", replacement = " ", x = .) %>% 
    grepl(pattern = "First Nation|Inuit|Metis",x = ., ignore.case = T) 
}

make_Indigenous2 <- function(df){
  #' return vector indicating if they are indigenous
  #'
  #' @param df must have all of the following columns,IndigenousGroup, Indigenous
  #'
  #'  
  df %>% mutate(., IndigenousGroup_BOOL = make_IndigenousGroup_BOOL(.)) %>% 
    select(IndigenousGroup_BOOL, Indigenous) %>% 
    mutate(Indigenous = clean_str(Indigenous)) %>% 
    mutate(Indigenous2 = ifelse(Indigenous == YES_STR | 
                                 IndigenousGroup_BOOL == TRUE, 
                                 YES_STR, 
                                 Indigenous )) %>% pull(Indigenous2)
}



make_Comorbidity_all <- function(df, sym_col_nm_patern = "^RF.*(?<!Spec)$"){
  #' return df indicating  many comorbitity flags
  #'
  #' @param df must have all of the following columns,PHACID, Asymptomatic, and some columns that match ,sym_col_nm_patern
  #'
  #'
  #'
  df %>% select(PHACID, matches(sym_col_nm_patern, perl = T)) %>%
  mutate_at(vars(matches(sym_col_nm_patern, perl = T)), clean_yes_no ) %>% 
  pivot_longer(matches(sym_col_nm_patern, perl = T)) %>% #count(value, sort = T) %>% view()
  mutate(value_y = (value == YES_STR)*1.0, 
         value_n = (value == NO_STR)*1.0) %>% 
  group_by(PHACID) %>% summarise(ComorbidityYes = ifelse(sum(value_y) >= 1, YES_STR, ""),
                                 ComorbidityNo = ifelse(sum(value_n) >= n(), NO_STR, ""),
                                 CountRF = sum(value_y)
  ) %>% 
  mutate(Comorbidity = if_else(ComorbidityYes == YES_STR, 
                               YES_STR, 
                               if_else(ComorbidityNo == NO_STR, 
                                       NO_STR, 
                                       UNKNOWN_STR 
                                       ) 
                               )
         )
}


make_Asymptomatic_all <- function(df, sym_col_nm_patern = "^sym.*(?<!Spec)$"){
  #' return df indicating  many asymptomatic flags
  #'
  #' @param df must have all of the following columns,PHACID, Asymptomatic, and some columns that match ,sym_col_nm_patern
  #'
  #'
  #'
  df2 <- 
    df %>% select(PHACID, Asymptomatic, matches(sym_col_nm_patern, perl = T))  %>% 
    mutate_at(vars(matches(sym_col_nm_patern, perl = T)), clean_yes_no ) %>% 
    mutate(Asymptomatic = clean_yes_no(Asymptomatic) ) 
  
  df3 <- 
    df2 %>%
    pivot_longer(matches(sym_col_nm_patern, perl = T)) %>%
    mutate(IsOther = name == "SymOther") %>% #count(IsOther) %>% 
    mutate(value_y = (value == YES_STR)*1.0, 
           value_n = (value == NO_STR | (value == UNKNOWN_STR & name == "SymOther"))*1.0) %>% 
    mutate(value_either = (value == YES_STR | value == NO_STR)) %>%#ount(value_y, value_n, value_either)
    group_by(PHACID) %>% summarise(AsymptomaticYES1 = ifelse(sum(value_n) >= n(), YES_STR, ""),
                                   AsymptomaticNO = ifelse(sum(value_y) >= 1, NO_STR, ""),
                                   SymptomsMissing = ifelse(sum(value_either) == 0 , YES_STR,"")) %>% #count(AsymptomaticYES1, AsymptomaticNO, SymptomsMissing) %>% 
    inner_join(df2 %>% select(PHACID, Asymptomatic), ., by = "PHACID") %>% 
    mutate(AsymptomaticYES = ifelse(Asymptomatic == YES_STR & SymptomsMissing  == YES_STR, YES_STR, "")) %>% 
    mutate(Asymptomatic2 = ifelse(AsymptomaticYES == YES_STR | AsymptomaticYES1  == YES_STR,YES_STR,
                                  ifelse(AsymptomaticNO == NO_STR ,NO_STR,UNKNOWN_STR))) %>% 
    select(-Asymptomatic)
    
    #%>% count(AsymptomaticYES1, AsymptomaticNO, SymptomsMissing, Asymptomatic, AsymptomaticYES, Asymptomatic2) %>% view()
  
  
  return(df3)
  
}
make_Asymptomatic_2 <-function(df){
  #' return vector indicating if they are Asymptomatic
  #'
  #' @param df must have all of the columns required by make_Asymptomatic_all
  #'
df %>%   
  make_Asymptomatic_all() %>% 
  select(PHACID, Asymptomatic2) %>% 
  left_join(df %>% select(PHACID), . , by = "PHACID") %>% 
  pull(Asymptomatic2)
}




make_Death <- function(df){
  #' return vector indicating if they are or were in the ICU
  #'
  #' @param df must have all of the following columns,"COVIDDeath", "Disposition"
  #'
  df %>% select(c("COVIDDeath", "Disposition")) %>% 
    mutate_at(vars(any_of(c("COVIDDeath"))), clean_yes_no ) %>%
    mutate_at(vars(any_of(c("Disposition"))), clean_str ) %>% #count(COVIDDeath, Disposition, sort = T)
    mutate(Death = ifelse(COVIDDeath == YES_STR, YES_STR, 
                          ifelse(Disposition %in%  c("Deceased", "", UNKNOWN_STR), UNKNOWN_STR, NO_STR)       
                          )) %>% 
    pull(Death)
}

make_EpisodeDate <- function(df){
  #' return vector indicating the episode date
  #'
  #' @param df must have all of the following columns, "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'
  cols_order = c("OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1")
  
  df %>% select(cols_order) %>% 
    non_missing_cols(cols_order = cols_order)
}

make_EpisodeType <- function(df){
  #' return vector indicating episode type
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'
  df %>% 
    select(c("OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1")) %>% 
    non_missing_cols_str(cols_order = c("OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"),
                              strs_for_col = c("Onset date", "Lab specimen collection date", "Lab test result date"))
}


make_Gender_2 <- function(df){
  #' return vector indicating gender after cleaning
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  df %>% 
    select(Gender)  %>% 
    mutate(Gender2 = clean_Categorical(Gender, good_vals = c("Female", "Male", "Other"), bad_value_replace = NOT_STATED_STR) ) %>% #count(Gender2)
    #clean_str() %>% 
    pull(Gender2)
}

make_Occupation_2 <- function(df){
  #' return vector indicating gender after cleaning
  #'
  #' @param df must have all of the following columns "Occupation_other"
  #'  
  #'  
  #'  
  df %>% 
    select(Occupation_other)  %>% 
    mutate(Occupation_Other = clean_str(Occupation_other)) %>% 
    mutate(Occupation_Other2 = if_else(Occupation_other == "", NOT_STATED_STR, Occupation_other)) %>% #count(Occupation_Other2, sort = T) %>% 
    pull(Occupation_Other2)
}


make_healthcare_worker_2 <- function(df){
  #' return vector indicating if health care worker
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  #'  
  #'  
  df %>% 
    select(Healthcare_worker)  %>% 
    mutate(Healthcare_worker2 = clean_Categorical(Healthcare_worker, 
                                             good_vals = c(YES_STR, NO_STR, UNKNOWN_STR), 
                                             bad_value_replace = NOT_STATED_STR )) %>% #count(Healthcare_worker, Healthcare_worker2, sort = T)
    pull(Healthcare_worker2)
}



make_LTC_Resident_2 <- function(df){
  #' return vector indicating gender after cleaning
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  #'  
  #'  
  df %>% 
    select(LTC_resident)  %>% 
    mutate(LTC_resident2 = clean_Categorical(LTC_resident, 
                                             good_vals = c(YES_STR, NO_STR, UNKNOWN_STR), 
                                             bad_value_replace = NOT_STATED_STR )) %>% #count(LTC_resident2, LTC_resident, sort = T)
    pull(LTC_resident2)
}

make_OnsetDate2 <- function(df){
  #' return vector indicating gender after cleaning
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  #'  
  #'  
  df %>% 
    select(OnsetDate)  %>%
    mutate(OnsetDate2 = clean_str (as.character(OnsetDate), NA_replace = NOT_STATED_STR, replacement = "-") ) %>% 
    pull(OnsetDate2)
}

make_Disposition2 <- function(df){
  #' return vector indicating Disposition2 
  #'
  #' @param df must have all of the following columns "Disposition"
  #'  
  #'  
  
  disposition_mapping <- 
  read.csv(strip.white = T, 
text="Disposition, Disposition2
ill, Stable
Self isolation, Stable
Stable, Stable
Not reported, Unknown
Other, Unknown
Pending, Unknown
,Unknown
Recovered, Recovered
Deceased, Deceased
", sep = ",") %>% as_tibble() %>% mutate_all(clean_str)
  
  
  
  df %>% 
    select(Disposition)  %>% 
    mutate(Disposition = clean_str (Disposition) ) %>% 
    left_join(disposition_mapping, by = "Disposition") %>% #count(Disposition, Disposition2, sort = T) 
    pull(Disposition2)
}

make_RecoveryDate2 <- function(df){
  #' return vector indicating the revoverydate
  #'
  #' @param df must have all of the following columns "RecoveryDate", "Disposition2"
  #'  
  df2 <- if("Disposition2" %in% colnames(df)){
    df %>% select(RecoveryDate, Disposition2)
  }else{
    df %>% mutate(., Disposition2 = make_Disposition2(.))%>% select(RecoveryDate, Disposition2)
  }
  
  df2 %>% mutate(RecoveryDate2 = as_datetime(ifelse(Disposition2 == "Recovered", RecoveryDate, NA ))) %>% pull(RecoveryDate2)
}


make_ExposureCountry <- function(df, sym_col_nm_patern = "^(TravelFromCountry|TravelToCountry)\\d", colapse_str = " ", don_t_care_country = c("Canada", "")){
  #' return vector indicating the exposure countries
  #'
  #' @param df must have all of the following columns "exposure_cat"
  #'
  
  df %>% select(PHACID, matches(sym_col_nm_patern)) %>%
    pivot_longer(matches(sym_col_nm_patern), values_drop_na = T) %>% 
    mutate(value = clean_str(value)) %>% 
    filter(! value %in% don_t_care_country) %>% 
    distinct(PHACID, value) %>% 
    group_by(PHACID) %>% 
    summarise(ExposureCountry = paste0(value, collapse = colapse_str)) %>% 
    left_join(df, ., by = "PHACID") %>% 
    replace_na(list(ExposureCountry = "")) %>% 
    pull(ExposureCountry)
}


make_exposure_cat2 <- function(df){
  #' return vector indicating the exposure_cat2
  #'
  #' @param df must have all of the following columns "exposure_cat"
  #'  
  
  
  
  df %>% mutate(EXPOSURE_CAT = clean_str(EXPOSURE_CAT))%>% #count(EXPOSURE_CAT, sort = T) %>% 
    mutate(exposure_cat2 = if_else(EXPOSURE_CAT == "", "Missing", 
                           if_else(grepl(pattern = "domestic" , x = EXPOSURE_CAT, ignore.case = T), "Domestic acquisition", EXPOSURE_CAT))
           ) %>%# count(exposure_cat2) %>% 
    pull(exposure_cat2)
}



make_final_clean_df <- function(df, 
                                report_filter, 
                                cols,
                                cols_2_clean = get_cols_2_clean(report_filter = report_filter)
){
  #' Cleans a DF before returning it
  
  
  df_cleaned <- 
    df %>% 
    select_if(.predicate = is.character) %>% 
    select(any_of(cols_2_clean)) %>% 
    mutate_all(clean_str)
  
  df[!colnames(df) %in% colnames(df_cleaned)] %>%
    bind_cols(., df_cleaned) %>%
    select(cols) %>% 
    mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) 
  
  
  
}




get_WHO <- function(con = get_covid_cases_db_con(), 
                         df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed", "Probable")),
                         cols = get_whp_cols()){
  
  
  
}



get_StatsCan <- function(con = get_covid_cases_db_con(), 
                        df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                        cols = get_StatsCan_cols()){
  #' Return df for the statcan report
  #'
  #'

  
  df2 <- 
    df %>% 
    mutate(., EpisodeDate = make_EpisodeDate(.),
           Gender2 = make_Gender_2(.),
           Occupation2 = make_Occupation_2(.),
           Healthcare_worker2 = make_healthcare_worker_2(.),
           LTC_Resident2 = make_LTC_Resident_2(.),
           OnsetDate2 = make_OnsetDate2(.),
           HospStatus = make_hospstatus(.),
           Disposition2 = make_Disposition2(.),
           RecoveryDate = make_RecoveryDate2(.),
           exposure_cat2 = make_exposure_cat2(.),
           RecoveryDate2 = make_RecoveryDate2(.),
           Occupation_other2 = make_Occupation_2(.)
           
    ) %>% 
    rename(Agegroup10 := AgeGroup10  ,age := Age)
  
  df2 %>%   
    make_Asymptomatic_all() %>% 
    select(PHACID, Asymptomatic2) %>% 
    left_join(df2, ., by = "PHACID") %>%   
    make_final_clean_df(report_filter = "STATCAN", cols = cols)
    # select(cols)%>% 
    # mutate_if(.predicate = is.character, clean_str) %>% 
    # mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) %>% 
    # mutate (PHACID = number_2_phacid(PHACID))
}


get_HCDaily <- function(con = get_covid_cases_db_con(), 
                                    df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                                    cols = get_HCDaily_cols()){
  #' reuturns Data frame for the HCDaily report
  #'
  #'
  df2 <- 
    df %>% 
    mutate(., EpisodeDate = make_EpisodeDate(.),
           Gender2 = make_Gender_2(.),
           Occupation2 = make_Occupation_2(.),
           Healthcare_worker2 = make_healthcare_worker_2(.),
           LTC_Resident2 = make_LTC_Resident_2(.),
           OnsetDate2 = make_OnsetDate2(.),
           HospStatus = make_hospstatus(.),
           Disposition2 = make_Disposition2(.),
           RecoveryDate = make_RecoveryDate2(.),
           exposure_cat2 = make_exposure_cat2(.),
           RecoveryDate2 = make_RecoveryDate2(.)
      
           ) %>% 
    rename(Agegroup10 := AgeGroup10  ,age := Age)
    
    
  df2 %>%   
    mutate(., Asymptomatic2 = make_Asymptomatic_2(.)) %>% 
    make_final_clean_df(report_filter = "HCDaily", cols = cols)
    # make_Asymptomatic_all() %>% 
    # select(PHACID, Asymptomatic2) %>% 
    # left_join(df2, . , by = "PHACID") %>% 
    # select(cols)%>% 
    # mutate_if(.predicate = is.character, clean_str) %>% 
    # mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) #%>% 
    #mutate (PHACID = number_2_phacid(PHACID))
      
  
}



get_domestic_survelance <- function(con = get_covid_cases_db_con(), 
                                    df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Probable", "Confirmed")),
                                    #countries_tbl = get_db_tbl(con = con, tlb_nm = "Travel_Countries") %>% select(PHACID, ExposureCountry),
                                    cols = get_domestic_survelance_cols()){
  #' reuturns Data frame for the Domestic survelance report
  #'
  #'
  df2 <- 
  df %>%
    mutate(., hospstatus = make_hospstatus(.))  %>% 
    mutate(., Hospitalized = make_Hospitalized(.)) %>%
    mutate(., IntensiveCareUnit = make_IntensiveCareUnit(.))  %>% 
    mutate(., Death = make_Death(.))
  
  
  df2 %>%   
    mutate(., Asymptomatic2 = make_Asymptomatic_2(.)) %>% 
    make_final_clean_df(report_filter = "Domestic surveillance", cols = cols)
    # make_Asymptomatic_all() %>% 
    # select(PHACID, Asymptomatic2) %>% 
    # left_join(df2,., by = "PHACID") %>% 
    # select(cols) %>% 
    # mutate_if(.predicate = is.character, clean_str) %>% 
    # mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) %>% 
    # mutate (PHACID = number_2_phacid(PHACID))
}






get_case_data_domestic_epi <- function(con = get_covid_cases_db_con(), 
                                       df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Probable", "Confirmed")),
                                       #countries_tbl = get_db_tbl(con = con, tlb_nm = "Travel_Countries") %>% select(PHACID, ExposureCountry) ,
                                       cols = get_case_data_domestic_epi_cols()
                                       
){
  
  df <- 
    df %>% 
    mutate_at(vars(any_of(c("ICU", "MechanicalVent", "Hosp", "Asymptomatic", "Indigenous", "ClusterOutbreak", "Reserve", 
                            "Healthcare_worker", "LTC_resident", "Isolation", "DeathResp", "COVIDDeath", "Travel", "CloseContactCase", 
                            "AnimalContact", "HealthFacilityExposure", "Hospitalized" ))), clean_yes_no ) %>% 
    mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
    mutate_at(vars(matches("^sym.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
    mutate_at(vars(matches("^Dx.*(?<!Spec)$", perl = T)), clean_yes_no )
  
  # 
  # df$EpisodeDate <- df %>%  make_EpisodeDate()
  # df$EpisodeType <- df %>% make_EpisodeType()
  # 

  # df %>% mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
  #   select(matches("^RF.*(?<!Spec)$", perl = T)) %>%
  #   pivot_longer(matches("^RF.*(?<!Spec)$", perl = T)) %>% count(value)
  # 
  
  
  
  df<-  
    df %>%
    mutate(., EpisodeDate = make_EpisodeDate(.))  %>% 
    mutate(., EpisodeType = make_EpisodeType(.))  %>% 
    mutate(., hospstatus = make_hospstatus(.))  %>% 
    mutate(., Hospitalized = make_Hospitalized(.))  #%>% select(Hospitalized, hospstatus)
  
  
  #x <- df$RFCardiacDisease %>% clean_Categorical(good_vals = c(YES_STR, NO_STR, ))
  
  df <- 
    df %>% 
    make_Comorbidity_all() %>% 
    left_join(df, ., by = "PHACID")
    # 
    # df %>% select(PHACID, matches("^RF.*(?<!Spec)$", perl = T)) %>%
    # mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
    # pivot_longer(matches("^RF.*(?<!Spec)$", perl = T)) %>% #count(value, sort = T) %>% view()
    # mutate(value_y = (value == YES_STR)*1.0, 
    #        value_n = (value == NO_STR)*1.0) %>% 
    # group_by(PHACID) %>% summarise(ComorbidityYes = ifelse(sum(value_y) >= 1, YES_STR, ""),
    #                                ComorbidityNo = ifelse(sum(value_n) >= n(), NO_STR, ""),
    #                                CountRF = sum(value_y)
    #                                ) %>% 
    # mutate(Comorbidity = if_else(ComorbidityYes == YES_STR, YES_STR, if_else(ComorbidityNo == NO_STR, NO_STR, UNKNOWN_STR ) )) %>% #count( ComorbidityYes, ComorbidityNo, CountRF, Comorbidity, sort = T)
    # left_join(df, ., by = "PHACID")
  
  df <- 
    df %>% 
    make_Asymptomatic_all() %>% 
    left_join(df , ., by = "PHACID")


  df <- df %>% mutate(., Indigenous2 = make_Indigenous2(.)) 
#   df$IndigenousGroup_BOOL <- 
#     df$IndigenousGroup %>% 
#     gsub(pattern = "[^[:alnum:]]", replacement = " ", x = .) %>% 
#     gsub(pattern = "[ ]+", replacement = " ", x = .) %>% 
#     grepl(pattern = "First Nation|Inuit|Metis",x = ., ignore.case = T) 
#   
#   
#   df <- df %>% mutate(Indigenous2 = ifelse(Indigenous == YES_STR | IndigenousGroup_BOOL == TRUE, YES_STR, Indigenous ))  #%>% #count(IndigenousGroup, Indigenous , Indigenous2, sort = T) %>% view()
# # 
#   df <- 
#   countries_tbl %>% 
#     mutate(ExposureCountry = stringr::str_to_upper(stringr::str_trim(ExposureCountry))) %>% #count(ExposureCountry, sort = T) %>% view()
#     left_join(df, ., by = "PHACID")

  df <- df %>% mutate(., ExposureCountry = make_ExposureCountry(.))
  
  
  
  
  
  df <- 
    df %>% 
    rename(agegroup10 := AgeGroup10, 
           agegroup20 := AgeGroup20, 
           Occupation_Other := Occupation_other,
           Location := LOCATION) %>%
    select(cols)  
  
  
  
  
  
  
  df <- df %>% make_final_clean_df(report_filter = "qry_allcases", cols = cols)
    # 
    # mutate_if(.predicate = is.character, clean_str) %>% 
    # mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) %>% 
    # mutate (PHACID = number_2_phacid(PHACID))
  return(df)
}







######################################
#
get_db_error_report_by_case_error_Residency_canadian<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "" & nchar(ResidenceCountry) > 0) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is unspecified, yet ResidenceCountry is not blank.")%>% collect() %>% 
    mutate(typ = "Residency" )
}
get_db_error_report_by_case_error_Residency_canadian_but_not<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "Canadian resident" & !(ResidenceCountry == "CANADA")) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is Canadian, yet ResidenceCountry is not.")%>% collect() %>% 
    mutate(typ = "Residency" )
}
get_db_error_report_by_case_error_Age_Age_group<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & (tolower(AgeGroup10) == tolower("Unknown") | tolower(AgeGroup20) == tolower("Unknown") )) %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Agegroup is listed as unkown.")%>% collect() %>% 
    mutate(typ = "Age" )
}
get_db_error_report_by_case_error_Age_Age_units<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & AgeUnit == "") %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Age units are not.") %>% collect() %>% 
    mutate(typ = "Age" )
}


get_db_error_report_by_case_Sym<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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

get_db_error_report_by_case_Hosp<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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

get_db_error_report_by_case_ICU<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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


get_db_error_report_by_case_Isolation<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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


get_db_error_report_by_case_vent<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!tolower(MechanicalVent) == "yes") %>% 
    filter(! is.na(VentStartDate) | ! is.na(VentEndDate) ) %>% 
    select(PHACID) %>% 
    collect() %>% 
    mutate(err = paste0("A vent date is listed but MechanicalVent is not Yes") ) %>% 
    select(PHACID, err) %>% 
    mutate(typ = "vent" )
}
get_db_error_report_by_case_Reovered<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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
get_db_error_report_by_case_Death<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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



get_db_error_report_by_case_travel<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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

get_db_error_report_by_case_close<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
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




get_db_error_report_by_case_Dead_and_alive<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter( (! is.na(DeathDate)) &  (! is.na(RecoveryDate)) ) %>% 
    select(PHACID, DeathDate, RecoveryDate) %>% 
    mutate(err = paste0("Both Death date '",DeathDate,"' and recovery date '",RecoveryDate,"' listed.") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "DeadorAlive" )
}

get_db_error_report_by_case_dead_before_onset<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter( (! is.na(DeathDate)) &  (! is.na(OnsetDate)) ) %>% 
    filter( DeathDate < OnsetDate) %>% 
    select(PHACID, DeathDate, OnsetDate) %>% 
    mutate(err = paste0("Dead before Onset '",DeathDate,"' < '",OnsetDate,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Early Dead" )
}


get_db_error_report_by_case_recovered_before_onset<- function(con = get_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter( (! is.na(RecoveryDate)) &  (! is.na(OnsetDate)) ) %>% 
    filter( RecoveryDate < OnsetDate) %>% 
    select(PHACID, RecoveryDate, OnsetDate) %>% 
    mutate(err = paste0("Recovered before Onset '",RecoveryDate,"' < '",OnsetDate,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Early Recovered" )
}


get_db_error_report_by_case_teasting_CloseContact_disagree<- function(con = get_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  #' Generates Error List on DB
  a_tbl %>% 
    filter( TestingReason == "Contact of a case" &  (!CloseContactCase ==  "Yes")) %>% 
    select(PHACID, TestingReason, CloseContactCase) %>% 
    mutate(err = paste0("TestingReason '",TestingReason,"' disagrees with  CloseContactCase '",CloseContactCase,"'") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Contact Testing Reason" )
}



###########################################
#
get_db_error_report_by_case <- function(con = get_db_con(), a_tbl = get_flat_case_tbl(con = con)){
  
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
      get_db_error_report_by_case_teasting_CloseContact_disagree(a_tbl = a_tbl)
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



extract_table_report <- function(df_fun, fn, report_filter, ...){
  
  if(!get_export_should_write(report_filter= report_filter)){
    print(paste0("Not wrting '",report_filter,"' today."))
    return(NULL)
  }
  
  print(paste0("reporting '",report_filter,"' now....")) 
  
  tic <- Sys.time()
  df <- df_fun(...)
  toc <- Sys.time()
  print(paste0("took ", format(toc - tic), " to get data"))
  
  tic <- Sys.time()
  
  target_dir <- dirname(fn)
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  
  writexl::write_xlsx(x = df ,#%>% head(50000),
                      path = fn,
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE)
  toc <- Sys.time()
  print(paste0("took ", format(toc - tic), " to write data"))
}



extract_case_db_errs <- function(){
  extract_table_report(df_fun = get_db_error_report_by_case, 
                       fn = path.expand( file.path(get_case_db_errs_dir(), 
                                                   paste0("db_errs "  , format(Sys.Date() ,"%Y-%m-%d"), ".xlsx")
                       )),
                       report_filter = "db_errs"
  )
}



extract_case_data_domestic_epi <- function(){
  extract_table_report(df_fun = get_case_data_domestic_epi, 
                       fn = path.expand( file.path(get_case_data_domestic_epi_dir(), 
                                                   paste0("qry_allcases "  , format(Sys.Date() ,"%m-%d-%Y"), ".xlsx")
                       )),
                       report_filter = "qry_allcases"
  )
}


extract_case_data_domestic_survelance <- function(){
  extract_table_report(df_fun = get_domestic_survelance, 
                       fn = path.expand( file.path(get_domestic_survelance_dir(), 
                                                   paste0("Domestic surveillance data - "  , format(Sys.Date() ,"%Y-%m-%d"), ".xlsx")
                       )),
                       report_filter = "Domestic surveillance"
  )
}


extract_case_data_get_HCDaily <- function(){
  extract_table_report(df_fun = get_HCDaily, 
                       fn = path.expand( file.path(get_HCDaily_dir(), 
                                                   paste0(format(Sys.Date() ,"%Y%m%d"), "_HCDaily.xlsx")
                       )),
                       report_filter = "HCDaily"
  )
  
  
}


extract_case_data_get_StatsCan <- function(){
  extract_table_report(df_fun = get_StatsCan, 
                       fn = path.expand( file.path(get_StatsCan_dir(), 
                                                   paste0("Weekly Extended Dataset_", format(Sys.Date() ,"%Y%m%d"), ".xlsx")
                       )),
                       report_filter = "STATCAN"
  )
  

}


backup_db <- function(full_fn = file.path(DIR_OF_DB_2_BACK_UP, NAME_DB_2_BACK_UP),
                      base_backup_location = file.path(dirname(full_fn), "BACKUP")
                        ){
  
  
  mmm <- format(Sys.Date(), "%b")
  mmm_dd <- format(Sys.Date(), "%b %d")
  target_dir <-file.path(base_backup_location, mmm, mmm_dd)
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  target_fn <- file.path(target_dir, basename(full_fn))
  
  print(paste0("backing up database to '", target_fn, "'"))
  tic = Sys.time()
  file.copy(full_fn, target_fn, overwrite = FALSE, recursive = FALSE,
           copy.mode = TRUE, copy.date = TRUE)
  toc = Sys.time()
  print(paste0("Done backup took ", format(toc - tic), "."))
}
# 
# compact_db <- function(full_fn = file.path(DIR_OF_DB, NAME_DB)){
#   msaccess.exe /compact "c:\path\to\my\database.mdb"
#   # LAUNCH ACCESS APP
#   oApp = COMCreate("Access.Application")
#   a <- path.expand(
#   file.path("c:", "Program Files (x86)", "Microsoft Office", "Office16")#, "msaccess.exe")
#   )
#   tmp <- getwd()
#   setwd(a)
#   getwd()
#   fn <- path.expand(full_fn)
#   system(command = paste0("msaccess.exe", "  /compact ", fn), intern =T , wait = T)
#   
#   accfiles <- list.files(path="C:\\Databases\\", pattern="\\.accdb", full.names=TRUE)
#   
#   for (file in accfiles){      
#     bkfile = sub(".accdb", "_bk.accdb", file)
#     
#     oApp$CompactRepair(file, bkfile, FALSE)
#     
#     file.copy(bkfile, file, overwrite = TRUE)
#     file.remove(bkfile)      
#   }
#   
#   oApp <- NULL
#   gc()
#   
# }
do_end_of_day_tasks <- function(){
  backup_db()
  extract_case_db_errs()
  extract_case_data_get_StatsCan()
  extract_case_data_domestic_epi()
  extract_case_data_domestic_survelance()
  extract_case_data_get_HCDaily()
  extract_epi_curves()
}
do_end_of_day_tasks()

