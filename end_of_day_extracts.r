


rm(list=ls())
gc()

#options(java.parameters = "-Xmx8000m")

library(tidyverse)
#library(rio)
library(janitor)
library(stringr)
library(DBI)
library(odbc)
library(dbplyr)
library(scales)
library(svglite)
library(docstring)
library(writexl)
library(readxl)
library(lubridate)
library(AzureStor)
#library(RDCOMClient)
library(metabaser)


META_BASE_HRE_URL <- "https://discover-metabase.hres.ca/api"
META_BASE_HRE_CASES_DB_ID <- 2
META_BASE_HRE_USER_NAME <- keyring::key_get("username_for_meta_base_on_HRE") #"howard.swerdfeger@canada.ca"
META_BASE_HRE_PASSWORD <- keyring::key_get("password_for_meta_base_on_HRE")




DIR_OF_HPOC_ROOT = "//Ncr-a_irbv2s/irbv2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020"

DIR_OF_DB = file.path("~", "..", "Desktop") 
#NAME_DB = "COVID-19_v2.accdb"
#DIR_OF_DB = file.path("~", "..", "Desktop")#"~"
#DIR_OF_DB = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "DATABASE", "MS ACCESS")#"~"

# 
DIR_OF_DB_2_BACK_UP = file.path("~", "..", "Desktop")
DIR_OF_DB_2_BACK_UP_INTO = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "DATABASE", "MS ACCESS", "BACKUP")
#DIR_OF_DB_2_BACK_UP = file.path(DIR_OF_HPOC_ROOT, "DATA AND ANALYSIS", "DATABASE", "MS ACCESS")#"~"


NAME_DB = "COVID-19_v2.accdb"#"Case.xlsx"
NAME_DB_2_BACK_UP = "COVID-19_v2.accdb"


#DIR_OF_OUTPUT = "//Ncr-a_irbv2s/irbv2/PHAC/IDPCB/CIRID/VIPS-SAR/EMERGENCY PREPAREDNESS AND RESPONSE HC4/EMERGENCY EVENT/WUHAN UNKNOWN PNEU - 2020/DATA AND ANALYSIS/DATABASE/R/output"
#DIR_OF_OUTPUT = "~"
DIR_OF_INPUT = getwd()
END_OF_DAY_REPORT_INPUT = "end_of_day_reports_input.xlsx"

YES_STR = "YES"
NO_STR = "NO"
Y_STR = "Y"
N_STR = "N"
NA_STR = "NA"
DO_NOT_STR = "DO NOT"

UNKNOWN_STR = "UNKNOWN"
#BLANK_VALUE_STR = "BLANK"
BLANK_VALUE_STR = ""
NOT_STATED_STR = "Not stated"
DEF_TYPE_DB = "MS_Access"
#DEF_TYPE_DB = "xlsx"
OUTPUT_DATE_FORMAT = "%Y-%m-%d"


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
    print(paste0("getting Con for ", db_type, " at ", db_full_nm))
    con <- dbConnect(odbc::odbc(),
                     .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=", 
                                                 db_full_nm, 
                                                 ";PWD=", db_pwd, ";ReadOnly=1;applicationintent=readonly;"))
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
  
  
  x %>% clean_str() %>% recode(Y = YES_STR,
                               N = NO_STR)
  
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
clean_str_b <- function(x, 
                        NA_replace = "", 
                        BLANK_replace = "",
                        capitalize_function = str_to_title){
  x %>% 
    str_replace_all(pattern = "[ ]+", replacement = " ") %>%
    ifelse(is.na(.), NA_replace, .) %>% 
    #str_to_lower() %>% 
    capitalize_function() %>% 
    trimws() %>%  #%>% table()
    ifelse(. == "", BLANK_replace, .) 
  
}



########################################
# 
clean_str <- function(x, 
                      do_pattern = T,
                      pattern = "[[:punct:]]", 
                      replacement = " ", 
                      NA_replace = "", 
                      BLANK_replace = "",
                      capitalize_function = str_to_title,
                      do_NA_string_replace = T){
  #' By default this replactes all punctuation, and trims spaces so there is only one
  #' then sets title case
  #' 
  x %>% 
    {if (do_pattern)str_replace_all(string = . , pattern = pattern, replacement = replacement) else .} %>%
    str_replace_all(pattern = "[ ]+", replacement = " ") %>%
    capitalize_function() %>% 
    ifelse(is.na(.), NA_replace, .) %>% 
    {if(do_NA_string_replace) ifelse(. == NA_STR , NA_replace, .) else .} %>%
    #str_to_lower() %>% 
    trimws() %>%  #%>% table()
    ifelse(. == "", BLANK_replace, .) %>% 
    capitalize_function() 
}


clean_st_CONSTANTS <- function(...){
  #'
  #'
  #'
  
  YES_STR <<- clean_str(YES_STR , ...)
  NO_STR<<- clean_str(NO_STR , ...)
  UNKNOWN_STR<<- clean_str(UNKNOWN_STR , ...)
  BLANK_VALUE_STR<<- clean_str(BLANK_VALUE_STR , ...) 
  NOT_STATED_STR<<- clean_str(NOT_STATED_STR , ...)
  Y_STR<<- clean_str(Y_STR , ...)
  N_STR<<- clean_str(N_STR , ...)
  NA_STR<<- clean_str(NA_STR , ...)
  DO_NOT_STR<<- clean_str(DO_NOT_STR , ...)
  
}

########################
# Run this to ensure that the constants all have the correct caseing
clean_st_CONSTANTS(capitalize_function = str_to_title)



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
                       tlb_nm = "case",
                       force_refresh = F){
  #' gets a raw tabie from a DB connections
  #' this will have a memmory cache to avoid multiple feteches 
  #'
  
  if (force_refresh){
    get_db_tbl_IN_MEM_CACHE[[tlb_nm]]  <<- NULL
  }
  
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
      ,
      ret_df %>% select(matches("Date", ignore.case = F)) %>% #count(HospStartDate)
        select_if(function(x) inherits(x, "POSIXct")) %>%
        mutate_all(as_date)
    )
  
  
  #mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date)
  
  ret_df <- bind_cols(
    ret_df[! colnames(ret_df) %in% colnames(df_dts)] ,
    df_dts
  )
  
  # This preserves the Accents
  lapply(ret_df %>% select_if(is.character) %>% colnames(), function(col_nm){
    Encoding(ret_df[[col_nm]]) <<- 'latin1'
  })
  
  
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
  #
  readxl::read_xlsx(file.path(in_dir, fn), sheet = "input") %>% 
    filter(report == report_filter) %>% 
    mutate(clean_str = clean_yes_no(clean_str)) %>% 
    filter(clean_str == YES_STR) %>%
    pull(col_nm)
}
#str_to_upper

str_to_same <- function(x){
  #'
  #'
  x  
}



get_cols_cleaning <- function(in_dir = DIR_OF_INPUT, 
                              fn = END_OF_DAY_REPORT_INPUT, 
                              report_filter
){
  #' Return a table of report name and directories to put the report into 
  #'
  #'
  #
  readxl::read_xlsx(file.path(in_dir, fn), sheet = "input", col_types = "text") %>% 
    filter(report == report_filter) %>% 
    mutate_at(c("clean_it","punct_replace", "na_blank_replace", "case_standard"), function(x){if_else(is.na(x), "", x)}) %>%
    mutate(clean_it = trimws(str_to_title(clean_it))) %>% 
    mutate(na_blank_replace = trimws(na_blank_replace)) %>% 
    mutate(case_standard = trimws(case_standard)) %>% 
    mutate(punct_replace = trimws(punct_replace)) 
  
  
  #clean_it)#, 
  #do_pattern = FALSE, 
  #capitalize_function  = str_to_same)
}






get_export_should_write <- function(report_filter, 
                                    ...){
  #' Returns true if we should write the extract today
  #' False if we shouldn't
  #' 
  
  get_reports_dir_locations() %>% 
    filter(report == report_filter) %>% 
    pivot_longer(., colnames(.)) %>% 
    mutate(value = clean_str(value)) %>% 
    filter(name == as.character( lubridate::wday(Sys.Date(), label = T))) %>%
    pull(value) %>% 
    trimws() %>% nchar() > 0
  
}



get_report_save_type <- function(report_filter, 
                            ...){
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(type)
}

get_report_cols <- function(report_filter, 
                            ...){
  #' Return Columns needed for the  Needed report 
  #'
  #'
  get_reports_column_list(...) %>% 
    filter(report == report_filter) %>% pull(col_nm)
}


get_report_dir_copy <- function(report_filter, ...){
  #' Return director of the  report 
  #'
  #'
  
  get_reports_dir_locations(...) %>% 
    filter(report == report_filter) %>% pull(CopyLocation)
}

get_report_dir <- function(report_filter, ...){
  #' Return director of the  report 
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
  #'
  #'
  
  #  This is the SQL code to generate it
  #  hospstatus: IIf(ICU="Yes" Or icustartdate Is Not Null Or mechanicalvent="Yes",
  #                 "Hospitalized - ICU",
  #                 
  #                 IIf(hosp="Yes" Or hospstartdate Is Not Null,
  #                     "Hospitalized - non-ICU",
  #                     IIf(hosp="No","Not hospitalized","Unknown")))
  
  
  df %>% 
    select(c("ICU", "MechanicalVent", "Hosp", "ICUStartDate", "HospStartDate", "VentStartDate")) %>% 
    mutate_at(vars(any_of(c("ICU", "MechanicalVent", "Hosp"))), clean_yes_no ) %>% 
    mutate(hospstatus = 
             if_else( ICU == YES_STR | !is.na(ICUStartDate) | MechanicalVent == YES_STR | !is.na(VentStartDate) , 
                      "Hospitalized - ICU",
                      if_else( Hosp == YES_STR | !is.na(HospStartDate), 
                               "Hospitalized - non-ICU",
                               if_else( Hosp == NO_STR | !is.na(HospStartDate), 
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


make_sex_2 <- function(df){
  #' return vector indicating sex_2 after cleaning
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  df %>% 
    select(sex)  %>% 
    mutate(sex2 = clean_Categorical(sex, good_vals = c("Female", "Male", "Other"), bad_value_replace = NOT_STATED_STR) ) %>% #count(sex2)
    #clean_str() %>% 
    pull(sex2)
}

make_Occupation_2 <- function(df){
  #' return vector indicating occupation_2 after cleaning
  #'
  #' @param df must have all of the following columns "Occupationspec"
  #'  
  #'  
  #'  
  df %>% 
    select(occupationspec)  %>% 
    mutate(occupationspec = clean_str(occupationspec)) %>% 
    mutate(occupationspec2 = if_else(occupationspec == "", NOT_STATED_STR, occupationspec)) %>% #count(occupationspec2, sort = T) %>% 
    pull(occupationspec2)
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
  #' return vector indicating LTC_resident_2 after cleaning
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
  #' return vector indicating onsetdate2 after cleaning
  #'
  #' @param df must have all of the following columns "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1"
  #'  
  #'  
  #'  
  df %>% 
    select(OnsetDate)  %>%
    mutate(OnsetDate2 = clean_str(as.character(OnsetDate), NA_replace = NOT_STATED_STR, replacement = "-") ) %>% 
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
    mutate(Disposition = clean_str(Disposition) ) %>% 
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
    df %>% mutate(., Disposition2 = make_Disposition2(.)) %>% select(RecoveryDate, Disposition2)
  }
  
  df2 %>% mutate(RecoveryDate2 = if_else(Disposition2 == "Recovered", RecoveryDate, as.Date(NA, "1970-01-01") )) %>% pull(RecoveryDate2)
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
                                #cols_2_clean = get_cols_2_clean(report_filter = report_filter),
                                cleaning_instructions = get_cols_cleaning(report_filter = report_filter)
){
  #' Cleans a DF before returning it
  #df <- get_db_tbl(con = con, tlb_nm = "qry_allcases_with_Intl_Travel_Exposure")
  
  
  
  # Get clean Cols  
  cols2clean <- 
    df %>% 
    select_if(.predicate = is.character) %>% 
    select(any_of(cleaning_instructions %>% filter (clean_it == YES_STR) %>% pull(col_nm))) %>%  
    colnames()
  
  
  df_cleaned <- 
    map_dfc(cols2clean , function(col){
      
      clean_col_inst <- cleaning_instructions %>% filter(col_nm == col) %>% slice(1)
      df[[col]] %>%
        {if (clean_col_inst$punct_replace == DO_NOT_STR) . else gsub("[[:punct:]]", clean_col_inst$punct_replace, .)} %>%
        ifelse(. == "", clean_col_inst$na_blank_replace, .) %>%
        ifelse(is.na(.), clean_col_inst$na_blank_replace, .) %>%
        str_replace_all(pattern = "[ ]+", replacement = " ") %>%
        {if (nchar(clean_col_inst$case_standard) >= 1) get(clean_col_inst$case_standard)(.) else . } %>%
        trimws()
    })
  colnames(df_cleaned) <- cols2clean
  # df_cleaned <- 
  # df %>% 
  #   select_if(.predicate = is.character) %>% 
  #   select(any_of(cleaning_instructions %>% filter (clean_str == YES_STR) %>% pull(col_nm))) %>% 
  #   mutate_all(function(x, clean_col_inst){
  #     print(names(x))
  #     print(head(x))
  #     return(.)
  #     # x %>% 
  #     #   {if (clean_col_inst$punct_replace == DO_NOT_STR) . else gsub("[[:punct:]]", clean_col_inst$punct_replace, .)} %>%
  #     #   ifelse(. == "", clean_col_inst$na_blank_replace, .) %>%
  #     #   ifelse(is.na(.), clean_col_inst$na_blank_replace, .) %>%
  #     #   str_replace_all(pattern = "[ ]+", replacement = " ") %>%
  #     #   {if (length(clean_col_inst$case_standard) > 1) get(clean_col_inst$case_standard)(.) else . }
  #   }   )
  
  
  
  df2 <- 
    df[!colnames(df) %in% colnames(df_cleaned)] %>%
    {if(ncol(df_cleaned) > 0) bind_cols(., df_cleaned) else .} %>%
    #select(cols) %>% 
    mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) %>%  
    rename_all(str_to_lower)
  
  
  df2 %>% select(str_to_lower(cols))
  
  
  # 
  # get_cols_cleaning
  # df_cleaned <- 
  #   df %>% 
  #   select_if(.predicate = is.character) %>% 
  #   select(any_of(cols_2_clean)) %>% 
  #   mutate_all(clean_str)
  
  # df[!colnames(df) %in% colnames(df_cleaned)] %>%
  #   bind_cols(., df_cleaned) %>%
  #   select(cols) %>% 
  #   mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date)# %>%
  #   #mutate_if(is.Date, format, OUTPUT_DATE_FORMAT)
  
  
  
}


get_WHO <- function(con = get_metabase_con()#, 
                    #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                    #cols = get_report_cols("HCDaily")
){
  #' reuturns Data frame for the HCDaily report
  #'
  #'
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from who;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover data_hub took ",format(toc - tic)))
  return(all_cases)  
  
}



get_WHO_OLD <- function(con = get_covid_cases_db_con(), 
                    #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed", "Probable")),
                    cols = get_report_cols("WHO")
){
  df <- get_db_tbl(con = con, tlb_nm = "WHO")#, force_refresh = T)
  df %>% 
    make_final_clean_df(report_filter = "WHO", cols = cols)
  
  
}

get_email_sentence_OLD <- function(con = get_covid_cases_db_con(),
                               df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed", "Probable"))
){
  
  msg <- paste0("Total Confirmed & Probable cases in database on ",Sys.Date()," EOD is '", format(nrow(df), big.mark = ","),"' while Deaths are '", 
                df %>% filter(COVIDDeath == YES_STR) %>% nrow() %>% format(big.mark = ",") ,"'") 
  print(msg)
  return(msg)
}


get_email_sentence <- function(con = get_metabase_con()
                               ){
  
  
  nc <- metabase_query(con, "select count(*) from all_cases;", col_types = cols(.default = col_character())) %>% pull()
  nd <- metabase_query(con, "select count(*) from all_cases WHERE coviddeath ='yes';", col_types = cols(.default = col_character())) %>% pull()
  msg <- paste0("Total Confirmed & Probable cases in database on ",Sys.Date()," EOD is '", format(nc, big.mark = ","),"' while Deaths are '", 
                nd %>% format(big.mark = ",") ,"'") 
  print(msg)
  return(msg)
}






get_ijn <- function(con = get_covid_cases_db_con(),
                    df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed", "Probable"))
){
  df %>% 
    mutate(. , EpisodeDate = make_EpisodeDate(.)) %>% 
    mutate(. , EpisodeType = make_EpisodeType(.)) %>% 
    select(PHACReportedDate	,PT	,PTCaseID,	PHACID,	EpisodeDate	,EpisodeType	,Age	,sex	,Residency	,LOCATION,	LabSpecimenCollectionDate1,	matches("^Travel.*"),EXPOSURE_CAT,	Classification) %>% 
    #  filter(PHACID =="48-197101")
    filter(between(PHACReportedDate, Sys.Date()- 7 , Sys.Date()-1) | is.na(PHACReportedDate)) %>% 
    filter(EXPOSURE_CAT == "International travel") %>% 
    pivot_longer(matches("^Travel.*Date\\d$"), values_drop_na = F) %>% 
    group_by(PHACID) %>%  mutate(TravelDateMissing = all(is.na(value))) %>% ungroup() %>% 
    mutate(meet_travel_Date_condition = 
             if_else(TravelDateMissing, EpisodeDate >= PHACReportedDate - 16,
                     value >= PHACReportedDate - 16)) %>% 
    pivot_wider() %>% 
    filter(meet_travel_Date_condition) %>%
    filter(between(EpisodeDate,  Sys.Date()- 14, Sys.Date()+1))   %>% 
    select(- Travel, -TravelDateMissing, -meet_travel_Date_condition) %>% 
    select(PHACReportedDate	,PT	,PTCaseID,	PHACID,	EpisodeDate	,EpisodeType	,Age	,sex	,Residency	,LOCATION,	LabSpecimenCollectionDate1,	matches("^Travel.*"), EXPOSURE_CAT,	Classification)
  
}


get_values_count_summary <- function(con = get_metabase_con(),
                                     a_tbl = get_case_data_domestic_epi(con = con),
                                     strats = c("pt", "agegroup20"),
                                     MAX_NUM_unique_values_ALLOWED = 60#,
                                     #single_strat_wide = T
                                     # 
                                     # get_flat_case_tbl(con = con) %>% 
                                     # filter(Classification %in% c("Confirmed", "Probable"))
){
  
  a_tbl_mn <- 
    a_tbl %>% 
    select(matches("date")) %>% mutate(detectedatentry = as.Date(detectedatentry))
    mutate_all(as.Date, tryFormats = "%Y-%m-%d") %>% 
    #select_if(is.Date) %>%
    mutate_all(lubridate::month) %>% 
    rename_all(paste0, "_month")
  
  a_tbl_wk <- 
    a_tbl %>% 
    select(matches("Date")) %>%
    select_if(is.Date) %>%
    mutate_all(lubridate::week) %>% 
    rename_all(paste0, "_week")
  
  a_tbl_dow <- 
    a_tbl %>% 
    select(matches("Date")) %>%
    select_if(is.Date) %>%
    mutate_all(lubridate::wday) %>% 
    rename_all(paste0, "_dow")
  
  
  new_tbl <- bind_cols( a_tbl %>% select(! matches("Date")) , a_tbl_mn, a_tbl_wk, a_tbl_dow) 
  
  

  cnms <- colnames(new_tbl)
  cnms <- cnms[ ! cnms %in% c("phacid", "ptcaseid", "comments")]
  cnms <- cnms[ ! cnms %in% strats]
  cnms <- cnms[ ! cnms  %in% c("phacid")]
  cnms <- cnms[ ! grepl(pattern = "spec$", x = cnms)]
  cnms <- cnms[ ! grepl(pattern = "comment", x = cnms)]
  
  
  ret_val <- 
    map_dfr(cnms,function(cnm){
      print(cnm)
      uvals <- new_tbl %>% #count(!!sym(cnm))
        mutate(value = clean_str(!!sym(cnm), BLANK_replace = "NULL or BLANK STRING", NA_replace = "NULL or BLANK STRING")) %>% 
        group_by_at(c("value", strats)) %>% 
        summarise(n = n()) %>%
        ungroup()
        
      
      
      uvals <- 
        if(length(uvals$value %>% unique()) > MAX_NUM_unique_values_ALLOWED){
          return(NULL)
          # uvals <- uvals %>% arrange(desc(n)) %>% group_by(PT) %>%  
          #   do(head(., n = 9))
          # uvals
        }else{uvals}

      uvals <-
        uvals %>% group_by_at(strats) %>% 
        mutate(n_all_strats := sum(n, na.rm = T)) %>% ungroup() %>% 
        mutate(f_all_strats := n / n_all_strats) #%>% count(n, sort = T, name = "A")
      
      
            
      
      uvals_by_strat <- 
        lapply(strats, function(srtt){
          sum_nm <- paste0("d_", srtt)
          num_nm <- paste0("n_", srtt)
          #sum_nm_all_strats <- paste0("denom_", srtt)
          frac_nm <- paste0("f_", srtt)
          
          uvals %>% 
            group_by_at(c("value", srtt)) %>% 
            summarise(!!num_nm := sum(n, na.rm = T)) %>% ungroup() %>%
            group_by_at(srtt) %>% 
            mutate(!!sum_nm := sum(!!sym(num_nm), na.rm = T)) %>% ungroup() %>% 
            mutate(!!frac_nm := !!sym(num_nm) / !!sym(sum_nm))
          #   mutate(srtt)
          #             !!sum_nm_all_strats := sum(n_all_strats, na.rm = T)) %>% ungroup() %>% 
          # mutate(!!frac_nm := !!sym(sum_nm) / !!sym(sum_nm_all_strats))
          # 
        
          
        }) %>% reduce(inner_join)#, by = c("value", "n", strats))
      
      uvals2 <- inner_join(uvals, uvals_by_strat, by = c("value", strats))
      
        
      # uvals <-
      #   uvals %>% group_by_at(strats) %>% 
      #     mutate(n_all_strats := sum(n, na.rm = T)) %>% ungroup() %>% 
      #     mutate(f_all_strats := n / n_all_strats)
      
      
      uvals2 %>% 
        mutate(name = cnm,
               value = as.character(value)) %>%
        return()
    })
  
  
  
  # if (single_strat_wide = T){
  #   ret_wide <- 
  #     ret_val %>% pivot_wider(names_from = pt, values_from = n, values_fill  = 0) %>% 
  #     mutate(. , Canada = rowSums(.[3:ncol(.)])) 
  #   return(ret_wide)
  # }else{
    return(ret_val)
  #}
  
}



get_StatsCan <- function(con = get_metabase_con()){
  #' Return df for the statcan report
  #'
  #'
  
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from statscan;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover statscan took ",format(toc - tic)))
  return(all_cases)  
  
}

get_StatsCan_OLD <- function(con = get_covid_cases_db_con(), 
                         #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                         cols = get_report_cols("STATCAN")){
  #' Return df for the statcan report
  #'
  #'
  
  
  df <- get_db_tbl(con = con, tlb_nm = "qry_StatsCan")#, force_refresh = T)
  df %>% 
    make_final_clean_df(report_filter = "STATCAN", cols = cols)
  
 
  
  
}

get_DataHub_OLD <- function(con = get_covid_cases_db_con(), 
                        df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                        cols = get_report_cols("DataHub")){
  df2 <- 
    df %>% 
    mutate(., EpisodeDate = make_EpisodeDate(.)) %>%
    mutate(., Occupation2 = make_Occupation_2(.)) %>%
    mutate(., Healthcare_worker2 = make_healthcare_worker_2(.)) %>%
    mutate(., Asymptomatic2 = make_Asymptomatic_2(.)) %>%
    mutate(., OnsetDate2 = make_OnsetDate2(.)) %>%
    mutate(., HospStatus = make_hospstatus(.)) %>%
    mutate(., Disposition2 = make_Disposition2(.)) %>%
    mutate(., RecoveryDate2 = make_RecoveryDate2(.)) %>%
    mutate(., exposure_cat2 = make_exposure_cat2(.))#%>%
    
    
    
  
    
  
  pts <- df2 %>% distinct(PT) %>% pull()
    
  to_impute <- c("OnsetDate","EpisodeDate")
  to_impute_from <- c("PHACReportedDate", "ReportedDate", "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1", "EpisodeDate")
    
  imuted_dates <- 
    map_dfr(pts, function(curr_pt){
      
      df_for_impute <-
        df2 %>% 
        filter(PT == curr_pt) %>% 
        select(PHACID,PT,  to_impute, to_impute_from)
      
      
      imuted_dates_pt <- 
        map_dfc(to_impute, function(c_nm){make_imputed_date(df = df_for_impute, date_col_nm = c_nm) }) %>% 
        set_names(to_impute) %>%
        rename_all(function(x){paste0(x, "_imputed")}) %>% 
        #select_if(function(x) any(!is.na(x))) %>% 
        #select_if(function(x){sum(is.na(x))/length(x)< 0.5}) %>%
        bind_cols(df_for_impute %>% select(PHACID), .)
      
      print(curr_pt)
      print(ncol(imuted_dates_pt))
      return(imuted_dates_pt)
    })    
    
  df3 <- 
    df2 %>% 
    left_join(., imuted_dates, by = "PHACID") %>%
    select(cols, matches("_Imputed")) %>% 
    #bind_cols(., imuted_dates) %>% 
    make_final_clean_df(report_filter = "DataHub", cols = cols) 
  
  
  return(df3)
}




get_DataHub <- function(con = get_metabase_con()#, 
                        #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                        #cols = get_report_cols("HCDaily")
){
  #' reuturns Data frame for the HCDaily report
  #'
  #'
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from data_hub;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover data_hub took ",format(toc - tic)))
  return(all_cases)  
  
}

get_HCDaily <- function(con = get_metabase_con()#, 
                        #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed")),
                        #cols = get_report_cols("HCDaily")
                        ){
  #' reuturns Data frame for the HCDaily report
  #'
  #'
  
  
  
  
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from data_hub;", col_types = cols(.default = col_character())) %>% select(-pt)
  toc <- Sys.time()
  print(paste0("discover data_hub took ",format(toc - tic)))
  
  
  return(all_cases)  
  
  # df <- get_db_tbl(con = con, tlb_nm = "qry_HCDaily")#, force_refresh = T)
  # df %>% 
  #   make_final_clean_df(report_filter = "HCDaily", cols = cols)
  
  
  
  
  
  # df2 <- 
  #   df %>% 
  #   mutate(., EpisodeDate = make_EpisodeDate(.),
  #          sex2 = make_sex_2(.),
  #          Occupation2 = make_Occupation_2(.),
  #          Healthcare_worker2 = make_healthcare_worker_2(.),
  #          LTC_Resident2 = make_LTC_Resident_2(.),
  #          OnsetDate2 = make_OnsetDate2(.),
  #          HospStatus = make_hospstatus(.),
  #          Disposition2 = make_Disposition2(.),
  #          RecoveryDate = make_RecoveryDate2(.),
  #          exposure_cat2 = make_exposure_cat2(.),
  #          RecoveryDate2 = make_RecoveryDate2(.)
  #     
  #          ) %>% 
  #   rename(Agegroup10 := AgeGroup10  ,age := Age)
  #   
  #   
  # df2 %>%   
  #   mutate(., Asymptomatic2 = make_Asymptomatic_2(.)) %>% 
  #   make_final_clean_df(report_filter = "HCDaily", cols = cols)
  
  
}


# # missing_analysis <- function(df2, cols_not = c("PHACID", "PT")){
#   
#   df2_missing <-     
#     df2 %>% mutate_at(vars(!all_of(c("PHACID", "PT"))),
#                       function(x){
#                         is.na(x) | nchar(x) == 0
#                       })
#   
#   
# }

get_completness_analysis <- function(con = get_covid_cases_db_con(),
                                     df = get_flat_case_tbl(con) %>% filter(Classification %in% c("Probable", "Confirmed"))
)
{
  df2 <- 
    df %>% 
    make_final_clean_df(., report_filter = "Completness",  
                        cols = colnames(.)#, 
                        #cols_2_clean = colnames(.)
                        #cleaning_instructions = get_cols_cleaning(report_filter = "Completness")
    )
  
  
  df2_Deceased <- df2 %>% filter (disposition == "deceased")
  
  df2_missing <-     
    df2 %>% mutate_at(vars(!all_of(c("phacid", "pt"))),
                      function(x){
                        is.na(x) | nchar(x) == 0
                      })
  
  
  cols <- df2_missing %>% select(-c("phacid", "pt")) %>% colnames() 
  
  base <- expand.grid(pt = unique(df$PT), TF = c(TRUE, FALSE))
  
  
  df2_missing_n <- 
    lapply(cols, function(col){
      df2_missing %>% count(pt, !!sym(col)) %>% 
        mutate(colnm = col) %>%
        rename(TF := !!sym(col))
    }) %>% bind_rows()
  
  df2_missing_n %>% 
    pivot_wider(names_from = TF,values_from = n,values_fill = 0) %>% clean_names() %>% 
    mutate(total = false + true) %>%
    mutate(fraction_complete = false/total) %>% 
    rename(has_data := false, no_data := true) %>% 
    pivot_wider(names_from = pt, values_from = c("has_data",  "no_data", "total", "fraction_complete"))
  
}


get_domestic_survelance <- function(con = get_metabase_con()
                                   ){
  #' returns Data frame for the Domestic survelance report

  
  
  
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from modelling_data;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover modelling_data took ",format(toc - tic)))
  
  
  return(all_cases)
} 


get_domestic_survelance_OLD <- function(con = get_covid_cases_db_con(), 
                                    #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Probable", "Confirmed")),
                                    #countries_tbl = get_db_tbl(con = con, tlb_nm = "Travel_Countries") %>% select(PHACID, ExposureCountry),
                                    cols = get_report_cols("Domestic surveillance")){
  #' returns Data frame for the Domestic survelance report
  #'
  #'
  df <- get_db_tbl(con = con, tlb_nm = "qry_ModellingData")#, force_refresh = T)
  df %>% 
    make_final_clean_df(report_filter = "Domestic surveillance", cols = cols)
  
  # 
  # 
  # 
  # df2 <- 
  # df %>%
  #   mutate(., hospstatus = make_hospstatus(.))  %>% 
  #   mutate(., Hospitalized = make_Hospitalized(.)) %>%
  #   mutate(., IntensiveCareUnit = make_IntensiveCareUnit(.))  %>% 
  #   mutate(., Death = make_Death(.))
  # 
  # 
  # df2 %>%   
  #   mutate(., Asymptomatic2 = make_Asymptomatic_2(.)) %>% 
  #   make_final_clean_df(report_filter = "Domestic surveillance", cols = cols)
  
}


#' 
#' df %>% select(matches("date"))
#' df %>% select_if(function(x){is(x, class2 = "POSIXct")})
#' df %>% select_if(is, class = "POSIXct")
#' 
#' class(df$OnsetDate)
#' 


get_deltas_dates <- function(df,
                          colforcompare = "OnsetDate",
                          cols_dts = df %>% select_if(function(x){is(x, class2 = "POSIXct")}) %>% colnames(),
                          units = "days",
                          min_frac = 0.5){
  #' what are the deltas of all the relevent columns

  map_dfc(grep(pattern = colforcompare,  x = cols_dts ,value = T , invert = T), function(curr_col){
    newcolnm <- paste0(colforcompare, "_2_", curr_col)

    x = as.integer(difftime(df[[curr_col]], df[[colforcompare]], units = units))
    if (sum(is.na(x))*(1/min_frac) < length(x)){
      x = replace(x, is.na(x), values = sample(x = x[!is.na(x)], size = sum(is.na(x)), replace = T))
    }else{
      x = rep(NA, length(x))
    
    }
    tibble(col = x
    ) %>%
      rename(!!sym(newcolnm) := col)
  })
}
#' 
#' 
#' #todo:
#' #TODO:
make_imputed_date <- function(df = get_db_tbl(con = con, tlb_nm = "qry_allcases_with_Intl_Travel_Exposure"),
                              date_col_nm = "OnsetDate",
                              date_col_nm_patern = "date"
){
  #df %>% impute_deltas() %>% view()


  cols_dts = df %>% select_if(function(x){is(x, class2 = "POSIXct") | is.Date(x)}) %>% colnames()

  df2 <-
    bind_cols(df %>% select(!!sym(date_col_nm)),
              get_deltas_dates(df = df, colforcompare = date_col_nm, cols_dts = cols_dts))

  
  cols_to_use <- 
    map_dfc( df2 %>% select(starts_with(date_col_nm)) %>% select_if(is.numeric), function(x) {
      quantile(x, probs = c(0.95), na.rm = T) - quantile(x, probs = c(0.05), na.rm = T)
    }) %>% sort() %>% colnames()  
  
  df2[[date_col_nm]] <- as.Date(df2[[date_col_nm]])
  x <- df2[[date_col_nm]]
  
  #x%>% is.na() %>% sum()
  for (c_nm in cols_to_use){
      col_2_nm <- gsub(paste0(date_col_nm, "_2_"), "",c_nm)
      x <-
      if_else(!is.na(x), x ,
           if_else(!is.na(df2[[c_nm]]), 
                   as.Date(df[[col_2_nm]] - df2[[c_nm]], origin="1970-01-01"),
                   as.Date(NA)
           )
      )  
      #print(x %>% is.na() %>% sum() )
    
  }
  
  
  return(x) 
}
# 
# library(feather)
# 
# dir <- DIR_OF_HPOC_ROOT
# 
# AZURE_KEY = keyring::key_get(report_filter)
# blob_cont <- blob_container(endpoint = "https://storphacdpihaadv01.blob.core.windows.net/hswerdfe", key=AZURE_KEY)
# 
# 
# 
# 
# 
# tic <- Sys.time()
# short_fn <- "qry_allcases_testing_2.xlsx"
# writexl::write_xlsx(x = df,
#                     path = file.path(rappdirs::user_cache_dir(), short_fn),
#                     col_names = TRUE,
#                     format_headers = TRUE,
#                     use_zip64 = FALSE
# )
# AzureStor::upload_blob(container = blob_cont, src = file.path(rappdirs::user_cache_dir(), short_fn), dest = short_fn)
# 
# toc <- Sys.time()
# print (paste0("xlsx = ",  format(toc - tic)))
# 
# 
# tic <- Sys.time()
# short_fn <- "qry_allcases_testing_2.csv"
# df %>% write_csv(file.path(rappdirs::user_cache_dir(), short_fn))
# AzureStor::upload_blob(container = blob_cont, src = file.path(rappdirs::user_cache_dir(), short_fn), dest = short_fn)
# toc <- Sys.time()
# print (paste0("csv = ",  format(toc - tic)))
# 
# 
# tic <- Sys.time()
# short_fn <- "qry_allcases_testing_2.rds"
# df %>% saveRDS(file.path(rappdirs::user_cache_dir(), short_fn))
# AzureStor::upload_blob(container = blob_cont, src = file.path(rappdirs::user_cache_dir(), short_fn), dest = short_fn)
# toc <- Sys.time()
# 
# print (paste0("rds = ",  format(toc - tic)))
# 
# 
# tic <- Sys.time()
# short_fn <- "qry_allcases_testing_2.feather"
# df %>% write_feather(file.path(rappdirs::user_cache_dir(), short_fn))
# AzureStor::upload_blob(container = blob_cont, src = file.path(rappdirs::user_cache_dir(), short_fn), dest = short_fn)
# toc <- Sys.time()
# print (paste0("feather = ",  format(toc - tic)))



get_metabase_con <- function(base_url = META_BASE_HRE_URL,
                              database_id = META_BASE_HRE_CASES_DB_ID,
                              username = META_BASE_HRE_USER_NAME,
                              password = META_BASE_HRE_PASSWORD)
{
    metabase_login(base_url = base_url,
                                    database_id = database_id,
                                    username = username,
                                    password = password)  
}
  




get_case_data_web_epi <- function(con = get_metabase_con(), 
                                       #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Probable", "Confirmed")),
                                       #countries_tbl = get_db_tbl(con = con, tlb_nm = "Travel_Countries") %>% select(PHACID, ExposureCountry) ,
                                       cols = get_report_cols("qry_allcases")
                                       
){
  #' return Dataframe with Query all cases in it
  #' 
  #' 
  # if (! is.null(get_db_tbl_IN_MEM_CACHE[["get_case_data_domestic_epi"]])){
  #   return(get_db_tbl_IN_MEM_CACHE[["get_case_data_domestic_epi"]])
  # }
  # 
  
  
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from all_cases_web;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover all_cases took ",format(toc - tic)))
  
  
  return(all_cases)
}



get_case_data_domestic_epi <- function(con = get_metabase_con(), 
                                       #df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Probable", "Confirmed")),
                                       #countries_tbl = get_db_tbl(con = con, tlb_nm = "Travel_Countries") %>% select(PHACID, ExposureCountry) ,
                                       cols = get_report_cols("qry_allcases")
                                       
){
  #' return Dataframe with Query all cases in it
  #' 
  #' 
  # if (! is.null(get_db_tbl_IN_MEM_CACHE[["get_case_data_domestic_epi"]])){
  #   return(get_db_tbl_IN_MEM_CACHE[["get_case_data_domestic_epi"]])
  # }
  # 

  
  
  tic <- Sys.time()
  all_cases <- metabase_query(con, "select * from all_cases;", col_types = cols(.default = col_character()))
  toc <- Sys.time()
  print(paste0("discover all_cases took ",format(toc - tic)))
  
  
  return(all_cases)
  
  # df <- get_db_tbl(con = con, tlb_nm = "qry_allcases_with_Intl_Travel_Exposure")#, force_refresh = T)
  # 
  # pts <- df %>% distinct(PT) %>% pull()
  # 
  # 
  # to_impute <- c("PHACReportedDate", "ReportedDate", "OnsetDate", "LabSpecimenCollectionDate1", "LabTestResultDate1", "EpisodeDate")
  # 
  # 
  # imuted_dates <- 
  #   map_dfr(pts, function(curr_pt){
  #     
  #     df_for_impute <-
  #     df %>% 
  #       filter(PT == curr_pt) %>% 
  #       select(PHACID,PT,  to_impute)
  #     
  #     
  #     imuted_dates_pt <- 
  #       map_dfc(to_impute, function(c_nm){make_imputed_date(df = df_for_impute, date_col_nm = c_nm) }) %>% 
  #       set_names(to_impute) %>%
  #       rename_all(function(x){paste0(x, "_imputed")}) %>% 
  #       #select_if(function(x) any(!is.na(x))) %>% 
  #       #select_if(function(x){sum(is.na(x))/length(x)< 0.5}) %>%
  #       bind_cols(df_for_impute %>% select(PHACID), .)
  #     
  #     #print(curr_pt)
  #     #print(ncol(imuted_dates_pt))
  #     return(imuted_dates_pt)
  #   })
  # #df %>% pull(OnsetDate) %>% is.na() %>% sum()
  # df2 <- 
  # df %>% 
  #   left_join(., imuted_dates, by = "PHACID") %>%
  #   #bind_cols(., imuted_dates) %>% 
  #   make_final_clean_df(report_filter = "qry_allcases", cols = cols)
  # 
  # 
  # get_db_tbl_IN_MEM_CACHE[["get_case_data_domestic_epi"]]  <<- df2
  # #df2 %>% select(pt, episodedate, episodedate_imputed) %>% filter(pt == "mb") %>% filter(is.na(episodedate))
  # return(df2)
 
  
  #   
  #   
  #   df <- 
  #     df %>% 
  #     mutate_at(vars(any_of(c("ICU", "MechanicalVent", "Hosp", "Asymptomatic", "Indigenous", "ClusterOutbreak", "Reserve", 
  #                             "Healthcare_worker", "LTC_resident", "Isolation", "DeathResp", "COVIDDeath", "Travel", "CloseContactCase", 
  #                             "AnimalContact", "HealthFacilityExposure", "Hospitalized" ))), clean_yes_no ) %>% 
  #     mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
  #     mutate_at(vars(matches("^sym.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
  #     mutate_at(vars(matches("^Dx.*(?<!Spec)$", perl = T)), clean_yes_no )
  #   
  #   # 
  #   # df$EpisodeDate <- df %>%  make_EpisodeDate()
  #   # df$EpisodeType <- df %>% make_EpisodeType()
  #   # 
  # 
  #   # df %>% mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
  #   #   select(matches("^RF.*(?<!Spec)$", perl = T)) %>%
  #   #   pivot_longer(matches("^RF.*(?<!Spec)$", perl = T)) %>% count(value)
  #   # 
  #   
  #   
  #   
  #   df<-  
  #     df %>%
  #     mutate(., EpisodeDate = make_EpisodeDate(.))  %>% 
  #     mutate(., EpisodeType = make_EpisodeType(.))  %>% 
  #     mutate(., hospstatus = make_hospstatus(.))  %>% 
  #     mutate(., Hospitalized = make_Hospitalized(.))  #%>% select(Hospitalized, hospstatus)
  #   
  #   
  #   #x <- df$RFCardiacDisease %>% clean_Categorical(good_vals = c(YES_STR, NO_STR, ))
  #   
  #   df <- 
  #     df %>% 
  #     make_Comorbidity_all() %>% 
  #     left_join(df, ., by = "PHACID")
  #     # 
  #     # df %>% select(PHACID, matches("^RF.*(?<!Spec)$", perl = T)) %>%
  #     # mutate_at(vars(matches("^RF.*(?<!Spec)$", perl = T)), clean_yes_no ) %>% 
  #     # pivot_longer(matches("^RF.*(?<!Spec)$", perl = T)) %>% #count(value, sort = T) %>% view()
  #     # mutate(value_y = (value == YES_STR)*1.0, 
  #     #        value_n = (value == NO_STR)*1.0) %>% 
  #     # group_by(PHACID) %>% summarise(ComorbidityYes = ifelse(sum(value_y) >= 1, YES_STR, ""),
  #     #                                ComorbidityNo = ifelse(sum(value_n) >= n(), NO_STR, ""),
  #     #                                CountRF = sum(value_y)
  #     #                                ) %>% 
  #     # mutate(Comorbidity = if_else(ComorbidityYes == YES_STR, YES_STR, if_else(ComorbidityNo == NO_STR, NO_STR, UNKNOWN_STR ) )) %>% #count( ComorbidityYes, ComorbidityNo, CountRF, Comorbidity, sort = T)
  #     # left_join(df, ., by = "PHACID")
  #   
  #   df <- 
  #     df %>% 
  #     make_Asymptomatic_all() %>% 
  #     left_join(df , ., by = "PHACID")
  # 
  # 
  #   df <- df %>% mutate(., Indigenous2 = make_Indigenous2(.)) 
  # #   df$IndigenousGroup_BOOL <- 
  # #     df$IndigenousGroup %>% 
  # #     gsub(pattern = "[^[:alnum:]]", replacement = " ", x = .) %>% 
  # #     gsub(pattern = "[ ]+", replacement = " ", x = .) %>% 
  # #     grepl(pattern = "First Nation|Inuit|Metis",x = ., ignore.case = T) 
  # #   
  # #   
  # #   df <- df %>% mutate(Indigenous2 = ifelse(Indigenous == YES_STR | IndigenousGroup_BOOL == TRUE, YES_STR, Indigenous ))  #%>% #count(IndigenousGroup, Indigenous , Indigenous2, sort = T) %>% view()
  # # # 
  # #   df <- 
  # #   countries_tbl %>% 
  # #     mutate(ExposureCountry = stringr::str_to_upper(stringr::str_trim(ExposureCountry))) %>% #count(ExposureCountry, sort = T) %>% view()
  # #     left_join(df, ., by = "PHACID")
  # 
  #   df <- df %>% mutate(., ExposureCountry = make_ExposureCountry(.))
  #   
  #   
  #   
  #   
  #   
  #   df <- 
  #     df %>% 
  #     rename(agegroup10 := AgeGroup10, 
  #            agegroup20 := AgeGroup20, 
  #            Occupationspec := Occupationspec,
  #            Location := LOCATION) %>%
  #     select(cols)  
  #   
  #   
  #   
  #   
  #   
  #   
  #   df <- df %>% make_final_clean_df(report_filter = "qry_allcases", cols = cols)
  #     # 
  #     # mutate_if(.predicate = is.character, clean_str) %>% 
  #     # mutate_if(.predicate = function(x) inherits(x, "POSIXct"), as_date) %>% 
  #     # mutate (PHACID = number_2_phacid(PHACID))
  #   return(df)
}







######################################
#
get_db_error_report_by_case_error_Residency_canadian<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "" & nchar(ResidenceCountry) > 0) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is unspecified, yet ResidenceCountry is not blank.")%>% collect() %>% 
    mutate(typ = "Residency is unspecified, yet ResidenceCountry is not blank." )
}
get_db_error_report_by_case_error_Residency_canadian_but_not<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(Residency == "Canadian resident" & !(str_to_lower( ResidenceCountry )==str_to_lower("CANADA"))) %>% 
    select(PHACID) %>% 
    mutate(err = "Residency is Canadian, yet ResidenceCountry is not.")%>% collect() %>% 
    mutate(typ = "Residency is Canadian, yet ResidenceCountry is not." )
}
get_db_error_report_by_case_error_Age_Age_group<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & (tolower(AgeGroup10) == tolower("Unknown") | tolower(AgeGroup20) == tolower("Unknown") )) %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Agegroup is listed as unkown.")%>% collect() %>% 
    mutate(typ = "Age group unkown, but known age" )
}
get_db_error_report_by_case_error_Age_Age_units<- function(con = get_covid_cases_db_con(), a_tbl = tbl(con, "case") ){
  a_tbl %>% 
    filter(!is.na(Age) & AgeUnit == "") %>% 
    select(PHACID) %>% 
    mutate(err = "Exact Age is given but Age units are not.") %>% collect() %>% 
    mutate(typ = "No Age units" )
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
    mutate(typ = "Asymptomatic but not" )
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
    mutate(typ = "A hospital date is listed but Hosp is not Yes" )
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
    mutate(typ = "Recovery date listed but not recovered" )
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
    mutate(typ = "Death date listed but not dead." )
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
    mutate(typ = "Both Death date and recovery date listed." )
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

get_db_error_report_by_case_2020_dates<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  
  tmp <- 
    a_tbl %>% 
    select(matches("Date")) %>%
    select_if(is.Date) 
  
  col_nms <- colnames(tmp)
  #select(PHACReportedDate, ReportedDate) %>% 
  #mutate(a = PHACReportedDate > Sys.Date()) %>% select(a) %>% table()
  tmp %>%   mutate_all(function(x){x < as.Date("2020-01-01")}) %>%
    bind_cols(a_tbl %>% select(PHACID), .) %>% 
    pivot_longer(cols = matches("Date"), values_drop_n = T) %>% 
    filter(value == T) %>% 
    mutate(err = paste0(name , " is before 2020-01-01") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "Before 2020-01-01 Date" )
  
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


get_db_error_report_by_age_bad<- function(con = get_covid_cases_db_con(), a_tbl = get_flat_case_tbl(con = con) ){
  a_tbl %>% 
    select(Age, PHACID) %>% 
    filter(Age < 0  | Age > 113) %>%
    mutate(err = paste0("Age  ",Age," is  wrong") ) %>% 
    select(PHACID, err)  %>% collect() %>% 
    mutate(typ = "age out of range" )
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
      get_db_error_report_by_age_bad(a_tbl = a_tbl),
      get_db_error_report_by_case_2020_dates(a_tbl = a_tbl)
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

save_file_as_excel_and_rds_file <- function(x, path, ...){
  
  library(tools)
  rm_ext <- paste0(".",tools::file_ext(path), "$")
  dir <- dirname(path)
  base_nm <- basename(path)
  basbasename <- str_remove(base_nm, rm_ext)
  

  
  writexl::write_xlsx(x = x, 
                      path = file.path(dir , paste0(basbasename, ".xlsx")),
                      ...)
  saveRDS(object = x ,file = file.path(dir, paste0(basbasename, ".rds")))
  
  return(T) 
}


save_file_as_sqlite_xlsx_and_rds_file <- function(x, path, ...){
  library(tools)
  rm_ext <- paste0(".",tools::file_ext(path), "$")
  dir <- dirname(path)
  base_nm <- basename(path)
  basbasename <- str_remove(base_nm, rm_ext)
  
  
  conn <- dbConnect(RSQLite::SQLite(), file.path(dir, paste0(basbasename, ".db")))
  conn <- dbConnect(RSQLite::SQLite(), file.path(dir, paste0(basbasename, ".db")))
  dbWriteTable(conn = conn, value = df, name = "all_cases_web")
  
  writexl::write_xlsx(x = x, 
                      path = file.path(dir , paste0(basbasename, ".xlsx")),
                      ...)
  saveRDS(object = x ,file = file.path(dir, paste0(basbasename, ".rds")))
  
  return(T) 
  
  
}



extract_table_report <- function(df_fun,
                                 fn, 
                                 report_filter, 
                                 password = NULL, 
                                 save_type = "file", 
                                 dir = dirname(fn), 
                                 short_fn = basename(fn), 
                                 saving_func = writexl::write_xlsx,
                                 ...){
  
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
  if (save_type == "file")
  {  
  
  
    target_dir <- dirname(fn)
    if ( ! dir.exists(target_dir)){
      dir.create(target_dir, recursive = T)
    }
      saving_func(x = df ,#%>% head(50000),
                          path = fn,
                          col_names = TRUE,
                          format_headers = TRUE,
                          use_zip64 = FALSE)
      
      # xlsx::write.xlsx(x = df, 
      #            file = fn, 
      #            col.names=TRUE, 
      #            row.names=FALSE, 
      #            append=FALSE, 
      #            showNA=FALSE, 
      #            password=password)
  }else if (save_type == "azureblob"){
   #todo: 
    AZURE_KEY = keyring::key_get(report_filter)
    blob_cont <- blob_container(endpoint = dir, key=AZURE_KEY)
    saving_func(x = df,
        path = file.path(rappdirs::user_cache_dir(), short_fn),
        col_names = TRUE,
        format_headers = TRUE,
        use_zip64 = FALSE
      )
    AzureStor::upload_blob(container = blob_cont, src = file.path(rappdirs::user_cache_dir(), short_fn), dest = paste0("data/",short_fn))
    #TODO:
  }
  
  toc <- Sys.time()
  print(paste0("took ", format(toc - tic), " to write data"))
  
  
  new_Loc <- get_report_dir_copy(report_filter = report_filter)
  
  if (! is.na(new_Loc)){
    target_fn = file.path(new_Loc, basename(fn) )
    
    if ( ! dir.exists(new_Loc)){
      dir.create(new_Loc, recursive = T)
    }
    
    
    print(paste0("Now I am copying over the report to the new location: '",new_Loc,"'", Sys.time()))
    file.copy(fn, target_fn, overwrite = TRUE, recursive = FALSE,
              copy.mode = TRUE, copy.date = TRUE)
    
  }
  
}



extract_case_db_errs <- function(){
  extract_table_report(df_fun = get_db_error_report_by_case, 
                       fn = path.expand( file.path(get_report_dir("db_errs"), 
                                                   paste0("db_errs "  , format(Sys.Date() ,"%Y-%m-%d"),".xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "db_errs"
  )
}



extract_case_data_domestic_epi <- function(){
  extract_table_report(df_fun = get_case_data_domestic_epi, 
                       fn = path.expand( file.path(get_report_dir("qry_allcases"), 
                                                   paste0("qry_allcases "  , format(Sys.Date() ,"%m-%d-%Y"),"_DISCOVER.xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "qry_allcases", 
                       saving_func = save_file_as_excel_and_rds_file
  )
}




extract_case_data_static_file <- function(){
  extract_table_report(df_fun = get_case_data_domestic_epi, 
                       fn = path.expand( file.path(get_report_dir("static_file"), 
                                                   paste0("qry_allcases_current.rds")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "static_file", 
                       saving_func = save_file_as_excel_and_rds_file
  )
}

  

extract_case_data_static_file_web <- function(){
  extract_table_report(df_fun = get_case_data_web_epi, 
                       fn = path.expand( file.path(get_report_dir("static_file_web"), 
                                                   paste0("all_cases_web_current.rds")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "static_file_web", 
                       saving_func = save_file_as_excel_and_rds_file
  )
}









extract_case_data_domestic_survelance <- function(){
  extract_table_report(df_fun = get_domestic_survelance, 
                       fn = path.expand( file.path(get_report_dir("Domestic surveillance"), 
                                                   paste0("Domestic surveillance data - "  , format(Sys.Date() ,'%Y-%m-%d'),"_DISCOVER.xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "Domestic surveillance"
  )
}
extract_case_data_domestic_survelance_OLD <- function(){
  extract_table_report(df_fun = get_domestic_survelance_OLD, 
                       fn = path.expand( file.path(get_report_dir("Domestic surveillance"), 
                                                   paste0("Domestic surveillance data - "  , format(Sys.Date() ,"%Y-%m-%d"),"_NEW_FORMAT.xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "Domestic surveillance"
  )
}

extract_case_data_get_HCDaily <- function(){
  
  fn = paste0(format(Sys.Date() ,"%Y%m%d"), "_HCDaily_DISCOVER.xlsx")#_SEMI_AUTOMATED.xlsx")
  full_fn = path.expand( file.path(get_report_dir("HCDaily"), fn))
  
  
  extract_table_report(df_fun = get_HCDaily, 
                       fn = full_fn,
                       report_filter = "HCDaily"
  )
  
  
}





extract_case_data_get_DataHub <- function(){
  
  # fn = paste0(format(Sys.Date() ,"%Y%m%d"), "_DataHub_NEW_FORMAT.xlsx")#_SEMI_AUTOMATED.xlsx")
  fn = paste0("current_DataHub.xlsx")#_SEMI_AUTOMATED.xlsx")
  full_fn = path.expand( file.path(get_report_dir("DataHub"), fn))
  
  
  extract_table_report(df_fun = get_DataHub, 
                       fn = full_fn,
                       report_filter = "DataHub",
                       save_type = get_report_save_type("DataHub")
  )
  
  
  
  
}




extract_case_data_get_DataHub_OLD <- function(){
  
  # fn = paste0(format(Sys.Date() ,"%Y%m%d"), "_DataHub_NEW_FORMAT.xlsx")#_SEMI_AUTOMATED.xlsx")
  fn = paste0("current_DataHub_NEW_FORMAT.xlsx")#_SEMI_AUTOMATED.xlsx")
  full_fn = path.expand( file.path(get_report_dir("DataHub"), fn))
  
  
  extract_table_report(df_fun = get_DataHub_OLD, 
                       fn = full_fn,
                       report_filter = "DataHub",
                       save_type = get_report_save_type("DataHub")
  )

  
  
  
}



extract_case_data_get_StatsCan <- function(){
  fn = paste0("Weekly Extended Dataset_", format(Sys.Date() ,"%Y%m%d"),"_DISCOVER.xlsx")#, "_SEMI_AUTOMATED.xlsx")
  full_fn = path.expand( file.path(get_report_dir("STATCAN"), fn))
  
  extract_table_report(df_fun = get_StatsCan, 
                       fn = full_fn,
                       report_filter = "STATCAN"
  )
  
  
  
}
extract_case_data_get_StatsCan_OLD <- function(){
  fn = paste0("Weekly Extended Dataset_", format(Sys.Date() ,"%Y%m%d"),"_NEW_FORMAT.xlsx")#, "_SEMI_AUTOMATED.xlsx")
  full_fn = path.expand( file.path(get_report_dir("STATCAN"), fn))
  
  extract_table_report(df_fun = get_StatsCan_OLD, 
                       fn = full_fn,
                       report_filter = "STATCAN"
  )
  
  
  
}

extract_case_data_get_WHO <- function(){
  extract_table_report(df_fun = get_WHO, 
                       #password = keyring::key_get(),
                       fn = path.expand( file.path(get_report_dir("WHO"), 
                                                   paste0("Canada_COVID19_WHO_linelist-", format(Sys.Date() ,"%d%B%Y"),".xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "WHO"
  )
  
}

extract_case_data_get_WHO_OLD <- function(){
  extract_table_report(df_fun = get_WHO_OLD, 
                       #password = keyring::key_get(),
                       fn = path.expand( file.path(get_report_dir("WHO"), 
                                                   paste0("Canada_COVID19_WHO_linelist-", format(Sys.Date() ,"%d%B%Y"),"_NEW_FORMAT.xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "WHO"
  )
  
}



extract_case_data_Count_Summary <- function(){
  extract_table_report(df_fun = get_values_count_summary, 
                       fn = path.expand( file.path(get_report_dir("CountSummary"), 
                                                   paste0("CountSummary_", format(Sys.Date() ,"%Y-%m-%d"),".xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "CountSummary"
  )
  
}




extract_case_data_get_ijn <- function(){
  extract_table_report(df_fun = get_ijn, 
                       #password = keyring::key_get(),
                       fn = path.expand( file.path(get_report_dir("ijn"), 
                                                   paste0("IJN_Daily updt_", format(Sys.Date() ,"%b%d"), "_pm.xlsx")
                       )),
                       report_filter = "ijn"
  )
  
}





extract_case_data_get_completness_analysis <- function(){
  extract_table_report(df_fun = get_completness_analysis, 
                       #password = keyring::key_get(),
                       fn = path.expand( file.path(get_report_dir("Completness"), 
                                                   paste0("completness-", format(Sys.Date() ,"%Y-%m-%d"),".xlsx")#, "_SEMI_AUTOMATED.xlsx")
                       )),
                       report_filter = "Completness",
  )
  
}


extract_case_data_get_metabase_diff <- function(){
  report_filter = "metabase_diff"
  
  meta_diff_dir <- get_report_dir(report_filter)
  
  if ( ! dir.exists(meta_diff_dir)){
    dir.create(meta_diff_dir, recursive = T)
  }
  
  library(metabaser)
  #META_BASE_HRE_URL <- "https://discover-metabase.hres.ca/api"
  #META_BASE_HRE_URL <- "https://discover-evh1.hres.ca/api"
  # META_BASE_HRE_CASES_DB_ID <- 2
  # META_BASE_HRE_USER_NAME <- keyring::key_get("username_for_meta_base_on_HRE") #"howard.swerdfeger@canada.ca"
  # META_BASE_HRE_PASSWORD <- keyring::key_get("password_for_meta_base_on_HRE")
  # 
  metabase_handle <- metabase_login(base_url = META_BASE_HRE_URL,
                                    database_id = META_BASE_HRE_CASES_DB_ID,
                                    username = META_BASE_HRE_USER_NAME,
                                    password = META_BASE_HRE_PASSWORD)
#   
#   
#   qry_allcases_web_v2
#   
#   
#   
#   
#   sql_str <- 
#   "select cases.phacid,
#     cases.pt,
#     case when onsetdate is not null then onsetdate when min(labspecimencollectiondate) is not null then min(labspecimencollectiondate) when min(labtestresultdate) is not null then min(labtestresultdate) end as episodedate,
#     cases.classification,
# 	case when cases.sex = '' or cases.sex is null then 'not stated' else sex end as sex2,
# 	case when (age <= 19 or agegrouping = '0-19') then '0 to 19'  
# 		when (age <= 29 or agegrouping = '20-29') then '20 to 29' 
# 		when (age <= 39 or agegrouping = '30-39') then '30 to 39' 
# 		when (age <= 49 or agegrouping = '40-49') then '40 to 49' 
# 		when (age <= 59 or agegrouping = '50-59') then '50 to 59' 
# 		when (age <= 69 or agegrouping = '60-69') then '60 to 69' 
# 		when (age <= 79 or agegrouping = '70-79') then '70 to 79' 
# 		when (age >= 80 or agegrouping = '80+') then '80 or plus' 
# 		else 'unknown' end as agegroup10,
#        cases.age,
#        case when occupation is null or occupation = '' then 'not stated' else occupation end as occupation2,
#        case when occupationhcw is null or occupationhcw = '' then 'not stated' else occupationhcw end as healthcare_worker2,
#        case when ltc_resident is null or ltc_resident = '' then 'not stated' else ltc_resident end as ltc_resident2,
# 	case when cases.phacid in (
# 		select phacid
# 		from symptoms
# 		where symptomid > 1 and symptomid < 35 and symptomvalue = 'yes'
# 		) then 'no'
# 		when cases.phacid not in (
# 			select phacid
# 			from symptoms
# 			where symptomid = 1
# 			) then 'unknown' 
# 		else (
# 			select symptomvalue
# 			from symptoms
# 			where symptomid = 1 and cases.phacid = symptoms.phacid
# 			) end as asymptomatic2,
#        cases.onsetdate,
#        max(case when symptomid = 2 then symptomvalue end) symcough,
#        max(case when symptomid = 3 then symptomvalue end) symfever,
#        max(case when symptomid = 4 then symptomvalue end) symchills,
#        max(case when symptomid = 5 then symptomvalue end) symsorethroat,
#        max(case when symptomid = 6 then symptomvalue end) symrunnynose,
#        max(case when symptomid = 7 then symptomvalue end) symshortnessofbreath,
#        max(case when symptomid = 8 then symptomvalue end) symnausea,
#        max(case when symptomid = 9 then symptomvalue end) symheadache,
#        max(case when symptomid = 10 then symptomvalue end) symweakness,
#        max(case when symptomid = 11 then symptomvalue end) sympain,
#        max(case when symptomid = 12 then symptomvalue end) symirritability,
#        max(case when symptomid = 13 then symptomvalue end) symdiarrhea,
#        max(case when symptomid = 35 then symptomvalue end) symother,
#        max(case when symptomid = 35 then symptomspec end) symotherspec,
# 	case when icu = 'yes' or icustartdate is not null or mechanicalvent = 'yes' or ventstartdate is not null then 'hospitalized - icu' 
# 		when hosp = 'yes' or hospstartdate is not null then 'hospitalized - non-icu'
# 		when hosp = 'no' then 'not hospitalized'
# 		else 'unknown' end as hospstatus,
#     case when disposition = 'ill' then 'stable'
# 		when disposition in ('not reported', 'other', 'pending', '', null) then 'unknown'
# 		when disposition = 'self isolation' then 'stable'
# 		when disposition = 'recovered' then 'recovered'
# 		when disposition = 'deceased' then 'deceased' else disposition end as disposition2,
#     case when disposition = 'recovered' then recoverydate else null end as recoverydate2,
# 	case when cases.phacid in (
# 			select phacid
# 			from travel
# 			where travel_international_loc not in ('canada', 'unclassifiable', 'pei', 'not collected') 
# 		) or cases.pt = 'repatriate' then 'international travel' 
# 		when exposures.closecontacttravel = 'yes' then 'domestic acquisition'
# 		when exposures.closecontactcase = 'yes' then 'domestic acquisition'
# 		when closecontactcase is not null or cases.phacid in (
# 			select phacid
# 			from travel
# 			where travel_international_loc in ('canada') 
# 			) or exposures.travel not in ('', 'yes', null) then 'domestic acquisition'
# 		when cases.pt = 'on' then 'information pending' end as exposure_cat2
# from 
# 		cases
#     left join
#         symptoms on cases.phacid = symptoms.phacid
#     left join
#         severity on cases.phacid = severity.phacid
#     left join
#         exposures on cases.phacid = exposures.phacid
#     left join
#         lab on cases.phacid = lab.phacid
# where cases.classification = 'confirmed'
# group by cases.phacid, severity.phacid, exposures.phacid;"
#   
#   
#   
#   tmp2 <- metabase_query(metabase_handle, sql_str, col_types = cols(.default = col_character()))
#   
#   tmp <- metabase_query(metabase_handle, "select * from all_cases", col_types = cols(.default = col_character()))
# 
#   
  
    
  cases_meta_raw <- metabase_query(metabase_handle, "SELECT * from cases", col_types = cols(.default = col_character()))
  
  cases_meta_raw <- metabase_query(metabase_handle, "SELECT * from cases", col_types = cols(.default = col_character()))
  severity_meta_raw <- metabase_query(metabase_handle, "SELECT * from severity", col_types = cols(.default = col_character()))
  diagnoses_meta_raw <- metabase_query(metabase_handle, "SELECT * from diagnoses", col_types = cols(.default = col_character()))
  diagnosislookup_meta_raw <- metabase_query(metabase_handle, "SELECT * from diagnosislookup", col_types = cols(.default = col_character()))
  symptoms_meta_raw <- metabase_query(metabase_handle, "SELECT * from symptoms", col_types = cols(.default = col_character()))
  symptomlookup_meta_raw <- metabase_query(metabase_handle, "SELECT * from symptomlookup", col_types = cols(.default = col_character()))
  
  diagnoses_meta <- 
    diagnoses_meta_raw %>% 
    left_join(diagnosislookup_meta_raw, by = "diagnosisid") %>% 
    select(-id, -diagnosisid) %>% 
    pivot_wider(names_from = diagnosisname, values_from = diagnosisvalue, names_prefix = "Dx") %>% 
    setNames(., str_to_lower( colnames(.))) %>% 
    clean_names()
  
  
  symptoms_meta <- 
    symptoms_meta_raw %>% 
    left_join(symptomlookup_meta_raw, by = "symptomid") %>% 
    select(-id, -symptomid) %>% 
    pivot_wider(names_from = symptomname, values_from = symptomvalue, names_prefix = "sym") %>%
    setNames(., str_to_lower( colnames(.))) %>% 
    rename(asymptomatic := symasymptomatic) %>% 
    clean_names()
  
  
  
  cases_meta_raw %>% count(phacid , sort = T)
  severity_meta_raw %>% count(phacid , sort = T)
  
  
  
  #cases_meta %>% count(deathcause, sort = T)
  
  cases_meta <- 
    cases_meta_raw %>%
    left_join(severity_meta_raw, by = "phacid") %>% #count(phacid , sort = T)
    left_join(symptoms_meta, by = "phacid") %>%
    left_join(diagnoses_meta, by = "phacid") %>%
    mutate_if(is.character, function(x){if_else(x == "not collected", "", x)})  %>% #count(disposition)
    mutate(disposition = if_else(disposition == "unknown", "", disposition))#  %>% count(disposition)
  #cases_meta <- metabase_query(metabase_handle, "SELECT phacid, pt, ptcaseid, sex from cases where sex != 'male' and sex != 'female' and sex != 'unknown'", col_types = cols(.default = col_character()))
  #cases_meta %>% count(sex, pt)
  
  
  # cases_meta %>% count(disposition, pt) %>%
  #   pivot_wider(names_from = "pt", values_from = "n") %>% 
  #   writexl::write_xlsx(x = . , path = file.path(meta_diff_dir, "meta_base_disposition.xlsx"))
  
  ms_access_con = get_covid_cases_db_con()
  cases_orig <- tbl(ms_access_con, "Case") %>% 
    collect()%>% 
    rename_all(tolower) %>% 
    mutate_if(is.character, str_to_lower)
  
  cases_missing_from_meta <- 
  anti_join(cases_orig %>% select(phacid, pt, ptcaseid, phacreporteddate, classification), cases_meta %>% select(pt, ptcaseid), by = c("pt", "ptcaseid"))
  
  cases_missing_from_access <- 
  anti_join(cases_meta %>% select(phacid, pt, ptcaseid, phacreporteddate, classification), cases_orig %>% select(pt, ptcaseid), by = c("pt", "ptcaseid"))
  
  
  writexl::write_xlsx(x = cases_missing_from_meta ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_cases_missing_from_meta.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
                      )
  
  
  
  writexl::write_xlsx(x = cases_missing_from_access ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_cases_missing_from_access.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
  )
  
  
  
  cols_missing_from_metabase <- setdiff(cases_orig %>% colnames(), cases_meta %>% colnames())
  writexl::write_xlsx(x = tibble(cols_missing_from_metabase) ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_cols_missing_from_metabase.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
  )
  
  cols_missing_from_access <- setdiff(cases_meta %>% colnames(), cases_orig %>% colnames())
  writexl::write_xlsx(x = tibble(cols_missing_from_metabase) ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_cols_missing_from_access.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
  )  

  
  
  common_cols  <- cases_orig %>% select(any_of(colnames(cases_meta))) %>% colnames() %>% sort()
  common_cols2  <- cases_meta %>% select(any_of(colnames(cases_orig)))%>% colnames()%>% sort()
  
  
  writexl::write_xlsx(x = tibble(common_cols) ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_Common_cols.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
  )  
  
  
  
  same_cases <- inner_join(cases_orig %>% select(common_cols), 
                           cases_meta %>% select(common_cols), 
                           by = c("pt", "ptcaseid"), 
                           suffix  = c("_orig", "_meta"))
  
  
  
  
  
  
  
  col_level_diff <- 
    map_dfr(setdiff(common_cols, c("phacid", "pt", "ptcaseid")), function(curr_col){
      print(curr_col)
      tibble(
        are_same = as.character(same_cases[[paste0(curr_col, "_orig")]])  == as.character(same_cases[[paste0(curr_col, "_meta")]]),
        colname = curr_col,
        phacid_orig = same_cases$phacid_orig,
        phacid_meta = same_cases$phacid_meta,
        ptcaseid = same_cases$ptcaseid,
        pt = same_cases$pt,
        value_orig = as.character( same_cases[[paste0(curr_col, "_orig")]] ) ,
        value_meta = as.character( same_cases[[paste0(curr_col, "_meta")]] )
      ) %>% filter(are_same == F)
    })  
  
  col_level_diff <- 
  col_level_diff %>% 
    mutate(value_orig = clean_str(value_orig),
           value_meta = clean_str(value_meta))
  writexl::write_xlsx(x = col_level_diff ,#%>% head(50000),
                      path = file.path(meta_diff_dir, paste0(format(Sys.Date() ,"%Y-%m-%d"), "_col_level_diff.xlsx" )),
                      col_names = TRUE,
                      format_headers = TRUE,
                      use_zip64 = FALSE
  )
  
  
  
  
  #END FUNCTION
}




backup_db <- function(full_fn = file.path(DIR_OF_DB_2_BACK_UP, NAME_DB_2_BACK_UP),
                      base_backup_location = DIR_OF_DB_2_BACK_UP_INTO #file.path(DIR_OF_DB_2_BACK_UP_INTO, "BACKUP")
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
  print(paste0("current time is ", Sys.time(), ", this can take 15 minutes with a slow network...."))
  ret_val <- 
    file.copy(full_fn, target_fn, overwrite = FALSE, recursive = FALSE,
              copy.mode = TRUE, copy.date = TRUE)#, showWarnings = TRUE)
  toc = Sys.time()
  
  if (!ret_val)
    stop("Did not backup the DB")
  
  
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



#
# con = get_covid_cases_db_con()
# df = get_flat_case_tbl(con = con) %>% filter(Classification %in% c("Confirmed", "Probable"))
# 
# 
# cols_to_check <- df %>% select_if(is.character) %>% colnames()
# 
# cols_to_check <- cols_to_check[!cols_to_check %in% c("PHACID", "PT", "PTCaseID", "PHACReportedDate")]
# 
# 
# ttfn <-
#   map_dfr(cols_to_check, function(curr_col){
#     print(curr_col)
#     tmp <- df %>%
#       rename(col_val := !!sym(curr_col)) %>%
#       count(col_val, sort = T) %>%
#       mutate(col_orig_val = col_val) %>%
#       mutate(col_val = clean_str(col_val))
# 
# 
#     tmp2 <- df %>%
#       rename(col_val := !!sym(curr_col)) %>%
#       mutate(col_val = clean_str(col_val)) %>%
#       count(col_val, sort = T)
# 
#     ret_val <-
#     full_join(tmp, tmp2, by = "col_val", suffix = c("_clean_first", "_clean_after")) %>%
#       mutate(col_name = curr_col) %>%
#       filter(n_clean_first != n_clean_after)
# 
#     Best_Values <-
#       ret_val %>%
#       group_by(col_val) %>%
#       arrange(col_val, desc(n_clean_first)) %>%
#       slice(which((n_clean_first == max(n_clean_first)))) %>%
#       rename(cleaned_value := col_val,
#              best_value := col_orig_val
#       ) %>% select(cleaned_value, best_value)
# 
#       # ungroup() %>%
#       # rename(new_value := col_val,
#       #        old_value := col_orig_val,
#       #        varName := col_name
#       # ) %>% select(varName, old_value, new_value)
# 
#     ret_val_2 <-
#       df %>%
#       rename(old_value := !!sym(curr_col)) %>%
#       mutate(cleaned_value = clean_str(old_value)) %>%
#       select(PHACReportedDate, PHACID, PTCaseID, PT, old_value, cleaned_value) %>%
#       inner_join(Best_Values, by = "cleaned_value") %>%
#       filter(best_value != old_value) %>%
#       mutate(varName = curr_col) %>%
#       mutate(Approved = "")  %>%
#       rename(new_value := best_value) %>%
#       selecextract_case_data_domestic_epit(Approved, PHACReportedDate, PHACID, PTCaseID, PT, varName, old_value,new_value)
# 
#     return(ret_val_2)
#   })
# 
# 
# ttfn %>% filter(old_value  !=  new_value )  %>% filter(new_value != "") %>% filter(!is.na(new_value)) %>%  writexl::write_xlsx(file.path("~", "..", "Desktop", "ttfn.xlsx"))

do_end_of_day_tasks <- function(){
  get_email_sentence()
  get_email_sentence_OLD()
  #backup_db()
  extract_case_db_errs()
  extract_case_data_get_StatsCan()
  extract_case_data_get_StatsCan_OLD()
  extract_case_data_static_file() 
  extract_case_data_static_file_web()
  
  extract_case_data_domestic_epi()
  
  extract_case_data_domestic_survelance()
  extract_case_data_domestic_survelance_OLD()
  extract_case_data_get_HCDaily() 
  extract_case_data_get_DataHub() 
  extract_case_data_get_DataHub_OLD() 
  extract_case_data_get_WHO()
  extract_case_data_get_WHO_OLD()
  #extract_case_data_get_completness_analysis()
  #extract_case_data_Count_Summary()
  
  extract_case_data_get_ijn()
  
  get_email_sentence()
  
  #extract_case_data_get_metabase_diff()
  # extract_positives_by_age_prov()
  # extract_epi_curves()
  # extract_percent_by_time()
  
}
do_end_of_day_tasks()

