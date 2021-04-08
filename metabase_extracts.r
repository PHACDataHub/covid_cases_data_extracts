

#rm(list=ls())
#gc()


library(tidyverse)
library(metabaser)
library(digest)
library(snakecase)
library(rio)
library(writexl)
library(readxl)
library(glue)
library(lubridate)
library(AzureStor)
library(haven)
library(arrow)
library(DBI)

CONFIG_FILE <- "metabase_extracts.xlsx"
META_BASE_HRE_URL <- "https://discover-metabase.hres.ca/api"
META_BASE_HRE_CASES_DB_ID <- 2
DIR_EXPORT_DEF <- file.path("~")


#holds a cache of data frames
CACHE_DFS <- list()




# metabase_set <-function(url, id){
#   Sys.setenv()
#   META_BASE_HRE_URL <<-url
#   META_BASE_HRE_CASES_DB_ID <<- id
# }


metabase_conn <- function(base_url = META_BASE_HRE_URL,
                             database_id = META_BASE_HRE_CASES_DB_ID,
                             username =  keyring::key_get("username_for_meta_base_on_HRE"),
                             password = keyring::key_get("password_for_meta_base_on_HRE")
                             ){
  #'
  #' easier metabase login
  #'
  metabase_login(base_url = base_url,
                 database_id = database_id,
                 username = username,
                 password = password)  
}



metabase_cache_clear <- function(){
  #' deletes cache for metabase
  CACHE_DFS <- list()
  gc()
}

save_azure <- function(df, 
                       full_fn, 
                       AZURE_KEY = keyring::key_get("DataHub"), 
                       saving_func = write_xlsx){
  #' 
  #' saves a dataframe to azure
  #' 
  #' 
  
  fn <- basename(full_fn)
  dir <- dirname(full_fn)
  
  
  #blob container
  blob_cont <- blob_container(endpoint = dir, key=AZURE_KEY)
  
  #save temp file
  saving_func(x = df,
              path = file.path(rappdirs::user_cache_dir(), fn),
              col_names = TRUE,
              format_headers = TRUE,
              use_zip64 = FALSE
  )
  
  
  #cpy to azure
  AzureStor::upload_blob(container = blob_cont, 
                         src = file.path(rappdirs::user_cache_dir(), fn), 
                         dest = paste0("data/",fn)
                         )
  
}


save_azure_modelling <- function(df, 
                       full_fn, 
                       AZURE_KEY = keyring::key_get("Modelling"), 
                       saving_func = write_csv){
  #' 
  #' saves a dataframe to azure
  #' 
  #' 
  
  fn <- basename(full_fn)
  dir <- dirname(full_fn)
  
  
  #blob container
  blob_cont <- blob_container(endpoint = dir, key=AZURE_KEY)
  
  #save temp file
  saving_func(x = df,
              path = file.path(rappdirs::user_cache_dir(), fn),
              col_names = TRUE,
              na = ""
  )
  
  
  #cpy to azure
  AzureStor::upload_blob(container = blob_cont, 
                         src = file.path(rappdirs::user_cache_dir(), fn), 
                         dest = paste0("data/",fn)
  )
  
}


save_csv <- function(df, full_fn){
  #'
  #'
  
  df %>% 
    write_csv(path = full_fn,
              col_names = TRUE,
              na = "")
  
}


save_sas7bdat <- function(df, full_fn, max_nchar_col_nm = 32){
  #'
  #'

  df %>% 
    rename_all(function(x){substr(x,1,max_nchar_col_nm)}) %>%
    haven::write_sas(data = . , path = full_fn)
}


save_feather <- function(df, full_fn){
  #'
  #'
  
  df %>% 
    arrow::write_feather(full_fn)
  
}


save_db <- function(df, full_fn){
  
  conn <- dbConnect(RSQLite::SQLite(), full_fn)
  dbWriteTable(conn, 
               "df" , 
               df, 
               overwrite =TRUE)
  
}

metabase_query_cache <- function(sql_str, 
                                 conn = metabase_conn(), 
                                 col_types = cols(.default = col_character()), ...){
  #'
  #' caches results from from metabase_query
  #'
  key <- digest(sql_str)
  df <- CACHE_DFS[[key]]
  if(!is.null(df)){
    message(glue("cache used for {sql_str}"))
    return(df)
  }
  tic <- Sys.time()
  df <- metabase_query(handle = conn, sql_str, col_types, ...)
  toc <- Sys.time()
  message(glue("took {toc-tic} to get {sql_str}"))
    
  CACHE_DFS[[key]] <<- df
  
  return(df)
}




metabase_extract <- function(sql_str = "select * from all_cases limit 100;",
                    dir = DIR_EXPORT_DEF,
                    suffix = format(Sys.Date()),
                    fn = glue("{substr(to_snake_case(sql_str),1,30)}_{suffix}.xlsx"),
                    full_fn = file.path(dir, fn),
                    adjust_func = do_nothing,
                    saving_func = write_xlsx#,
                    #saving_func_params = list(col_names = TRUE, format_headers = TRUE, use_zip64 = FALSE) 
                    ){
  #'
  #' do a single extract
  #' 
  
  basename(full_fn)
  tic <- Sys.time()
  message(glue("Beginning to extract '{basename(full_fn)}', current time is '{Sys.time()}'."))
  
  df_raw <-metabase_query_cache(sql_str = sql_str)
  df <- adjust_func(df_raw)
  #do.call(saving_func, c(list(x = df), path = full_fn))#, saving_func_params))
  saving_func(df, full_fn)
  toc <- Sys.time()
  
  message(glue("Finished extracting '{basename(full_fn)}', took {format(toc-tic)}."))
}



do_nothing <- function(df){
  #' takes a a variable does nothing are returns it
  return(df)
}



do_something <- function(df){
  #' takes a a variable does something are returns it
  df %>% clean_names()
}


make_data_hub <- function(df,
                          cols2keep = 
                            c( "episodedate"    ,      "classification" ,      "pt"       ,            "sexgender"     ,           
                           "agegroup10"   ,        "age"                 , "occupation"   ,       "occupationhcw" ,      
                           "ltc_resident"  ,       "asymptomatic2"        ,"onsetdate"     ,      "asymptomatic"   ,     
                           "symcough"       ,      "symfever"           ,  "symchills"       ,     "symsorethroat"   ,    
                           "symrunnynose"    ,     "symshortnessofbreath", "symnausea"        ,    "symheadache"      ,   
                           "symweakness"      ,    "sympain"              ,"symirritability"   ,   "symdiarrhea"       ,  
                           "symother"          ,   "symotherspec"         ,"hospstatus"         ,  "exposure_cat"      , 
                           "disposition"       ,  "resolutiondate"        ,"earliestdate", "earliestdatetype",
                           "last_refreshed" ), 
                          to_impute = c("onsetdate","episodedate"),   
                          to_impute_from = c("phacreporteddate", "reporteddate", "onsetdate", "earliestlabcollectiondate", "earliestlabtestresultdate", "episodedate", "earliestdate")
  )
  {
  #df <-  metabase_query_cache("select * from all_cases;")
  #a <- df %>% do_impute_dates(to_impute = to_impute, to_impute_from = to_impute_from) 
  df %>% 
    do_impute_dates(to_impute = to_impute, to_impute_from = to_impute_from) %>% 
    select(cols2keep, matches("_Imputed")) 
  
}








get_deltas_dates <- function(df,
                             colforcompare = "onsetdate",
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






#df <- metabase_query_cache("select * from all_cases limit 2000;")
make_imputed_date <- function(df,
                              date_col_nm,
                              to_impute_from,
                              suppress_out_of_range = TRUE
                              #date_col_nm_patern = "date"
){
  #df %>% impute_deltas() %>% view()
  df <- 
    df %>% 
    mutate_at(vars(to_impute_from), as.Date)
  
  
  to_impute_rng <- df[[date_col_nm]] %>% range(na.rm = T)
  
  cols_dts <- df %>% 
    select(to_impute_from) %>% colnames()
    #select_if(function(x){is(x, class2 = "POSIXct") | is.Date(x)}) %>% colnames()
  
  df3 <-
    bind_cols(df %>% select(!!sym(date_col_nm)),
              get_deltas_dates(df = df, colforcompare = date_col_nm, cols_dts = cols_dts))
  
  
  cols_to_use <- 
    map_dfc( df3 %>% select(starts_with(date_col_nm)) %>% select_if(is.numeric), function(x) {
      quantile(x, probs = c(0.95), na.rm = T) - quantile(x, probs = c(0.05), na.rm = T)
    }) %>% sort() %>% colnames()  
  
  #df[[date_col_nm]] <- as.Date(df[[date_col_nm]])
  x <- df3[[date_col_nm]]
  
  #x%>% is.na() %>% sum()
  for (c_nm in cols_to_use){
    col_2_nm <- gsub(paste0(date_col_nm, "_2_"), "",c_nm)
    x <-
      if_else(!is.na(x), x ,
              if_else(!is.na(df3[[c_nm]]), 
                      as.Date(df[[col_2_nm]] - df3[[c_nm]], origin="1970-01-01"),
                      as.Date(NA)
              )
      )  
    #print(x %>% is.na() %>% sum() )
    
  }
  
  # suppress dates that are out of the origional range
  if (suppress_out_of_range){
    x <- if_else(x < to_impute_rng[1] | x > to_impute_rng[2] , as.Date(NA), x)
  }
  
  return(x) 
}



#df2 <- metabase_query_cache("select * from all_cases")
#df2 %>% do_impute_dates()

do_impute_dates <- function(df2, 
                            to_impute = c("onsetdate","episodedate"),   
                            to_impute_from = c("phacreporteddate", "reporteddate", "onsetdate", "earliestlabcollectiondate", "earliestlabtestresultdate", "episodedate", "earliestdate")
                            ){
  #'
  #'  adds imputed dates to the dataframe
  #'  
  #df_dh <- metabase_query_cache("select * from data_hub limit 100")
  #df2 <- metabase_query_cache("select * from all_cases limit 100") 
  
  #df2 <- df
  pts <- df2 %>% distinct(pt) %>% pull()
  
  df2 <- 
    df2 %>% 
    mutate_at(vars(to_impute_from, to_impute), as.Date)
  
  imuted_dates <- 
    map_dfr(pts, function(curr_pt){
      
      df_for_impute <-
        df2 %>% 
        filter(pt == curr_pt) %>% 
        select(phacid,pt,  to_impute, to_impute_from)
      
      
      imuted_dates_pt <- 
        map_dfc(to_impute, function(c_nm){make_imputed_date(df = df_for_impute, date_col_nm = c_nm, to_impute_from = to_impute_from) }) %>% 
        set_names(to_impute) %>%
        rename_all(function(x){paste0(x, "_imputed")}) %>% 
        bind_cols(df_for_impute %>% select(phacid), .)
      
      print(curr_pt)
      print(ncol(imuted_dates_pt))
      return(imuted_dates_pt)
    })    
  
  
  df2 %>% 
    left_join(., imuted_dates, by = "phacid")
  
  
}







func_from_string <- function(str_func, default_func = do_nothing){
  #' 
  #' 
  #' takes a string and looks up a function that matches
  #' 
  #' 
  if (is.na(str_func)) {
    return(default_func)
  }
  
  if (nchar(str_func) == 0){
    return(default_func)
  }
  
  
  #tmp_func <- mget(x = str_func, ifnotfound = list(default_func))
  #tmp_func <- tmp_func[[1]]
  obj_found <- getAnywhere(str_func)
  if (length(obj_found$objs) >= 1){
    return(obj_found$objs[[1]])
  }else
  {
    message(glue("the function {str_func} matched {length(obj_found$objs)} results returning default {default_func}" ))
    return(default_func)
  }
  return(default_func)
}



remove_pt_cols <- function(df){
  #'
  #'  removes pt col
  #'  adjust date and number variable formats
  #'  
  df %>% select(-pt) %>%
    mutate(across(.cols = c(episodedate, onsetdate2, resolutiondate2, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age), .fns = as.integer))
}





keep_only_trend_epi_cols <- function(df){
  #'
  #'  keep only trend epi cols
  #'  adjust date and number variable formats
  #'   TODO:  labspecimencollectiondate is not in it fix when it is 
  #'  
  df %>% select(phacid, phacreporteddate, episodedate, pt, age_years, agegroup10, agegroup20, onsetdate, earliestlabcollectiondate, sex, gender, sexgender, coviddeath, hosp, icu, exposure_cat, earliestdate, earliestdatetype) %>%
    mutate(across(.cols = c(phacreporteddate, episodedate, onsetdate, earliestlabcollectiondate, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age_years), .fns = as.integer))
}

keep_only_trendvoc_epi_cols <- function(df){
  #'
  #'  keep only trend epi cols
  #'  adjust date and number variable formats
  #'   TODO:  labspecimencollectiondate is not in it fix when it is 
  #'  
  df %>% select(phacid, phacreporteddate, episodedate, pt, age_years, agegroup10, agegroup20, onsetdate, earliestlabcollectiondate, sex, gender, sexgender, coviddeath, hosp, icu, exposure_cat, earliestdate, earliestdatetype, variantid, variantidentified, variantscreenresult, variantsequenceresult) %>%
    mutate(across(.cols = c(phacreporteddate, episodedate, onsetdate, earliestlabcollectiondate, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age_years), .fns = as.integer))
}


keep_only_web_epi_cols <- function(df){
  #'
  #'  keep only web epi cols
  #'  adjust date and number variable formats
  #'   
  #'  
  df %>% select(phacid, episodedate, episodetype, sex_excunk, exposure_cat, agegroup10, mechanicalvent, hospstatus, coviddeath, pt, earliestdate, earliestdatetype, last_refreshed) %>%
    mutate(across(.cols = c(episodedate, earliestdate), .fns = as.Date))
}



keep_only_weekly_report_cols <- function(df){
  #'
  #'  keep only weekly report cols
  #'  adjust date and number variable formats
  #'  
  #'  
  df %>% select(phacid, ptcaseid, phacreporteddate, classification, pt, episodedate, earliestdate, earliestdatetype, age, ageunit, agegroup10, agegroup20, sexgender, exposure_cat, exposurecountry, hospstatus, coviddeath, disposition, last_refreshed) %>%
    mutate(across(.cols = c(phacreporteddate, episodedate, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age), .fns = as.integer))
}



who_cols_formatting <- function(df){
  #'
  #'  adjust date and number variable formats
  #'  
  #'  
  #'  
  df %>% mutate(across(.cols = c(report_date,	report_pointofentry_date,	patcourse_dateonset,	patcourse_dateonset_unk,	patcourse_preshcf,	patcourse_dateiso,	patcourse_datedeath,
                                 expo_travel_date1,	expo_travel_date2,	expo_travel_date3,	expo_case_date_first1,	expo_case_date_last1,	expo_case_date_first2,	expo_case_date_last2,	
                                 expo_case_date_first3,	expo_case_date_last3,	expo_case_date_first4,	expo_case_date_last4,	expo_case_date_first5,	expo_case_date_last5, lab_date1, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(patinfo_ageonset), .fns = as.integer))
}



dashboard_and_epi_cols_formatting <- function(df){
  #'
  #'  adjust date and number variable formats
  #'  
  #'  
  #'  
  df %>%
    mutate(across(.cols = c(phacreporteddate, reporteddate,	dateofentry,	onsetdate,	episodedate,	earliestdate, hospstartdate,	hospenddate,	icustartdate,	icuenddate,	
                            isolationstartdate,	isolationenddate,	ventstartdate,	ventenddate,	resolutiondate,	deathdate,	earliestlabtestresultdate,	
                            earliestlabcollectiondate,	earliestfirstcontactdate,	latestcontactdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(phacreporteddate_epiweek, onsetdate_epiweek,	episodedate_epiweek, labtestresultdate_epiweek,labspecimencollectiondate_epiweek), .fns = as.numeric)) %>%
    mutate(across(.cols = c(age, age_years, exposure_linked, numberofcontacts, rfcount, comp_riskfactors, symcount), .fns = as.integer))
}



statcan_cols_formatting <- function(df){
  #'
  #'  adjust date and number variable formats
  #'  
  #'  
  #'  
  df %>%
    mutate(across(.cols = c(episodedate, onsetdate2, resolutiondate2, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age), .fns = as.integer))
}



modelling_cols_formatting <- function(df){
  #'
  #'  adjust date and number variable formats
  #'  
  #'  
  #'  
  df %>% mutate(across(.cols = c(reporteddate, onsetdate, earliestlabcollectiondate, earliestlabtestresultdate, phacreporteddate, hospstartdate, hospenddate, dateofentry, icustartdate, 
                                 icuenddate, isolationstartdate, isolationenddate, ventstartdate, ventenddate, resolutiondate, deathdate, earliestfirstcontactdate, earliestdate), .fns = as.Date)) %>%
    mutate(across(.cols = c(age, numberofcontacts), .fns = as.integer))
}


########################################
# 
clean_str <- function(x, 
                      do_pattern = T,
                      pattern = "[[:punct:]]", 
                      replacement = " ", 
                      NA_replace = "", 
                      BLANK_replace = "",
                      NA_STR = "na",
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







count_summary <- function(df, 
                         strats = c("pt", "agegroup10"),
                         MAX_NUM_unique_values_ALLOWED = 60){
  
  
  a_tbl <- df
  
  a_tbl_mn <- 
    a_tbl %>% 
    select(matches("date")) %>% #mutate(detectedatentry = as.Date(detectedatentry))
    select(-matches("week")) %>%
    select(-matches("earliestdatetype"))%>%
    select(-matches("detectedatentry")) %>%
    mutate_all(as.Date, tryFormats = "%Y-%m-%d") %>% 
    #select_if(is.Date) %>%
    mutate_all(lubridate::month) %>% 
    rename_all(paste0, "_month")
  
  a_tbl_wk <- 
    a_tbl %>% 
    select(matches("date")) %>%
    select(-matches("week")) %>%
    select(-matches("earliestdatetype"))%>%
    select(-matches("detectedatentry")) %>%
    mutate_all(as.Date, tryFormats = "%Y-%m-%d") %>% 
    select_if(is.Date) %>%
    mutate_all(lubridate::week) %>% 
    rename_all(paste0, "_week")
  
  a_tbl_dow <- 
    a_tbl %>% 
    select(matches("date")) %>%
    select(-matches("week")) %>%
    select(-matches("earliestdatetype"))%>%
    select(-matches("detectedatentry")) %>%
    mutate_all(as.Date, tryFormats = "%Y-%m-%d") %>% 
    select_if(is.Date) %>%
    mutate_all(lubridate::wday) %>% 
    rename_all(paste0, "_dow")
  
  
  new_tbl <- bind_cols( a_tbl %>% select(! matches("date")) , a_tbl_mn, a_tbl_wk, a_tbl_dow) 
  
  
  
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
        mutate(value = clean_str(!!sym(cnm), 
                                 BLANK_replace = "NULL or BLANK STRING", 
                                 NA_replace = "NULL or BLANK STRING")) %>% 
        group_by_at(c("value", strats)) %>% 
        summarise(n = n()) %>%
        ungroup()
      
      
      
      uvals <- 
        if(length(uvals$value %>% unique()) > MAX_NUM_unique_values_ALLOWED){
          return(NULL)
          # uvals <- uvals %>% arrange(desc(n)) %>% group_by(pt) %>%  
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
          
          
        }) %>% reduce(inner_join)#, by = c("value", "n", strats))
      
      uvals2 <- inner_join(uvals, uvals_by_strat, by = c("value", strats))
      
      
      
      
      uvals2 %>% 
        mutate(name = cnm,
               value = as.character(value)) %>%
        return()
    })
  
  return(ret_val)
  
  
  
}




extract_should_write <- function(instr){
  #' Returns true if we should write the extract today
  #' False if we shouldn't
  #' 
  
  instr %>% 
    pivot_longer(., colnames(.)) %>% 
    mutate(value = clean_str(value)) %>% 
    filter(name == as.character( lubridate::wday(Sys.Date(), label = T))) %>%
    pull(value) %>% 
    trimws() %>% nchar() > 0
  
}





extracts <- function(confic_fn = CONFIG_FILE){
  #' read in instrutions 
  #' read stuff from metabase
  #' adjust it 
  #' save it
  
  instruct <- read_xlsx(confic_fn)
  
  for (i in 1:nrow(instruct)){
    
    print(glue("index {i}"))
    
    if( ! extract_should_write(instr = instruct[i,]) ){
      nm <- instruct[[i, "nm"]]
      message(glue("Not writing extract i={i} nm={nm} today."))
      next
    }
    sql_str <- instruct[[i, "sql_str"]]
    
    suffix <- 
    tryCatch(eval(parse(text = instruct[[i, "suffix"]])),
      error=function(cond) {
        message("failed to run suffix")
        message(cond)
        return(glue("suffix"))
      }
    ) 
    

    fn <- if(! is.na(glue(instruct[[i, "fn"]]))) glue(instruct[[i, "fn"]]) else glue("{substr(to_snake_case(sql_str),1,30)}_{suffix}.xlsx")
    dir <- if(! is.na(instruct[[i, "dir"]])) instruct[[i, "dir"]] else DIR_EXPORT_DEF
      
    
    adjust_func <- func_from_string(str_func = instruct[[i, "adjust_func"]] , default_func = do_nothing)
    
    
    
    
    
    saving_func <- func_from_string(str_func = instruct[[i, "saving_func"]] , default_func = write_xlsx)

    
    metabase_extract(
            sql_str = sql_str,
            dir = instruct[[i, "dir"]],
            suffix = suffix,
            fn = fn,
            adjust_func = adjust_func,
            saving_func = saving_func#,
            #saving_func_params = list(col_names = TRUE, format_headers = TRUE, use_zip64 = FALSE) 
    )
  
    
  }
}









######################################
#
get_db_error_report_by_case_error_Residency_canadian<- function(a_tbl){
  a_tbl %>% 
    filter(residency == "" & nchar(residencecountry) > 0) %>% 
    select(phacid) %>% 
    mutate(err = "Residency is unspecified, yet ResidenceCountry is not blank.") %>% 
    mutate(typ = "Residency is unspecified, yet ResidenceCountry is not blank." )
}
get_db_error_report_by_case_error_Residency_canadian_but_not<- function(a_tbl){
  a_tbl %>% 
    filter(residency == "canadian resident" & !(residencecountry=="canada")) %>% 
    select(phacid) %>% 
    mutate(err = "Residency is Canadian, yet ResidenceCountry is not.") %>% 
    mutate(typ = "Residency is Canadian, yet ResidenceCountry is not." )
}
get_db_error_report_by_case_error_Age_Age_group<- function(a_tbl){
  a_tbl %>% 
    filter(!is.na(age) & (agegroup10 == "Unknown" | agegroup20 == "Unknown" )) %>% 
    select(phacid) %>% 
    mutate(err = "Exact Age is given but Agegroup is listed as unkown.")%>% 
    mutate(typ = "Age group unkown, but known age" )
}
get_db_error_report_by_case_error_Age_Age_units<- function(a_tbl){
  a_tbl %>% 
    filter(!is.na(age) & is.na(ageunit)) %>% 
    select(phacid) %>% 
    mutate(err = "Exact Age is given but Age units are not.") %>% 
    mutate(typ = "No Age units" )
}


get_db_error_report_by_case_Sym<- function(a_tbl){
  a_tbl %>% 
    filter(asymptomatic == "yes" ) %>% 
    select(phacid, asymptomatic, matches("^sym")) %>% 
    collect() %>% 
    pivot_longer(matches("^sym"), values_drop_na = T) %>% 
    #filter(! is.na(value)) %>% 
    filter(value != "" & value != ".", value != "no" & value != "unknown") %>% 
    mutate(err = paste0("",name,"=", value, "") ) %>% 
    group_by(phacid) %>% 
    summarise(err = paste0(err, collapse = " + ") ) %>% 
    mutate(err = paste("Asymptomatic is 'yes' but",  err, sep = " ") ) %>% 
    select(phacid, err) %>% 
    mutate(typ = "Asymptomatic but not" )
}

get_db_error_report_by_case_Hosp<- function(a_tbl){
  a_tbl %>% 
    select(phacid, hosp, hospstartdate, hospenddate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!hosp == "yes") %>% 
    filter(! is.na(hospstartdate) | ! is.na(hospenddate)) %>% 
    select(phacid) %>% 
    mutate(err = paste0("A hospital date is listed but Hosp is not Yes") ) %>% 
    select(phacid, err) %>% collect() %>% 
    mutate(typ = "A hospital date is listed but Hosp is not Yes" )
}

get_db_error_report_by_case_ICU<- function(a_tbl){
  a_tbl %>% 
    select(phacid, icu, icustartdate, icuenddate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(icu) == "yes") %>% 
    filter(! is.na(icustartdate) | ! is.na(icuenddate)) %>% 
    select(phacid) %>% 
    mutate(err = paste0("A ICU date is listed but ICU is not Yes") ) %>% 
    select(phacid, err) %>% collect() %>%
    mutate(typ = "ICU" )
}


get_db_error_report_by_case_Isolation<- function(a_tbl){
  a_tbl %>% 
    select(phacid, isolation, isolationstartdate, isolationenddate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(isolation) == "yes") %>% 
    filter(! is.na(isolationstartdate) | ! is.na(isolationenddate) ) %>% 
    select(phacid) %>% 
    mutate(err = paste0("A Isolation date is listed but Isolation is not Yes") ) %>% 
    select(phacid, err) %>% collect() %>% 
    mutate(typ = "Isolation" )
}


get_db_error_report_by_case_vent<- function(a_tbl){
  a_tbl %>% 
    filter(!tolower(mechanicalvent) == "yes") %>% 
    filter(! is.na(ventstartdate) | ! is.na(ventenddate) ) %>% 
    select(phacid) %>% 
    collect() %>% 
    mutate(err = paste0("A vent date is listed but MechanicalVent is not Yes") ) %>% 
    select(phacid, err) %>% 
    mutate(typ = "vent" )
}
get_db_error_report_by_case_resolved<- function(a_tbl){
  a_tbl %>% 
    select(phacid, disposition, resolutiondate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(disposition) == "resolved") %>% 
    filter(! is.na(resolutiondate) ) %>% 
    select(phacid, resolutiondate, disposition) %>% 
    mutate(err = paste0("Resolution date of '",resolutiondate,"' is listed but disposition is '", disposition, "'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Resolution date listed but not resolved" )
}
get_db_error_report_by_case_Death<- function(a_tbl ){
  a_tbl %>% 
    select(phacid, disposition, deathdate) %>% 
    #filter(tolower(Hosp) == "no" | tolower(Hosp) == "unknown" | Hosp == "") %>% 
    filter(!tolower(disposition) == "deceased") %>% 
    filter(! is.na(deathdate) ) %>% 
    select(phacid, deathdate, disposition) %>% 
    mutate(err = paste0("Death date of '",deathdate,"' is listed but disposition is '", disposition, "'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Death date listed but not dead." )
}



get_db_error_report_by_case_travel<- function(a_tbl ){
  tmp_df <- 
    a_tbl %>% 
    filter(travel != "yes") %>% 
    select(phacid, matches("^travel")) %>% collect() 
  
  tmp_df %>% 
    mutate_all(as.character) %>% 
    pivot_longer(matches("^travel")) %>% 
    filter(name != "travel") %>% 
    filter(!is.na(value)) %>% 
    filter(value != "") %>% 
    mutate(err = paste0("'",name,"' is '", value, "'") ) %>% 
    select(phacid, err)  %>%
    group_by(phacid) %>%
    summarise(err = paste0(err, collapse = " and ")) %>% 
    mutate(err = paste0("travel is not yes but ", err) ) %>% 
    mutate(typ = "Travel" )
}




get_db_error_report_by_case_close<- function(a_tbl ){
  tmp_df <- 
    a_tbl %>% 
    filter(closecontactcase == "yes") %>% 
    select(phacid, matches("^closecontactcase"))
  
  tmp_df %>% 
    mutate_all(as.character) %>% 
    pivot_longer(matches("^closecontactcase")) %>% 
    filter(name != "closecontactcase") %>% 
    filter(!is.na(value) &  value != "") %>% 
    mutate(err = paste0("'",name,"' is '", value, "'") ) %>% 
    select(phacid, err)  %>%
    group_by(phacid) %>%
    summarise(err = paste0(err, collapse = " and ")) %>% 
    mutate(err = paste0("CloseContactCase is not yes but ", err) ) %>% 
    mutate(typ = "Close Contact" )
}

#get_db_error_report_by_case_date_diff <- function()




get_db_error_report_by_case_Dead_and_alive<- function(a_tbl ){
  #' Returns error report related to cases both dead and alive
  #' 
  a_tbl %>% 
    filter( (! is.na(deathdate)) &  (! is.na(resolutiondate)) ) %>% 
    select(phacid, deathdate, resolutiondate) %>% 
    mutate(err = paste0("Both Death date '",deathdate,"' and resolution date '",resolutiondate,"' listed.") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Both Death date and resolution date listed." )
}

get_db_error_report_by_case_dead_before_onset<- function(a_tbl ){
  a_tbl %>% 
    filter( (! is.na(deathdate)) &  (! is.na(onsetdate)) ) %>% 
    filter( deathdate < onsetdate) %>% 
    select(phacid, deathdate, onsetdate) %>% 
    mutate(err = paste0("Dead before Onset '",deathdate,"' < '",onsetdate,"'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Early Dead" )
}


get_db_error_report_by_case_resolved_before_onset<- function(a_tbl ){
  a_tbl %>% 
    filter( (! is.na(resolutiondate)) &  (! is.na(onsetdate)) ) %>% 
    filter( resolutiondate < onsetdate) %>% 
    select(phacid, resolutiondate, onsetdate) %>% 
    mutate(err = paste0("Resolved before Onset '",resolutiondate,"' < '",onsetdate,"'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Early resolution" )
}


get_db_error_report_by_case_teasting_CloseContact_disagree<- function(a_tbl ){
  #' Generates Error List on DB
  a_tbl %>% 
    filter( grepl("contact", testingreason) &  (closecontactcase !=  "yes")) %>% 
    filter( testingreason == "contact of a case" &  (closecontactcase !=  "yes")) %>% 
    select(phacid, testingreason, closecontactcase) %>% 
    mutate(err = paste0("TestingReason '",testingreason,"' disagrees with  CloseContactCase '",closecontactcase,"'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Contact Testing Reason" )
}

get_db_error_report_by_case_coviddeath_disposition_disagree<- function(a_tbl ){
  #' Generates Error List on DB
  a_tbl %>% 
    select(phacid, coviddeath, disposition) %>% 
    #mutate(coviddeath = clean_str(coviddeath), 
    #       disposition = clean_str(disposition)) %>% #count(coviddeath)
    filter(coviddeath == "yes" & disposition != "deceased") %>% 
    mutate(err = paste0("coviddeath '",coviddeath,"' disagrees with  disposition '",disposition,"'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Covid Death but not Deceased" )
}


get_db_error_report_by_case_PHAC_report_before_episode<- function(a_tbl ){
  a_tbl %>% 
    #mutate(., episodedate = make_episodedate(.)) %>% 
    #mutate(., EpisodeType = make_EpisodeType(.)) %>% 
    filter( (! is.na(episodedate)) &  (! is.na(phacreporteddate)) ) %>% 
    filter( phacreporteddate < episodedate) %>% 
    mutate(phacreporteddate = as.Date(phacreporteddate)) %>% 
    mutate(episodedate = as.Date(episodedate)) %>% 
    mutate(delta_days = phacreporteddate - episodedate) %>% 
    select(phacid, phacreporteddate, episodedate, episodetype, delta_days, classification) %>% 
    mutate(err = paste0("PHACReported before episodedate '",phacreporteddate,"' < '",episodedate,"'") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Early PHACReported" )
}


get_db_error_report_by_case_future_dates<- function(a_tbl ){
  
  tmp <- 
    a_tbl %>% 
    select(matches("date")) %>%
    select(-matches("week")) %>%
    select(-matches("earliestdatetype"))%>%
    select(-matches("detectedatentry")) %>%
    mutate_all(as.Date) %>% 
    select_if(is.Date) 
  
  col_nms <- colnames(tmp)
  #select(phacreporteddate, ReportedDate) %>% 
  #mutate(a = phacreporteddate > Sys.Date()) %>% select(a) %>% table()
  tmp %>%   mutate_all(function(x){x > Sys.Date()}) %>%
    bind_cols(a_tbl %>% select(phacid), .) %>% 
    pivot_longer(cols = matches("Date"), values_drop_n = T) %>% 
    filter(value == T) %>% 
    mutate(err = paste0(name , " is in the future? spooky!") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Future Date" )
  
}

get_db_error_report_by_case_2020_dates<- function(a_tbl ){
  
  tmp <- 
    a_tbl %>% 
    select(matches("date")) %>%
    select(-matches("week")) %>%
    select(-matches("earliestdatetype"))%>%
    select(-matches("detectedatentry")) %>%
    mutate_all(as.Date) %>% 
    select_if(is.Date) 
  
  col_nms <- colnames(tmp)
  #select(phacreporteddate, ReportedDate) %>% 
  #mutate(a = phacreporteddate > Sys.Date()) %>% select(a) %>% table()
  tmp %>%   mutate_all(function(x){x < as.Date("2020-01-01")}) %>%
    bind_cols(a_tbl %>% select(phacid), .) %>% 
    pivot_longer(cols = matches("Date"), values_drop_n = T) %>% 
    filter(value == T) %>% 
    mutate(err = paste0(name , " is before 2020-01-01") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Before 2020-01-01 Date" )
  
}



get_db_error_report_by_case_still_sick<- function(a_tbl ){
  a_tbl %>% 
    select(classification, disposition, phacid, onsetdate, reporteddate) %>% 
    filter(classification %in% c("confirmed", "probable")) %>%
    filter(! disposition %in% c("deceased", "resolved")) %>%
    mutate(onsetdate = as.Date(onsetdate)) %>% 
    filter(onsetdate <= Sys.Date() - 30*5) %>% 
    mutate(err = paste0("Onset Date ",onsetdate," is ",Sys.Date()- onsetdate," days ago....Thats a lot, maybe verify this with the pt?") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Sick Long time" )
}

get_db_error_report_by_case_still_sick_reported<- function(a_tbl ){
  a_tbl %>% 
    select(classification, disposition, phacid, onsetdate, reporteddate) %>% 
    filter(classification %in% c("confirmed", "probable")) %>%
    filter(! disposition %in% c("deceased", "resolved")) %>%
    mutate(reporteddate = as.Date(reporteddate)) %>% 
    
    filter(reporteddate <= Sys.Date() - 30*5) %>% 
    mutate(err = paste0("reporteddate ",reporteddate," is ",Sys.Date()- reporteddate," days ago....Thats a lot, maybe verify this with the pt?") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "Reported Long time ago." )
}


get_db_error_report_by_age_bad<- function(a_tbl ){
  a_tbl %>% 
    select(age, phacid) %>%
    mutate(age = as.integer(age)) %>% 
    filter(age < 0  | age > 113) %>%
    mutate(err = paste0("age  ",age," is  wrong") ) %>% 
    select(phacid, err)  %>% collect() %>% 
    mutate(typ = "age out of range" )
}






###########################################
#
db_error_report_by_case <- function(a_tbl){
  
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
      get_db_error_report_by_case_resolved(a_tbl = a_tbl),
      get_db_error_report_by_case_Death(a_tbl = a_tbl),
      get_db_error_report_by_case_Dead_and_alive(a_tbl = a_tbl),
      get_db_error_report_by_case_dead_before_onset(a_tbl = a_tbl),
      get_db_error_report_by_case_resolved_before_onset(a_tbl = a_tbl),
      get_db_error_report_by_case_travel(a_tbl = a_tbl),
      get_db_error_report_by_case_teasting_CloseContact_disagree(a_tbl = a_tbl),
      get_db_error_report_by_case_coviddeath_disposition_disagree(a_tbl = a_tbl),
      get_db_error_report_by_case_PHAC_report_before_episode(a_tbl = a_tbl),
      get_db_error_report_by_case_future_dates(a_tbl = a_tbl),
      get_db_error_report_by_case_still_sick(a_tbl = a_tbl),
      get_db_error_report_by_case_still_sick_reported(a_tbl = a_tbl),
      get_db_error_report_by_age_bad(a_tbl = a_tbl),
      get_db_error_report_by_case_2020_dates(a_tbl = a_tbl)
    ) %>% arrange(phacid)
  
  ids <- unique(errs$phacid)
  
  ret_val <-   
    a_tbl %>% 
    filter(phacid %in% ids) %>% 
    select(phacid, pt,    ptcaseid, classification, phacreporteddate ) %>% 
    left_join(errs, by = "phacid") %>% 
    arrange(desc(phacreporteddate), phacid, typ) %>% 
    select(phacid, pt, ptcaseid, typ, classification,phacreporteddate , err)
  
  return(ret_val)
}








  
