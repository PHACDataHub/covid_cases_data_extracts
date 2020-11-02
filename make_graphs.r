
library(binom)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(zoo)
library(stringr)
library(ggrepel)

theme_set(theme_bw())

get_lbl <- function(str, lang = "en"){
  
  tmp <- get_reports_lbls() %>% 
    filter(token == str) %>% pull(lang)
  
  if (length(tmp) == 0)
    return(str_to_upper( str))
  else
    return(str_to_title( tmp))
}

# plot_epi_curve(col_col_nm = "AgeGroup20", dt_col = "ReportedDate")
# plot_epi_curve()


####################################
#
get_db_counts <- function(con = get_covid_cases_db_con(), 
                          a_tbl = get_flat_case_tbl(con = con),
                          colnm = "Asymptomatic", 
                          agegrp = "AgeGroup20" ,
                          NA_replace = ""){
  
 
  a_tbl[colnm] <- if_else(is.na(a_tbl[[colnm]]),NA_replace, a_tbl[[colnm]] ) #%>% table()
  
  a_tbl %>% 
    #replace_na(list(colnm = NA_replace)) %>%
    select(colnm, "PT", agegrp) %>% 
    group_by(!!sym(colnm), !!sym("PT"), !!sym(agegrp)) %>% 
    summarise(n = n()) %>% 
    collect() %>% 
    ungroup() %>%
    separate(!!sym(agegrp), into = "min_age", remove = F) %>%
    mutate(min_age = as.integer(min_age)) %>%
    mutate(age_group = fct_reorder(!!sym(agegrp), min_age)) %>% 
    rename(nm := !!sym(colnm)) %>% 
    clean_names() %>% 
    select(pt, age_group, nm, n)
  
}

#a_tbl$Disposition
#dt_col = "ReportedDate"
#col_col_nm = "Hosp"
#col_col_nm = "AgeGroup10"
#col_col_nm = "Asymptomatic"
#col_col_nm = "Ethnicity"
#a_tbl <- a_tbl %>% filter(PT != "QC")
#a_tbl <- a_tbl %>% mutate(AgeGroup10 = as.ordered(AgeGroup10))
#COL_NM = c("COVIDDeath","SymShortnessofBreath","Asymptomatic", "Indigenous", "Hosp", "ICU")
plot_epi_curve <- function(con = get_covid_cases_db_con(), 
                           a_tbl = get_case_data_domestic_epi(con = con), #get_flat_case_tbl(con = con),
                           classifications_allowed = c("probable", "confirmed"),
                           dt_col = "onsetdate_imputed",
                           col_col_nm = "exposure_cat",
                           facet_col_nm = "pt",
                           clr = "blue",
                           early_dt =Sys.Date() %m-% months(6),
                           rolling_average = 7,
                           fog_days = 14,
                           Percent_fill = F,
                           MIN_LUMP_PROP = 0.02
)
{
  #' returng ggPlot object of the epi curve with a rolling average, it is also generally segmented by a column
  
  ln_lbl <- paste0("Rolling Average (", rolling_average, " days)")
  y_lbl <- if(Percent_fill) "Percent of cases and normalized Count" else "Number of new cases"

  #a_tbl %>% count(PT)
  
  foggy <- 
  a_tbl %>% 
    rename(facet_col := facet_col_nm) %>% 
    mutate(facet_col = fct_lump_prop(facet_col , prop = MIN_LUMP_PROP)) %>% 
    group_by(facet_col) %>% 
    summarise(base_fog = max(phacreporteddate, na.rm = T)) %>% 
    # {if(facet_col_nm == "PHACReportedDate") base_fog 
    #   else if (facet_col_nm == "ReportedDate") base_fog - 3
    #   
    #   }
    # {if(facet_col_nm == "PHACReportedDate") base_fog}
    group_by(facet_col) %>% 
    mutate(real_fog = as.Date(ifelse(dt_col == "PHACReportedDate", base_fog ,
           ifelse(dt_col == "ReportedDate", base_fog - 3 , 
           ifelse(dt_col == "OnsetDate",base_fog - 14, base_fog - 14
           )))))
  
  df_p <- 
  a_tbl %>% 
    filter(classification %in% classifications_allowed) %>% 
    select(phacid, dt_col, col_col_nm, facet_col_nm) %>%
    rename(date := dt_col, col_col := col_col_nm, facet_col := facet_col_nm) %>% #pull(date) %>% as.Date()
    filter(!is.na(date)) %>% 
    mutate(facet_col = fct_lump_prop(facet_col , prop = MIN_LUMP_PROP)) %>% 
    mutate(date = as.Date(date)) %>% 
    mutate(col_col = clean_str(x = col_col, BLANK_replace = "Unknown")) %>% #count(col_col) %>% pull(n) %>% sum()
    count(date, col_col, facet_col) %>% 
    
    #filter(!is.na(col_col)) %>%
    #filter(!is.na(facet_col)) %>%
    mutate(tot = sum(n)) %>% 
    group_by(col_col) %>% 
    mutate(col_percent = 100*sum(n)/ tot) %>% 
    mutate(col_lbl = paste0(col_col, " (", format(col_percent, digits = 2), "%)")) %>% 
    mutate(col_lbl = fct_reorder(col_lbl, col_percent)) %>% 
    ungroup() %>%
    group_by(facet_col) %>%
    mutate(facet_percent = 100*sum(n)/ tot) %>% 
    mutate(facet_max = max(n)) %>% 
    mutate(facet_total = sum(n)) %>% 
    mutate(facet_lbl = paste0(facet_col, "\n", 
                              format(facet_percent , digits = 2),
                              "%\nPeak:",format(facet_max , big.mark = ","),
                              "\nTotal:",format(facet_total, big.mark = ","))) %>% 
    mutate(facet_lbl = fct_reorder(facet_lbl, facet_percent)) %>% 
    
    ungroup() %>% 
    #mutate(col_col = clean_str(col_col, BLANK_replace = "Unknown")) 
    arrange(date) 
  
  
  
  
  #%>% # pull(n) %>% sum()
    #group_by(ID) %>% 
    #mutate(roll_mean = rollmean(n, rolling_average, na.pad = T, align = "right")) %>% 
    #mutate(is_weekend =  if_else(wday(date, label = TRUE) %in% c("Sun","Sat"), "weekend", "weekday") )%>%   
    #filter(date >= early_dt) #%>% 
    # mutate(ln_lbl = ln_lbl,
    #        bar_lbl = "n")
    # 

  
  
    
  df_pl <- 
    df_p %>% 
    filter(! is.na(date)) %>% 
    group_by(date, facet_lbl) %>% 
    summarise(n = sum(n)) %>% 
    ungroup() %>% 
    group_by(facet_lbl) %>% 
    arrange(date) %>% 
    #mutate(roll_mean=fnrollmean(n))
    mutate(roll_mean = rollmean(n, rolling_average, na.pad = T, align = "right")) %>%
    mutate(graph_type = "Numbers") %>% 
    ungroup()
  #df_pl$roll_mean
  #df_pl %>% view()
  df_pl <- 
  if(Percent_fill){
    df_pl %>% 
      group_by(facet_lbl) %>% 
      mutate(roll_mean_max =  max(roll_mean, na.rm = T)) %>%
      mutate(n_max =  max(n, na.rm = T)) %>%
      ungroup() %>%
      mutate(roll_mean = roll_mean/roll_mean_max)%>%
      mutate(n = n/n_max)# %>% group_by(facet_lbl) %>% slice(which.max(roll_mean)) 
  }else{
    df_pl
  }
  
  #df_pl  %>% group_by(facet_lbl) %>% slice(which.max(roll_mean)) 
  #df_pl  %>% group_by(facet_lbl) %>% count(date, sort = T)
  
  df_p <-
    if(Percent_fill){
      df_p %>% 
        group_by(date, facet_lbl) %>% 
        mutate(n_t =  sum(n)) %>%
        ungroup() %>%
        mutate(n = n/n_t)
    }else{
      df_p
    }

  df_p <- df_p %>%     mutate(graph_type = "Fraction")  
  
  df_p <- df_p %>% filter(date >= early_dt) 
  df_pl <- df_pl %>% filter(date >= early_dt) 
  
  ttl_lbl <- paste0(y_lbl," in Canada vs. ",get_lbl(dt_col)," \n", get_lbl(col_col_nm))
  sub_ttl_lbl <- paste0(min(df_p$date), " to ",  max(df_p$date))
  
  
  max_dt <- df_pl$date %>% max(na.rm = T)
  
  
    
  df_ribbon <- 
  foggy %>% filter(!is.na(facet_col)) %>% pull(facet_col) %>% 
  lapply(., function(curr_facet){
    foggy %>% filter(facet_col == curr_facet) %>% pull(real_fog) %>%
    seq(., max_dt, by = 1) %>% as_tibble() %>% mutate(facet_col = curr_facet)
  }) %>% bind_rows() %>% inner_join(
    df_p %>% group_by(date, facet_col) %>% 
      summarise(sum_n = sum(n, na.rm = T) ) %>% 
      group_by(facet_col) %>%
      summarise(max_n = max(sum_n, na.rm = T)), 
    by = "facet_col"
  ) %>% mutate(ymin = 0, graph_type = "Numbers") %>% 
    rename( ymax_A := max_n) %>% 
    rename(date := value) %>%
    inner_join(df_p %>% select(facet_col, facet_lbl), by = "facet_col")
  
  
  #df_ribbon %>% group_by(facet_col)
  
  # df_pl %>%   #   ungroup() %>% 
  #   mutate(max_date = max(date, na.rm = T)) %>% 
  #   distinct(facet_lbl, max_date) %>%
  #   inner_join(foggy, by = "facet_col") %>% select( facet_col, max_date, real_fog) 
    
    #group_by(facet_col) %>% 
    #mutate(seq(real_fog, max_date, by = 1))
   # pivot_longer(2:3) %>% 
    #group_by(facet_col) %>% 

    #expand(facet_col, a = full_seq(value, 1)) %>% group_by(facet_col) %>% summarise(min(a))
  
  # df_ribbon <- tibble(date = seq(from = Sys.Date() %m-% days(fog_days), to = Sys.Date(), by = 1),
  #        ymin = 0, ymax_A= max(df_pl$n), graph_type = "Numbers"
  # )
  
  
  if (grepl("AgeGroup", col_col_nm)){
    df_p$col_col <- as.ordered(df_p$col_col) 
  }
  #ffffe5
  #f7fcb9
  #d9f0a3
  #addd8e
  #78c679
  #41ab5d
  #238443
  #006837
  #004529
  #plot(df_p$date)
  #df_p %>% count(PT)
  #df_pl %>% count(PT)
  df_p %>% #filter(date >= early_dt) %>% 
    ggplot(aes(x = date, y = n)) +
    geom_ribbon(data = df_ribbon, mapping = aes(x = date, ymin = 0, ymax = ymax_A), inherit.aes = F, fill = "grey", alpha = 0.5)  +
    geom_col(mapping = aes(fill = col_lbl), alpha = 0.5) +
    geom_line(data = df_pl, mapping = aes(y = roll_mean, color = "movingAvg"), size = 1.5, linetype = "solid")  +
    scale_x_date(limits = c(early_dt, NA), date_breaks = "2 weeks", date_labels = "%d %b") + 
    #scale_y_continuous(labels = comma) +
    scale_colour_manual(labels = c(ln_lbl), 
                        values=c("movingAvg" = "black")) +
    #{if(  Percent_fill) facet_grid(vars(graph_type), scales = "free_y") }+
    #scale_colour_brewer() +
    #scale_fill_discrete(brewer.pal(11, "Set3"))+
    {if(grepl("AgeGroup", col_col_nm)) scale_fill_brewer(palette = "YlGn")} +
    facet_grid(rows = vars(facet_lbl), scales = "free_y") +
    #guides(fill = T) +
    labs(title = ttl_lbl, 
         subtitle = sub_ttl_lbl, 
         caption = paste0("generated on ", Sys.Date()),
         y = y_lbl, 
         x = get_lbl(dt_col),
         fill = get_lbl(col_col_nm),
         color = "") 
    
    # theme(legend.key=element_blank(),
    #       legend.title=element_blank()) 
      #theme(legend.position = c(0.7, 0.9))
    
}



extract_single_epi_curve <- function(dt_col, 
                         col_col_nm, 
                         ..., 
                         width = 10, 
                         height = 7, 
                         units = "in",
                         target_dir = get_generic_report_dir("epi_curves")
                         ){
  
  
  
  p <- plot_epi_curve(dt_col = dt_col, col_col_nm = col_col_nm, ...)
  
  fn = paste0("epi_curve_", Sys.Date(), "_", dt_col, "_", col_col_nm, ".svg")
  
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  
  ggsave(file=file.path(target_dir, fn), 
         plot=p, 
         width= width, 
         height= height,
         units  = units)
}



extract_single_percent_by_time <- function(dt_col, 
                                     col_col_nm, 
                                     ..., 
                                     width = 10, 
                                     height = 7, 
                                     units = "in",
                                     target_dir = get_generic_report_dir("percent_by_time")
){
  
  
  
  p <- plot_epi_curve(dt_col = dt_col, col_col_nm = col_col_nm, ...)
  
  fn = paste0("percent_by_time_", Sys.Date(), "_", dt_col, "_", col_col_nm, ".svg")
  
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  
  ggsave(file=file.path(target_dir, fn), 
         plot=p, 
         width= width, 
         height= height,
         units  = units)
}





extract_epi_curves <- function(report_filter= "epi_curves", 
                               target_dir = get_generic_report_dir("epi_curves"), 
                               dt_col = c("onsetdate_imputed"), 
                               col_col_nm = c("disposition",
                                              #"ethnicity", 
                                              "exposure_cat", 
                                              "asymptomatic2", 
                                              "agegroup20", 
                                              #"AgeGroup10", 
                                              #"Indigenous", 
                                              "hospstatus"#, 
                                              #"ICU", 
                                              #"PT"
                                              )){
  
  if(!get_export_should_write(report_filter= report_filter)){
    print(paste0("Not wrting '",report_filter,"' today."))
    return(NULL)
  }else{
    print(paste0("Now wrting '",report_filter,"' to '", target_dir , "'"))
  }
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }

  expand.grid(dt_col = dt_col, col_col_nm = col_col_nm) %>% as_tibble() %>%
    pmap_dfr(function(...) {
      current <- tibble(...)
      
      extract_single_epi_curve(dt_col = current$dt_col, col_col_nm =  current$col_col_nm)
      
    }) 
  
}






extract_percent_by_time <- function(report_filter= "percent_by_time", 
                               target_dir = get_generic_report_dir("percent_by_time"), 
                               dt_col = c("onsetdate_imputed"), 
                               col_col_nm = c("disposition",
                                              #"ethnicity", 
                                              "exposure_cat", 
                                              "asymptomatic2", 
                                              "agegroup20", 
                                              #"AgeGroup10", 
                                              #"Indigenous", 
                                              "hospstatus"#, 
                                              #"ICU", 
                                              #"PT"
                               )){
  
  if(!get_export_should_write(report_filter= report_filter)){
    print(paste0("Not wrting '",report_filter,"' today."))
    return(NULL)
  }else{
    print(paste0("Now wrting '",report_filter,"' to '", target_dir , "'"))
  }
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  expand.grid(dt_col = dt_col, col_col_nm = col_col_nm) %>% as_tibble() %>%
    pmap_dfr(function(...) {
      current <- tibble(...)
      
      extract_single_percent_by_time(dt_col = current$dt_col, col_col_nm =  current$col_col_nm, Percent_fill = T)
      
    }) 
  
}






####################################
#
clean_up <- function(v){
  #' Cleans up some mess in the string vector 
  v %>% str_to_lower() %>% 
    trimws() %>% 
    replace(., is.na(.), "") %>%
    replace(., . == "unknown", "") %>%
    replace(., . == "unk", "") %>% 
    replace(., . == "not asked", "")
}


####################################
#
get_pt_is_recorded <- function(case_cnts, min_fraction = 0.9, min_sample = 100, include_canada = TRUE){
  #'
  #' Does the PT meat some thrshold of having reported this variable.
  #'
  #'
  tbl_cols <- tibble(is_recorded_FALSE = numeric(), is_recorded_TRUE = numeric())

  
  tmp <-
    case_cnts %>%
    group_by(pt, is_recorded) %>% 
    summarise(nn = sum(n)) %>% 
    pivot_wider(names_from = is_recorded , values_from = nn, names_prefix = "is_recorded_", values_fill = 0) %>%
    bind_rows(tbl_cols, .) %>% replace_na(list(is_recorded_FALSE = 0, is_recorded_TRUE = 0)) %>% 
    mutate(total_counts = is_recorded_FALSE + is_recorded_TRUE) %>% 
    mutate(is_recorded_fraction = is_recorded_TRUE/ total_counts) %>%
    mutate(sample_size = total_counts *is_recorded_fraction) %>%
    arrange(desc(is_recorded_fraction)) %>% 
    #  mutate(lbl = paste(pt, "recoreded ", label_percent()(is_recorded_fraction)))
    # filter((is_recorded_fraction >= min_fraction & sample_size > min_sample) | pt == "CANADA") %>%
    select(pt, is_recorded_fraction, sample_size) %>% 
    ungroup()
  
  if (include_canada){
    tmp <- tmp %>% filter((is_recorded_fraction >= min_fraction & sample_size > min_sample) | pt == "CANADA")
  }else{
    tmp <- tmp %>% filter(is_recorded_fraction >= min_fraction & sample_size > min_sample)
  }
  return(tmp)
}



get_df_2_plot <- function(
  COL_NM,
  COL_Desc = get_lbl(COL_NM),
  NA_replace = "",
  #Ouput_file_nm =  paste0("fraction_", COL_Desc,"_by_age_and_prov_",Sys.Date(),".svg"), 
  min_fraction = 0.9, 
  min_sample = 100, 
  include_canada = F, 
  agegrp = "AgeGroup10",
  alpha = 0.05,
  con = get_covid_cases_db_con(),
  ...
){
  #'
  #' Returns DF with information about the rate that given column is "positive", by province and age group
  #'
  #'
  case_cnts = get_db_counts(con = con, colnm = COL_NM, agegrp = agegrp, NA_replace = NA_replace, ...)
  
    # Clean the data
  case_cnts <- 
    case_cnts %>% 
    mutate(nm = clean_up(nm)) %>% 
    mutate(nm = clean_str(nm)) %>%
    mutate(is_recorded = nm != "")
    
  
  
  
  
  
  # When it is recored, what value occures most offen
  positive_case <-
    case_cnts %>%
    filter(is_recorded == TRUE) %>% 
    group_by(nm) %>% 
    summarise(nn = sum(n)) %>% 
    arrange(desc(nn)) %>% slice(1) %>% pull(nm)
  
  
  # In spite of all this "Yes" is still always considered "Positive"
  if (
    case_cnts %>%
    filter(is_recorded == TRUE) %>% 
    filter(nm == YES_STR) %>% nrow() >= 1
  ){
    positive_case <- YES_STR
  }
  
  
  
  cases_total <- 
    case_cnts %>% 
    #filter(is_recorded == TRUE) %>%
    #filter(pt %in% kept_records$pt) %>% 
    mutate(nm = if_else(nm == positive_case, YES_STR, NO_STR)) %>% 
    group_by(nm,  age_group, is_recorded) %>% 
    summarise(n = sum(n)) %>% 
    mutate(pt = "CANADA") %>% 
    ungroup() %>% 
    bind_rows(case_cnts)
  
  
  # which provinces kept records
  kept_records <- 
    cases_total %>% 
    get_pt_is_recorded(min_fraction = min_fraction, 
                       min_sample = min_sample, 
                       include_canada = include_canada)
  
  
  
  
  
  ##############################
  # Overall rate
  pt_fraction <- 
    cases_total %>%
    filter(is_recorded == TRUE) %>%
    group_by(pt, nm) %>% 
    summarise(nn = sum(n)) %>%
    ungroup() %>% 
    pivot_wider(names_from = nm, values_from = nn, names_prefix = "nm_", values_fill = 0) %>% 
    mutate(nm_frac_yes = nm_Yes / (nm_Yes + nm_No)) %>% 
    select(pt, nm_frac_yes)
  
  
  lbls <- 
    kept_records %>% 
    left_join(pt_fraction, by = "pt") %>% 
    mutate(lbl = paste(pt, 
                       " recorded=", label_percent()(is_recorded_fraction), 
                       "\n",COL_Desc,"=", label_percent()(nm_frac_yes), 
                       "\nN=", label_number(big.mark = ",")(sample_size), 
                       sep = "" )
    ) %>% 
    select(pt, lbl)
  
  
  
  

  #binom.confint(sum(vehicleType=="suv"), length(vehicleType))
  #binom.confint(sum(vehicleType=="suv"), length(vehicleType), methods="exact")
  #pp <-
  tmp <- 
    cases_total %>%  #count(age_group)
    filter(is_recorded == TRUE) %>%
    filter(!is.na(age_group)) %>%
    filter(age_group != "Unknown") %>%
    group_by(age_group, nm,pt) %>%
    summarise(nn = sum(n)) %>%
    pivot_wider(names_from = nm, values_from = nn, names_prefix = "nm_", values_fill = 0) %>% 
    mutate(nm_total = (nm_Yes + nm_No)) %>%# filter(pt == "CANADA")
    mutate(nm_frac_yes2 = binom.confint(nm_Yes, nm_total, conf.level = 1 - alpha,  methods="exact") )  %>% 
    mutate(nm_frac_yes = nm_Yes / (nm_total)) %>%# filter(pt == "CANADA")
    inner_join(lbls, by = "pt") %>% #filter(pt == "CANADA")
    mutate(nm_per_yes = paste0(percent(nm_frac_yes, accuracy = 1), "\nN=", nm_total )) #%>% filter(pt == "CANADA")  
  
  tmp$nm_frac_yes2 %>% 
    unchop() %>% 
    rename_all(paste, "CI", sep = "_") %>% 
    bind_cols(tmp, . ) %>% 
    mutate(nm_per_yes = paste0(
      #percent(nm_frac_yes, accuracy = 1), 
      " (", 
      percent(lower_CI, accuracy = 1), "-", 
      percent(upper_CI, accuracy = 1), 
      ")"
      #, "\nN=", nm_total 
    )
    ) #%>% filter(pt == "CANADA")  
  
}

plot_positives_by_prov <- function(COL_NM = "ICU",
                                   COL_Desc = get_lbl(COL_NM),
                                   NA_replace = "",
                                   min_fraction = 0.9, 
                                   include_canada = F, 
                                   agegrp = "AgeGroup10",
                                   ...
                                   ){

  df <- get_df_2_plot(COL_NM = COL_NM,
                #Ouput_file_nm  = Ouput_file_nm,
                min_fraction = min_fraction,
                include_canada = include_canada, 
                agegrp = agegrp, 
                NA_replace = NA_replace,
                ... )
  
  
  pp <- 
    df %>%
    ggplot(  aes(x = age_group, y = nm_frac_yes, fill = pt, color = pt) ) + 
    geom_point(aes(size = n_CI)) + 
    geom_line(aes(group = pt))+
    geom_errorbar(aes(ymin=lower_CI, ymax=upper_CI), width = 0.2) +
    geom_ribbon(aes(ymin=lower_CI, 
                    ymax=upper_CI, 
                    x = as.numeric(age_group)), 
                alpha = 0.5) +
    #geom_col(color = "black", alpha = 0.5) +
    geom_label(mapping = aes(label = nm_per_yes, color = pt, y = upper_CI), fill = "white", alpha = 0.5, nudge_x = 0.25, nudge_y = 0.1, check_overlap = T)+
    
    #geom_smooth() + 
    scale_y_continuous() +
    facet_wrap(vars(lbl)) +
    labs(y = paste0("Fraction ",COL_Desc), 
         x = get_lbl(agegrp),
         color = "",
         fill = "",
         title = paste0("Fraction ",COL_Desc," by age and province"), 
         subtitle = "when substantial fraction of cases reported the relevent information.",
         caption = paste0("generated on ",Sys.Date())) +
    guides(fill = F, size = F)
  
  
  
  pp
}




extract_single_positives_by_age_prov <- function(
  COL_NM,
  agegrp,
  NA_replace,
  report_filter = "by_age_prov",
  ...,
  width = 10, 
  height = 7, 
  units = "in",  
  target_dir = get_generic_report_dir(report_filter)
  
){
  
  print(paste0("saving ", COL_NM))
  
  p <- plot_positives_by_prov(COL_NM = COL_NM, agegrp = agegrp, NA_replace = NA_replace, ...)
  
  fn = paste0("by_age_prob_", Sys.Date(), "_", COL_NM, "_", agegrp, ".svg")
  
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  
  ggsave(file=file.path(target_dir, fn), 
         plot=p, 
         width= width, 
         height= height,
         units  = units)
}


extract_positives_by_age_prov <- function(report_filter= "by_age_prov", 
                               target_dir = get_generic_report_dir(report_filter), 
                               agegrp = c("AgeGroup20", "AgeGroup10"), 
                               NA_replace = c("No","","", ""),
                               COL_NM = c(#"Disposition", 
                                          #"Ethnicity", 
                                          "COVIDDeath",
                                          #"SymShortnessofBreath",
                                          "Asymptomatic", 
                                          #"Indigenous", 
                                          "Hosp", 
                                          "ICU")){

  if(!get_export_should_write(report_filter= report_filter)){
    print(paste0("Not wrting '",report_filter,"' today."))
    return(NULL)
  }else{
    print(paste0("Now wrting '",report_filter,"' to '", target_dir , "'"))
  }
  
  if ( ! dir.exists(target_dir)){
    dir.create(target_dir, recursive = T)
  }
  
  tibble(COL_NM = COL_NM, NA_replace = NA_replace) %>% expand_grid(agegrp = agegrp)%>% mutate_all(as.character) %>%
  
  #expand.grid(agegrp = agegrp, COL_NM = COL_NM) %>% as_tibble() %>% mutate_all(as.character) %>% 
    pmap_dfr(function(...) {
      current <- tibble(...)
      
      extract_single_positives_by_age_prov(agegrp = current$agegrp, COL_NM =  current$COL_NM, NA_replace = current$NA_replace)
      
    }) 
  
}


#a_tbl

# 
# plot_survival_curve <- function(con = get_covid_cases_db_con(),
#                            a_tbl = get_flat_case_tbl(con = con),
#                            classifications_allowed = c("Probable", "Confirmed"),
#                            dt_col = "OnsetDate",
#                            col_col_nm = "EXPOSURE_CAT",
#                            facet_col_nm = "PT",
#                            early_dt =Sys.Date() %m-% months(6),
#                            rolling_average = 7,
#                            fog_days = 14,
#                            Percent_fill = F,
#                            MIN_LUMP_PROP = 0.02
# ){
# 
# 
#   a_tbl %>% filter(Classification %in% classifications_allowed) %>%
#     select(PT, OnsetDate, Disposition, COVIDDeath, DeathDate, RecoveryDate, AgeGroup20, AgeGroup10, Age) %>%
#     mutate(Disposition = clean_str(Disposition, BLANK_replace = "Ill")) %>%
#     mutate(RecoveryDate = as.Date(ifelse(Disposition == "Recovered", RecoveryDate, NA))) %>%
#     mutate(DeathDate = as.Date(ifelse(Disposition == "Deceased", DeathDate, NA))) %>%
#     mutate(has_OnsetDate = is.na(OnsetDate),
#            has_DeathDate =  is.na(DeathDate),
#            has_RecoveryDate=  is.na(RecoveryDate)) %>%
#     count(PT, Disposition, has_OnsetDate, has_DeathDate, has_RecoveryDate)
#     pivot_longer(cols = )
# 
# 
#     library(cmprsk)
# 
# }
# cmprskQ::
# cmprskQR::

