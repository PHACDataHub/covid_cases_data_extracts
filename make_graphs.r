
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
                          agegrp = "AgeGroup20" ){
  
  a_tbl %>% 
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





plot_epi_curve <- function(con = get_db_con(), 
                           a_tbl = get_flat_case_tbl(con = con),
                           classifications_allowed = c("Probable", "Confirmed"),
                           dt_col = "OnsetDate",
                           col_col_nm = "EXPOSURE_CAT",
                           clr = "blue",
                           early_dt =Sys.Date() %m-% months(6),
                           rolling_average = 7,
                           fog_days = 14
)
{
  #' returng ggPlot object of the epi curve with a rolling average, it is also generally segmented by a column
  
  ln_lbl <- paste0("Rolling Average (", rolling_average, " days)")
  y_lbl <- "Number of new cases"

  
  
  df_p <- 
  a_tbl %>% 
    filter(Classification %in% classifications_allowed) %>% 
    select(PHACID, dt_col, col_col_nm) %>%
    rename(date := dt_col, col_col := col_col_nm) %>% #pull(date) %>% as.Date()
    mutate(date = as.Date(date)) %>% 
    mutate(col_col = clean_str(x = col_col,NA_replace = "Unknown")) %>% #count(col_col) %>% pull(n) %>% sum()
    count(date, col_col) %>% 
    mutate(tot = sum(n)) %>% 
    group_by(col_col) %>% 
    mutate(col_lbl = paste0(col_col, " (", format(100*sum(n)/ tot, digits = 2), "%)")) %>% 
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
    group_by(date) %>% 
    summarise(n = sum(n)) %>% 
    arrange(date) %>% 
    mutate(roll_mean = rollmean(n, rolling_average, na.pad = T, align = "right")) 
  
  
  df_p <- df_p %>% filter(date >= early_dt) 
  df_pl <- df_pl %>% filter(date >= early_dt) 
  
  ttl_lbl <- paste0(y_lbl," in Canada vs. ",get_lbl(dt_col)," \n", get_lbl(col_col_nm))
  sub_ttl_lbl <- paste0(min(df_p$date), " to ",  max(df_p$date))
  
  
  df_ribbon <- tibble(date = seq(from = Sys.Date() %m-% days(fog_days), to = Sys.Date(), by = 1),
         ymin = 0, ymax_A= max(df_pl$n)
  )
  
  
  df_p %>% #filter(date >= early_dt) %>% 
    ggplot(aes(x = date, y = n)) +
    geom_ribbon(data = df_ribbon, mapping = aes(x = date, ymin = 0, ymax = ymax_A), inherit.aes = F, fill = "grey", alpha = 0.5) + 
    geom_col(mapping = aes(fill = col_lbl), alpha = 0.5) +
    geom_line(data = df_pl, mapping = aes(y = roll_mean, color = "movingAvg"), size = 1.5, linetype = "solid") +
    scale_x_date(limits = c(early_dt, NA), date_breaks = "2 weeks", date_labels = "%d %b") + 
    scale_y_continuous(limits = c(0, max(df_pl$n)) ,labels = comma) +
    scale_colour_manual(labels = c(ln_lbl), 
                        values=c("movingAvg" = "black"))+
     # scale_fill_manual(labels = c(bar_lbl), 
     #                     values=c("numberday" = clr))+
    #guides(fill = T) +
    labs(title = ttl_lbl, subtitle = sub_ttl_lbl, caption = paste0("generated on ", Sys.Date()),
         y = y_lbl, 
         x = get_lbl(dt_col),
         fill = get_lbl(col_col_nm),
         color = "") #+
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


extract_epi_curves <- function(report_filter= "epi_curves", 
                               target_dir = get_generic_report_dir("epi_curves"), 
                               dt_col = c("OnsetDate", "ReportedDate"), 
                               col_col_nm = c("EXPOSURE_CAT", "AgeGroup20", "Asymptomatic", "Indigenous", "Hosp", "ICU")){
  
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
  tmp <-
    case_cnts %>%
    group_by(pt, is_recorded) %>% 
    summarise(nn = sum(n)) %>% 
    pivot_wider(names_from = is_recorded , values_from = nn, names_prefix = "is_recorded_", values_fill = 0) %>%
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
  #Ouput_file_nm =  paste0("fraction_", COL_Desc,"_by_age_and_prov_",Sys.Date(),".svg"), 
  min_fraction = 0.9, 
  min_sample = 100, 
  include_canada = TRUE, 
  agegrp = "AgeGroup10",
  alpha = 0.05,
  con = get_covid_cases_db_con(),
  case_cnts = get_db_counts(con = con, colnm = COL_NM, agegrp = agegrp)
){
  #'
  #' Returns DF with information about the rate that given column is "positive", by province and age group
  #'
  #'

  
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
    filter(is_recorded == TRUE) %>%
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
    get_pt_is_recorded(min_fraction = min_fraction, min_sample = min_sample, include_canada = include_canada)
  
  
  
  
  
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


plot_positives_by_prov <- function(){

  df <- get_df_2_plot(COL_NM = COL_NM,
                #Ouput_file_nm  = Ouput_file_nm,
                min_fraction = 0.1,
                include_canada = T )
  
  
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
    geom_label(mapping = aes(label = nm_per_yes, color = pt), fill = "white", alpha = 0.5, nudge_x = 0.25, nudge_y = 0.1)+
    
    #geom_smooth() + 
    scale_y_continuous() +
    facet_wrap(vars(lbl)) +
    labs(y = paste0("Fraction ",COL_Desc), 
         title = paste0("Fraction ",COL_Desc," by age and province"), 
         subtitle = "when substantial fraction of cases reported the relevent information.",
         caption = Sys.Date()) +
    guides(fill = F, size = F)
  
  
  
  pp
}