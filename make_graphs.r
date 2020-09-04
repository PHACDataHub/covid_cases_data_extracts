

library(tidyverse)
library(ggplot2)
library(lubridate)
library(zoo)
library(stringr)
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
get_db_counts <- function(con = get_db_con(), 
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

