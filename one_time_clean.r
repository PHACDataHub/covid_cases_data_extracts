




df <-  get_db_tbl(con = get_covid_cases_db_con(), tlb_nm = "case")

# This preserves the Accents
lapply(df %>% select_if(is.character) %>% colnames(), function(col_nm){
  Encoding(df[[col_nm]]) <<- 'latin1'
})



clnm <- colnames(df)

clnm_not_dates  <- clnm[!clnm %>% grepl(pattern = "date", x = ., ignore.case = T)]

df_2 <- 
df %>% 
  select(any_of(clnm_not_dates)) %>%
  select_if(is.character) %>%
  mutate_all(clean_str_b, capitalize_function = str_to_lower) %>% 
  mutate(PHACID = number_2_phacid(PHACID))



# 
# df_2 %>% slice(5063)

df_2 <- df_2 %>% mutate(PT = str_to_upper(PT))

nms2 <- df_2 %>% colnames()


df_f <- 
df %>% select(-nms2[!nms2 == "PHACID"]) %>%
  mutate_if(function(x) inherits(x, "POSIXct"), function(x){format(as.Date(x), "%Y-%m-%d")}) %>%
  inner_join(df_2,  by = "PHACID") %>%
  select(colnames(df)) 


df_f %>% distinct(HealthRegion) %>% view()

# df_f %>% slice(765) %>% select(PHACID, PHACReportedDate, ReportedDate, OnsetDate)
 df_f %>% slice(834, 1006, 1016, 1596, 2744, 2863, 5855, 143626, 136830) %>% select(PHACID, PTCaseID, PT, Age, PHACReportedDate, ReportedDate, OnsetDate)

# This preserves the Accents
lapply(df_f %>% select_if(is.character) %>% colnames(), function(col_nm){
  Encoding(df_f[[col_nm]]) <<- 'latin1'
})

getwd()

df_f %>% 
  write_csv(path = file.path("~", "..", "Desktop", "cases_clean.csv"), na = "")





a_tbl = get_flat_case_tbl(con = con)

dts <- a_tbl %>% select(PT, PHACReportedDate, ReportedDate, OnsetDate, LabTestResultDate1, Age, Gender) 
p_missing <- unlist(lapply(dts, function(x) sum(is.na(x))))/nrow(dts)
sort(p_missing[p_missing > 0], decreasing = TRUE)

imp <- a_tbl %>% 
  select( PHACReportedDate, ReportedDate, OnsetDate, Age, Gender, Disposition, DeathDate, RecoveryDate) %>% 
  mutate_if(is.Date, as.numeric) %>%
  mice(maxit = 5)


# First, turn the datasets into long format
anesimp_long <- mice::complete(imp, action="long", include = TRUE)

# Convert two variables into numeric
anesimp_long$patriot_amident <- with(anesimp_long, 
                                     as.integer(anesimp_long$patriot_amident))
anesimp_long$pid_x <- with(anesimp_long, 
                           as.integer(anesimp_long$pid_x))

# Take log of M&A variable 
anesimp_long$LogMANO<-log(anesimp_long$MANo+1.01)

# Convert back to mids type - mice can work with this type
anesimp_long_mids<-as.mids(anesimp_long)
# Regression 


fitimp <- with(anesimp_long_mids,
               lm(ft_hclinton ~ manuf + pid_x +
                    patriot_amident + china_econ + LogMANO))

summary(pool(fitimp))








# Extract predictorMatrix and methods of imputation 

predM = imp$predictorMatrix
meth = imp$method

meth 


imp2 <- mice(a_tbl %>% select(PT, as.integer(PHACReportedDate), as.integer(ReportedDate), as.integer(OnsetDate), Age, Gender, Disposition, as.integer(DeathDate), as.integer(RecoveryDate)), 
             maxit = 5, 
             predictorMatrix = predM, 
             method = meth)#, print =  FALSE)


a_tbl %>% 
  mutate(Month_onset = month(OnsetDate)) %>%
  select(PHACID, PT, Month_onset, OnsetDate, EXPOSURE_CAT) %>% 
  count( PT, Month_onset, EXPOSURE_CAT) %>% write.csv("international.csv")
getwd()



con = get_covid_cases_db_con()
a_tbl = get_flat_case_tbl(con = con)


a_tbl %>% 
  mutate(report_delay = ReportedDate -OnsetDate) %>%
  group_by(PT) %>% mutate(n = n()) %>%
  filter((!is.na(ReportedDate)) | (!is.na(OnsetDate))) %>%
  filter(!is.na(report_delay)) %>%
  filter(!is.na(AgeGroup20)) %>%
  filter(AgeGroup20 != "Unknown") %>%
  filter(n >= 1000) %>%
  ggplot(aes(x = report_delay, fill = PT)) + 
  geom_bar(alpha = 0.5) +
  scale_x_continuous(limits = c(-3, 21)) +
  facet_wrap( vars(PT), scales = "free_y")
  #facet_grid(rows = vars(PT), cols = vars(AgeGroup20), scales = "free_y")
  
