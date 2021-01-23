

########################
#Clean up stuff sto start
rm(list=ls())
gc()


########################
#Login credentials
library(keyring)
key_set("username_for_meta_base_on_HRE")   #,password = metabase username: user.name@canada.ca
key_set("password_for_meta_base_on_HRE")   #,password = metabase password
key_set("DataHub")                         #,password = refer to handover
key_set("Modelling")                       #,password = refer to handover

# functions file
source("metabase_extracts.r")


#################
# Do all metabase extracts
#
# Alex says I need to have functions in different r file :P
#
extracts()
