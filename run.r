

########################
#Clean up stuff sto start
rm(list=ls())
gc()


########################
# functions file
source("metabase_extracts.r")


#################
# Do all metabase extracts
#
# Alex says I need to have functions in different r file :P
#
extracts()
