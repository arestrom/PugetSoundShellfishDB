#=================================================================
# Load data from shellfish local to new ps_shellfish local
# 
# NOTES: 
#  1.
#
# ToDo: 
#  1. Copy over, or create, tables needed for data dictionary. DONE. 
#  2. Add code to copy over remaining data tables. 
#
# AS 2023-01-15
#=================================================================

# Clear workspace
rm(list=ls(all.names = TRUE))

# Keep connections pane from opening
options("connectionObserver" = NULL)

# Libraries
library(DBI)
library(RPostgres)
library(odbc)
library(dplyr)
library(remisc)
library(sf)
library(glue)
library(lubridate)
library(tibble)

#=====================================================================================
# Function to get user for database
pg_user <- function(user_label) {
  Sys.getenv(user_label)
}

# Function to get pw for database
pg_pw <- function(pwd_label) {
  Sys.getenv(pwd_label)
}

# Function to get pw for database
pg_host <- function(host_label) {
  Sys.getenv(host_label)
}

# Function to connect to postgres
pg_con_local = function(dbname, port = '5432') {
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

# Function to generate dataframe of tables and row counts in database
db_row_counts = function(hostname = "local", dbname) {
  if (hostname == "local") {
    db_con = pg_con_local(dbname = dbname)
  } else if (hostname == "prod") {
    db_con = pg_con_prod(dbname = dbname)
  }
  db_tables = dbListTables(db_con)
  tabx = integer(length(db_tables))
  get_count = function(i) {
    tabxi = dbGetQuery(db_con, paste0("select count(*) from ", db_tables[i]))
    as.integer(tabxi$count)
  }
  rc = lapply(seq_along(tabx), get_count)
  dbDisconnect(db_con)
  rcx = as.integer(unlist(rc))
  dtx = tibble(table = db_tables, row_count = rcx)
  dtx = dtx %>% 
    arrange(table)
  dtx
}

#==============================================================================
# Identify tables where data needs to be loaded
#==============================================================================

# Get table names and row counts in source db
source_row_counts = db_row_counts(dbname = "shellfish")

# Get table names and row counts in sink db
sink_row_counts = db_row_counts(dbname = "ps_shellfish")

# Combine to a dataframe
compare_counts = source_row_counts %>% 
  full_join(sink_row_counts, by = "table") %>% 
  filter(!table == "spatial_ref_sys") %>% 
  select(table, shellfish = row_count.x, ps_shellfish = row_count.y)

# Verify that table names and row counts in source and sink db's are identical
identical(compare_counts$shellfish, compare_counts$ps_shellfish)

# Pull out cases that differ
diff_counts = NULL
if (!identical(compare_counts$shellfish, compare_counts$ps_shellfish)) {
  diff_counts = compare_counts %>% 
    filter(!shellfish == ps_shellfish)
}

#====================================================================================================
# Load fresh set of LUT data from shellfish to ps_shellfish...in order of DataDictionary.Rmd
#====================================================================================================

# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'location_type_lut')
# dbDisconnect(pg_con)
# 
# # -- 3. Add intertidal_area entry to location_type_lut
# # -- 4. Add Aerial flight route to location_type_lut
# # -- 5. Add beach to location_type_lut
# # -- 6. Add management_region to location_type_lut
# # -- 7. Add shellfish_management_area to location_type_lut
# # -- 8. Add intertidal_area to location_type_lut
# 
# # # Add new values
# # new_dat = tibble(location_type_id = get_uuid(5L),
# #                  location_type_description = c("Beach intertidal area",
# #                                                "Aerial flight route, Intertidal management",
# #                                                "Beach polygon, Intertidal management",
# #                                                "Shellfish Management Region",
# #                                                "Shellfish Management Area"),
# #                  obsolete_flag = rep(0, 5),
# #                  obsolete_datetime = rep(as.POSIXct(NA), 5))
# # 
# # # Combine
# # dat = rbind(dat, new_dat)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "mykos")
# dat = dbReadTable(pg_con, 'media_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "media_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'survey_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "survey_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
#
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'agency_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "agency_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Create LUT
# new_dat = tibble(organizational_unit_id = get_uuid(2L),
#                  agency_id = rep("bbe4126e-6081-44e3-af1b-012c98290603", 2),  # WDFW
#                  unit_code = c("Crustacean", "Intertidal"),
#                  unit_name = c("WDFW Puget Sound Crustacean Management Unit",
#                                "WDFW Puget Sound Intertidal Bivalve Management Unit"),
#                  obsolete_flag = rep(0, 2),
#                  obsolete_datetime = rep(as.POSIXct(NA), 2))
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "organizational_unit_lut")
# DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("new_dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'area_surveyed_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "area_surveyed_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'data_review_status_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "data_review_status_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'survey_completion_status_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "survey_completion_status_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'mobile_device_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "mobile_device_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Create LUT
# new_dat = tibble(protocol_type_id = get_uuid(3L),
#                  protocol_type_description = c("WDFW Technical Report",
#                                                "WDFW Internal Document",
#                                                "State-Tribal Management Agreement"),
#                  obsolete_flag = rep(0, 3),
#                  obsolete_datetime = rep(as.POSIXct(NA), 3))
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "protocol_type_lut")
# DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("new_dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'harvester_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "harvester_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'harvest_method_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "harvest_method_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'harvest_gear_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "harvest_gear_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'harvest_depth_range_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "harvest_depth_range_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'species_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "species_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'shell_condition_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "shell_condition_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'catch_result_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "catch_result_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Create LUT
# new_dat = tibble(sex_id = get_uuid(5L),
#                  sex_code = c("male", "female", "trans", "herm", "na"),
#                  sex_description = c("Male",
#                                      "Female",
#                                      "Transitional",
#                                      "Hermaphrodite",
#                                      "Not applicable"),
#                  obsolete_flag = rep(0, 5),
#                  obsolete_datetime = rep(as.POSIXct(NA), 5))
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "sex_lut")
# DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("new_dat"))
# 
# # Create LUT
# new_dat = tibble(season_type_id = get_uuid(4L),
#                  season_type_code = c("Dungeness", "SpotShrimp", "Clam", "Oyster"),
#                  season_type_description = c("Dungeness Crab",
#                                              "Spot Shrimp",
#                                              "Intertidal Clam",
#                                              "Intertidal Oyster"),
#                  obsolete_flag = rep(0, 4),
#                  obsolete_datetime = rep(as.POSIXct(NA), 4))
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "season_type_lut")
# DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("new_dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'season_status_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "season_status_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'species_group_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "species_group_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'beach_status_lut')
# dbDisconnect(pg_con)
# 
# # Rename
# names(dat) = c("regulatory_status_id", 
#                "status_code",
#                "status_description",
#                "obsolete_flag",
#                "obsolete_datetime")
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "regulatory_status_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'effort_estimate_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "effort_estimate_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'egress_model_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "egress_model_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'report_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "report_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'harvest_unit_type_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "harvest_unit_type_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'tide_strata_lut')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "tide_strata_lut")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))
