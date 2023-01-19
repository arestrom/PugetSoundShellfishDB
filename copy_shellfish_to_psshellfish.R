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
db_table_counts = function(db_server = "local", db = "shellfish", schema = "public") {
  if ( db_server == "local" ) {
    db_con = pg_con_local(dbname = db)
  } else {
    db_con = pg_con_prod(dbname = db)
  }
  # Run query
  qry = glue("select table_name FROM information_schema.tables where table_schema = '{schema}'")
  db_tables = DBI::dbGetQuery(db_con, qry) |> 
    pull(table_name)
  tabx = integer(length(db_tables))
  get_count = function(i) {
    tabxi = dbGetQuery(db_con, glue("select count(*) from {schema}.", db_tables[i]))
    as.integer(tabxi$count)
  }
  rc = lapply(seq_along(tabx), get_count)
  dbDisconnect(db_con)
  rcx = as.integer(unlist(rc))
  dtx = tibble(table = db_tables, row_count = rcx)
  dtx = dtx |> 
    arrange(table)
  dtx
}

#============================================================================================
# Verify the same number of rows exist in both new and old DBs....They now do! 
#============================================================================================

# Get table names and row counts
source_row_counts = db_table_counts(db_server = "local", db = "shellfish")
sink_row_counts = db_table_counts(db_server = "local", db = "ps_shellfish")

source_row_counts = source_row_counts |> 
  filter(!table %in% c("beach_info_2008", "beach_info_2010",
                       "beach_info_2011", "beach_info_2012",
                       "beach_info_2013", "beach_info_2014",
                       "beach_info_2015", "beach_info_2016",
                       "beach_info_2017", "beach_info_2018",
                       "beach_info_2017", "beach_info_2018",
                       "beach_info_2019", "flight_count_2008",
                       "flight_count_2011", "flight_count_2012",
                       "flight_count_2013", "flight_count_2014",
                       "flight_count_2015", "flight_count_2016",
                       "flight_count_2017", "flight_count_2018",
                       "flight_count_2019"))

# Combine to a dataframe
compare_counts = source_row_counts |> 
  full_join(sink_row_counts, by = "table") |> 
  # Ignore tables that exist in local but not prod
  filter(!table %in% c("geometry_columns", "geography_columns", "spatial_ref_sys")) |> 
  # Pull out and rename
  select(table, source_table = row_count.x, sink_table = row_count.y) |> 
  mutate(row_diff = abs(source_table - sink_table))

# Inspect any differences
diff_counts = compare_counts %>%
  filter(!row_diff %in% c(0L))

# Output message
if ( nrow(diff_counts) > 0 ) {
  cat("\nWARNING: Some row counts differ. Inspect 'diff_counts'.\n\n")
} else {
  cat("\nRow counts are the same. Ok to proceed.\n\n")
}

#====================================================================================================
# Load LUT data from shellfish to ps_shellfish...in order of DataDictionary.Rmd
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
# new_dat = tibble(season_type_id = get_uuid(3L),
#                  season_type_code = c("Dungeness", "SpotShrimp", "ClamOyster"),
#                  season_type_description = c("Dungeness Crab",
#                                              "Spot Shrimp",
#                                              "Intertidal Clam and Oyster"),
#                  obsolete_flag = rep(0, 3),
#                  obsolete_datetime = rep(as.POSIXct(NA), 3))
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

#======================================================================================================
# Copy location tables
#======================================================================================================

# # REDO: Dump newly uploaded location data
# qry = glue::glue("DELETE FROM location_boundary ",
#                  "WHERE location_boundary_id IS NOT NULL")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # REDO: Dump newly uploaded location data
# qry = glue::glue("DELETE FROM location_coordinates ",
#                  "WHERE location_coordinates_id IS NOT NULL")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# qry = glue::glue("DELETE FROM location ",
#                  "WHERE location_id IS NOT NULL")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# Point locations ===========================
# 
# # Read all point locations
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'point_location')
# loct = dbReadTable(pg_con, 'location_type_lut')
# dbDisconnect(pg_con)
# 
# # Identify location_types
# dat = dat |>
#   left_join(loct, by = "location_type_id") |>
#   select(-c(obsolete_flag, obsolete_datetime))
# 
# # Check loc types
# unique(dat$location_type_description)
# unique(dat$beach_id)
# 
# # Pull out ploc
# ploc = dat |>
#   select(location_id = point_location_id, location_type_id,
#          location_code, location_name, location_description,
#          comment_text, created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# # Write to location
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location")
# DBI::dbWriteTable(pg_con, tbl, ploc, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Read point_location as geometry
# qry = glue::glue("select point_location_id as location_id, horizontal_accuracy, ",
#                  "gid, geom AS geometry, created_datetime, created_by, ",
#                  "modified_datetime, modified_by ",
#                  "from point_location")
# 
# db_con = pg_con_local(dbname = "shellfish")
# plocc_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
# 
# # Pull out plocc and convert to geography
# plocc = plocc_st |>
#   st_transform(4326) |>
#   mutate(location_coordinates_id = get_uuid(nrow(dat))) |>
#   select(location_coordinates_id, location_id,
#          horizontal_accuracy, gid,
#          created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# # Write plocc to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# st_write(obj = plocc, dsn = pg_con, layer = "coords_temp")
# DBI::dbDisconnect(pg_con)
# 
# # Use select into query to get data into point_location
# qry = glue::glue("INSERT INTO location_coordinates ",
#                  "SELECT CAST(location_coordinates_id AS UUID), CAST(location_id AS UUID), ",
#                  "horizontal_accuracy, gid, ",
#                  "geometry as geog, ",
#                  "CAST(created_datetime AS timestamptz), created_by, ",
#                  "CAST(modified_datetime AS timestamptz), modified_by ",
#                  "FROM coords_temp")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # Drop temp
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, "DROP TABLE coords_temp")
# DBI::dbDisconnect(pg_con)
# 
# Polygon locations ===========================
# 
# # beach_boundary ==============
# 
# # Read all beach_boundary locations
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'beach_boundary_history')
# styp = dbReadTable(pg_con, 'survey_type_lut')
# dbDisconnect(pg_con)
# 
# # Check some fields
# unique(dat$survey_type_id)
# unique(dat$active_indicator)
# unique(dat$inactive_reason)
# 
# # Check survey_type
# dat = dat |>
#   left_join(styp, by = "survey_type_id") |>
#   select(-c(obsolete_flag, obsolete_datetime))
# 
# # Check some fields...can drop this
# unique(dat$survey_type_code)
# 
# # Check how many beach_ids
# length(unique(dat$beach_id))
# 
# # Pull out bloc
# bloc = dat |>
#   mutate(location_type_id = "8668a1fe-3d0c-4b2b-beb4-f606a0dc78e1") |>    # Beach polygon, Intertidal Management
#   mutate(comment_text = NA_character_) |>
#   mutate(location_code = NA_character_) |>
#   mutate(location_name = NA_character_) |>
#   mutate(created_datetime = with_tz(Sys.time(), "UTC")) |>
#   mutate(created_by = "stromas") |>
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(modified_by = NA_character_) |>
#   select(location_id = beach_id, location_type_id,
#          location_code, location_name,
#          comment_text, created_datetime, created_by,
#          modified_datetime, modified_by) |>
#   distinct()
# 
# unique(bloc$created_by)
# unique(bloc$modified_by)
# 
# # Write to location
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location")
# DBI::dbWriteTable(pg_con, tbl, bloc, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Read beach_boundary_history as geometry
# qry = glue::glue("select beach_boundary_history_id as location_boundary_id, ",
#                  "beach_id as location_id, beach_number as boundary_code, ",
#                  "beach_name as boundary_name, active_datetime, inactive_datetime, ",
#                  "inactive_reason, gid, geom AS geometry, created_datetime, created_by, ",
#                  "modified_datetime, modified_by ",
#                  "from beach_boundary_history")
# 
# db_con = pg_con_local(dbname = "shellfish")
# blocc_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
# 
# # Pull out blocc and convert to geography
# blocc = blocc_st |>
#   st_transform(4326) |>
#   mutate(gid = seq(1L, nrow(blocc_st))) %>%
#   select(location_boundary_id, location_id, boundary_code,
#          boundary_name, active_datetime, inactive_datetime,
#          inactive_reason, gid,
#          created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# # Write plocc to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# st_write(obj = blocc, dsn = pg_con, layer = "bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# # Use select into query to get data into point_location
# qry = glue::glue("INSERT INTO location_boundary ",
#                  "SELECT CAST(location_boundary_id AS UUID), CAST(location_id AS UUID), ",
#                  "boundary_code, boundary_name, ",
#                  "CAST(active_datetime AS timestamptz), ",
#                  "CAST(inactive_datetime AS timestamptz), ",
#                  "inactive_reason, gid, ",
#                  "geometry as geog, ",
#                  "CAST(created_datetime AS timestamptz), created_by, ",
#                  "CAST(modified_datetime AS timestamptz), modified_by ",
#                  "FROM bounds_temp")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # Drop temp
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, "DROP TABLE bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# management_region ==============
# 
# # Read all management region locations
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'management_region_lut')
# dbDisconnect(pg_con)
# 
# # Pull out bloc
# mloc = dat |>
#   mutate(location_type_id = "315b46b4-ff59-44f1-8d57-65c839f2e25b") |>    # Shellfish Management Region
#   mutate(comment_text = NA_character_) |>
#   mutate(location_code = as.character(management_region_code)) |>
#   mutate(location_name = NA_character_) |>
#   mutate(created_datetime = with_tz(Sys.time(), "UTC")) |>
#   mutate(created_by = "stromas") |>
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(modified_by = NA_character_) |>
#   select(location_id = management_region_id, location_type_id,
#          location_code, location_name,
#          location_description = management_region_description,
#          comment_text, created_datetime, created_by,
#          modified_datetime, modified_by) |>
#   distinct()
# 
# unique(mloc$created_by)
# unique(mloc$modified_by)
# 
# # Write to location
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location")
# DBI::dbWriteTable(pg_con, tbl, mloc, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Read management_region as geometry
# qry = glue::glue("select management_region_id as location_id, ",
#                  "management_region_code as boundary_code, ",
#                  "geom AS geometry ",
#                  "from management_region_lut")
# 
# db_con = pg_con_local(dbname = "shellfish")
# mlocc_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
# 
# # Get most recent gid
# qry = glue("select max(gid) from location_boundary")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "ps_shellfish")
# max_gid = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
# 
# # Add one
# new_gid = max_gid$max + 1L
# 
# # Pull out blocc and convert to geography
# mlocc = mlocc_st |>
#   st_transform(4326) |>
#   st_cast("MULTIPOLYGON") |>
#   mutate(location_boundary_id = get_uuid(nrow(mlocc_st))) |>
#   mutate(gid = seq(new_gid, new_gid + nrow(mlocc_st) - 1L)) |>
#   mutate(boundary_name = paste0("Region ", boundary_code)) |>
#   mutate(active_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(inactive_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(inactive_reason = NA_character_) |>
#   mutate(created_datetime = with_tz(Sys.time(), "UTC")) |>
#   mutate(created_by = "stromas") |>
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(modified_by = NA_character_) %>%
#   select(location_boundary_id, location_id, boundary_code,
#          boundary_name, active_datetime, inactive_datetime,
#          inactive_reason, gid,
#          created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# # Write plocc to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# st_write(obj = mlocc, dsn = pg_con, layer = "bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# # Use select into query to get data into point_location
# qry = glue::glue("INSERT INTO location_boundary ",
#                  "SELECT CAST(location_boundary_id AS UUID), CAST(location_id AS UUID), ",
#                  "boundary_code, boundary_name, ",
#                  "CAST(active_datetime AS timestamptz), ",
#                  "CAST(inactive_datetime AS timestamptz), ",
#                  "inactive_reason, gid, ",
#                  "geometry as geog, ",
#                  "CAST(created_datetime AS timestamptz), created_by, ",
#                  "CAST(modified_datetime AS timestamptz), modified_by ",
#                  "FROM bounds_temp")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # Drop temp
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, "DROP TABLE bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# shellfish_management_area ==============
# 
# # Read all shellfish management area locations
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'shellfish_management_area_lut')
# dbDisconnect(pg_con)
# 
# # Pull out bloc
# aloc = dat |>
#   mutate(location_type_id = "a34fbaa8-6a72-4df9-aded-0b4e85989766") |>    # Shellfish Management area
#   mutate(comment_text = NA_character_) |>
#   mutate(location_code = as.character(shellfish_area_code)) |>
#   mutate(location_name = NA_character_) |>
#   mutate(created_datetime = with_tz(Sys.time(), "UTC")) |>
#   mutate(created_by = "stromas") |>
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(modified_by = NA_character_) |>
#   select(location_id = shellfish_management_area_id, location_type_id,
#          location_code, location_name,
#          location_description = shellfish_area_description,
#          comment_text, created_datetime, created_by,
#          modified_datetime, modified_by) |>
#   distinct()
# 
# unique(aloc$created_by)
# unique(aloc$modified_by)
# 
# # Write to location
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location")
# DBI::dbWriteTable(pg_con, tbl, aloc, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Read shellfish_management_area as geometry
# qry = glue::glue("select shellfish_management_area_id as location_id, ",
#                  "shellfish_area_code as boundary_code, ",
#                  "geom AS geometry ",
#                  "from shellfish_management_area_lut")
# 
# db_con = pg_con_local(dbname = "shellfish")
# alocc_st = st_read(db_con, query = qry)
# dbDisconnect(db_con)
# 
# # Get most recent gid
# qry = glue("select max(gid) from location_boundary")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "ps_shellfish")
# max_gid = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
# 
# # Add one
# new_gid = max_gid$max + 1L
# 
# # Pull out blocc and convert to geography
# alocc = alocc_st |>
#   st_transform(4326) |>
#   st_cast("MULTIPOLYGON") |>
#   mutate(location_boundary_id = get_uuid(nrow(alocc_st))) |>
#   mutate(gid = seq(new_gid, new_gid + nrow(alocc_st) - 1L)) |>
#   mutate(boundary_name = paste0("Shellfish Area ", boundary_code)) |>
#   mutate(active_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(inactive_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(inactive_reason = NA_character_) |>
#   mutate(created_datetime = with_tz(Sys.time(), "UTC")) |>
#   mutate(created_by = "stromas") |>
#   mutate(modified_datetime = with_tz(as.POSIXct(NA), "UTC")) |>
#   mutate(modified_by = NA_character_) %>%
#   select(location_boundary_id, location_id, boundary_code,
#          boundary_name, active_datetime, inactive_datetime,
#          inactive_reason, gid,
#          created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# unique(alocc$created_by)
# unique(alocc$modified_by)
# 
# # Write alocc to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# st_write(obj = alocc, dsn = pg_con, layer = "bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# # Use select into query to get data into point_location
# qry = glue::glue("INSERT INTO location_boundary ",
#                  "SELECT CAST(location_boundary_id AS UUID), CAST(location_id AS UUID), ",
#                  "boundary_code, boundary_name, ",
#                  "CAST(active_datetime AS timestamptz), ",
#                  "CAST(inactive_datetime AS timestamptz), ",
#                  "inactive_reason, gid, ",
#                  "geometry as geog, ",
#                  "CAST(created_datetime AS timestamptz), created_by, ",
#                  "CAST(modified_datetime AS timestamptz), modified_by ",
#                  "FROM bounds_temp")
# 
# # Insert select to ps_shellfish
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # Drop temp
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, "DROP TABLE bounds_temp")
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("aloc", "alocc", "alocc_st", "bloc", "blocc", "blocc_st",
#             "mloc", "mlocc", "mlocc_st", "ploc", "plocc", "plocc_st"))
# 
#======================================================================================================
# Copy data tables
#======================================================================================================
# 
# # beach table becomes tide_correction ===============
# 
# # Read beach table, output to tide_correction table
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'beach')
# dbDisconnect(pg_con)
# 
# # Pull out bloc
# dat = dat |>
#   mutate(tide_correction_id = get_uuid(nrow(dat))) |> 
#   mutate(comment_text = NA_character_) |>
#   select(tide_correction_id, location_id = beach_id,
#          tide_station_location_id, low_tide_correction_minutes, 
#          low_tide_correction_feet, high_tide_correction_minutes,
#          high_tide_correction_feet,
#          created_datetime, created_by,
#          modified_datetime, modified_by)
# 
# # Check
# unique(dat$created_by)
# 
# #=========================================================
# 
# # Identify tide_correction data that is not in location table. Got an error when appending
# qry = glue("select location_id from location")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "ps_shellfish")
# loc = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
# 
# # Pull out tide_correction beach_ids not in location table-
# no_loc_id = dat |> 
#   anti_join(loc, by = "location_id")
# 
# # Get names of beaches
# qry = glue("select beach_id as location_id, local_beach_name ",
#            "from beach")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "shellfish")
# bch_names = dbGetQuery(db_con, qry)
# dbDisconnect(db_con)
# 
# # Combine to attach names
# no_loc_id = no_loc_id |> 
#   left_join(bch_names, by = "location_id")
# 
# # Pull out data for 11 beaches where there is no boundary or counts and, and no info in location table
# missing_ids = unique(no_loc_id$location_id)
# missing_ids = paste0(paste0("'", missing_ids, "'", collapse = ","))
# 
# # Add these to location table
# add_loc = no_loc_id |> 
#   mutate(location_type_id = "8668a1fe-3d0c-4b2b-beb4-f606a0dc78e1") |>    # Beach polygon, Intertidal management. Need polys!!!
#   mutate(location_code = NA_character_) |> 
#   mutate(location_name = local_beach_name) |> 
#   mutate(location_description = NA_character_) |>
#   mutate(comment_text = NA_character_) |> 
#   select(location_id, location_type_id, location_code, location_name,
#          location_description, comment_text, created_datetime, 
#          created_by, modified_datetime, modified_by)
# 
# # Write missing beaches to location table
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location")
# DBI::dbWriteTable(pg_con, tbl, add_loc, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # # Dump...I missed adding the name
# # qry = glue("delete from location where location_id in ({missing_ids})")
# # 
# # # Dump
# # db_con = pg_con_local(dbname = "ps_shellfish")
# # dbExecute(db_con, qry)
# # dbDisconnect(db_con)
# 
# #==================================================================
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "tide_correction")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # beach_allowance becomes location_quota ===============
# 
# # Read beach table, output to tide_correction table
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'beach_allowance')
# dbDisconnect(pg_con)
# 
# # Pull out bloc
# dat = dat |>
#   select(location_quota_id = beach_allowance_id, location_id = beach_id,
#          regulatory_status_id = beach_status_id, effort_estimate_type_id, 
#          egress_model_type_id, species_group_id, report_type_id,
#          harvest_unit_type_id, quota_year = allowance_year, allowable_harvest,
#          comment_text, created_datetime, created_by, modified_datetime, 
#          modified_by)
# 
# # Verify location_id is present in location table
# qry = glue("select location_id ",
#            "from location")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "ps_shellfish")
# loc_ids = dbGetQuery(db_con, qry) |> 
#   pull(location_id)
# dbDisconnect(db_con)
# 
# # Test...All there
# all(dat$location_id %in% loc_ids)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "location_quota")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # beach_season becomes season ===============
# 
# # Read beach table, output to tide_correction table
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'beach_season')
# dbDisconnect(pg_con)
# 
# # Pull out bloc
# dat = dat |>
#   mutate(season_type_id = "4aafd052-ec4b-4e15-b205-6f12fff5a066") |>   # ClamOyster
#   select(season_id = beach_season_id, location_id = beach_id,
#          season_type_id, season_status_id, species_group_id, 
#          season_start_datetime, season_end_datetime, season_description,
#          comment_text, created_datetime, created_by, modified_datetime, 
#          modified_by)
# 
# # Verify location_id is present in location table
# qry = glue("select location_id ",
#            "from location")
# 
# # Get values from shellfish
# db_con = pg_con_local(dbname = "ps_shellfish")
# loc_ids = dbGetQuery(db_con, qry) |> 
#   pull(location_id)
# dbDisconnect(db_con)
# 
# # Test...All there
# all(dat$location_id %in% loc_ids)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "season")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)

# egress_model ===============


# STOPPED HERE. ALL GOOD TO HERE.
















