#=================================================================
# Load data from shellfish local to new ps_shellfish local
# 
# NOTES: 
#  1. Run only a section at a time and verify all is correct! 
#
# ToDo: 
#  1. Copy over, or create, tables needed for data dictionary. DONE. 
#  2. Add code to copy over remaining data tables. Done. 
#
# AS 2023-05-17
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
# Verify the number of rows in both new and old DBs. Cross-check with new table names. 
#============================================================================================

# Get table names and row counts
source_row_counts = db_table_counts(db_server = "local", db = "current_shellfish", schema = "shellfish")
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

#======================================================================================================
# Copy location tables
#======================================================================================================

# # REDO: Dump newly uploaded location data. Got error initially.
# qry = glue::glue("DELETE FROM location_boundary ",
#                  "WHERE location_boundary_id IS NOT NULL")
# 
# # Dump
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# # REDO: Dump newly uploaded location data
# qry = glue::glue("DELETE FROM location_coordinates ",
#                  "WHERE location_coordinates_id IS NOT NULL")
# 
# # Dump
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)
# 
# qry = glue::glue("DELETE FROM location ",
#                  "WHERE location_id IS NOT NULL")
# 
# # Dump
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

# Point locations ===========================

# Read all point locations from current_shellfish
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "point_location")
dat = dbReadTable(pg_con, tbl)
tbl = Id(schema = "shellfish", table = "location_type_lut")
loct = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Identify location_types
dat = dat |>
  left_join(loct, by = "location_type_id") |>
  select(-c(obsolete_flag, obsolete_datetime))

# Check loc types
unique(dat$location_type_description)
unique(dat$beach_id)

# Pull out ploc
ploc = dat |>
  select(location_id = point_location_id, location_type_id,
         location_code, location_name, location_description,
         comment_text, created_datetime, created_by,
         modified_datetime, modified_by)

# Get location data already existing in ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location")
ps_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to keep only new data
ploc_new = ploc |> 
  anti_join(ps_dat, by = "location_id")

# Check number of rows expected
nrow(ploc_new) + nrow(ps_dat)
unique(ploc_new$created_by)

# Write to location
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location")
DBI::dbWriteTable(pg_con, tbl, ploc_new, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Read point_location as geometry
qry = glue::glue("select point_location_id as location_id, ",
                 "horizontal_accuracy, gid, ",
                 "geom AS geometry, created_datetime, ",
                 "created_by, modified_datetime, ",
                 "modified_by ",
                 "from shellfish.point_location")

db_con = pg_con_local(dbname = "current_shellfish")
plocc_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out plocc and convert to geography
plocc = plocc_st |>
  st_transform(4326) |>
  mutate(location_coordinates_id = get_uuid(nrow(dat))) |>
  select(location_coordinates_id, location_id,
         horizontal_accuracy, gid,
         created_datetime, created_by,
         modified_datetime, modified_by)

# Anti-join to keep only new data
plocc_new = plocc |> 
  anti_join(ps_dat, by = "location_id")

# Check number of rows expected
nrow(plocc_new)
unique(plocc_new$created_by)

# Write plocc to ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
st_write(obj = plocc_new, dsn = pg_con, layer = "coords_temp")
DBI::dbDisconnect(pg_con)

# Use select into query to get data into point_location
qry = glue::glue("INSERT INTO location_coordinates ",
                 "SELECT CAST(location_coordinates_id AS UUID), CAST(location_id AS UUID), ",
                 "horizontal_accuracy, gid, ",
                 "geometry as geog, ",
                 "CAST(created_datetime AS timestamptz), created_by, ",
                 "CAST(modified_datetime AS timestamptz), modified_by ",
                 "FROM coords_temp")

# Insert select to ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, qry)
DBI::dbDisconnect(pg_con)

# Drop temp
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, "DROP TABLE coords_temp")
DBI::dbDisconnect(pg_con)

# Polygon locations ===========================

# beach_boundary ==============

# Read all beach_boundary locations
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "beach_boundary_history")
dat = dbReadTable(pg_con, tbl)
tbl = Id(schema = "shellfish", table = "survey_type_lut")
styp = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Check some fields
unique(dat$survey_type_id)
unique(dat$active_indicator)
unique(dat$inactive_reason)

# Check survey_type
dat = dat |>
  left_join(styp, by = "survey_type_id") |>
  select(-c(obsolete_flag, obsolete_datetime))

# Check some fields...can drop this
unique(dat$survey_type_code)
unique(dat$created_by)

# Check how many beach_ids
length(unique(dat$beach_id))

# Pull out bloc
bloc = dat |>
  mutate(location_type_id = "8668a1fe-3d0c-4b2b-beb4-f606a0dc78e1") |>    # Beach polygon, Intertidal Management
  mutate(comment_text = NA_character_) |>
  mutate(location_code = NA_character_) |>
  mutate(location_name = NA_character_) |>
  select(location_id = beach_id, location_type_id,
         location_code, location_name,
         comment_text, created_datetime, created_by,
         modified_datetime, modified_by) |>
  distinct()

# Get location data already existing in ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location")
ps_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to keep only new data. Also get rid of duplicate location_IDs added by Kat
bloc_new = bloc |> 
  anti_join(ps_dat, by = "location_id") |> 
  filter(!created_by == "kmeyer")

unique(bloc_new$created_by)
unique(bloc_new$modified_by)

# Write to location
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location")
DBI::dbWriteTable(pg_con, tbl, bloc_new, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Read beach_boundary_history as geometry
qry = glue::glue("select beach_boundary_history_id as location_boundary_id, ",
                 "beach_id as location_id, beach_number as boundary_code, ",
                 "beach_name as boundary_name, active_datetime, inactive_datetime, ",
                 "geom AS geometry, created_datetime, created_by, ",
                 "modified_datetime, modified_by ",
                 "from shellfish.beach_boundary_history")

db_con = pg_con_local(dbname = "current_shellfish")
blocc_st = st_read(db_con, query = qry)
dbDisconnect(db_con)

# Pull out blocc and convert to geography
blocc = blocc_st |>
  st_transform(4326) |>
  select(location_boundary_id, location_id, boundary_code,
         boundary_name, active_datetime, inactive_datetime,
         created_datetime, created_by,
         modified_datetime, modified_by)

# Anti-join to keep only new data. Also get rid of duplicate location_IDs added by Kat
blocc_new = blocc |> 
  anti_join(ps_dat, by = "location_id") |> 
  filter(!created_by == "kmeyer")

unique(bloc_new$created_by)
unique(bloc_new$modified_by)

# Write plocc to ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
st_write(obj = blocc_new, dsn = pg_con, layer = "bounds_temp")
DBI::dbDisconnect(pg_con)

# Use select into query to get data into point_location
qry = glue::glue("INSERT INTO location_boundary (",
                 "location_boundary_id, location_id, boundary_code, boundary_name, ",
                 "active_datetime, inactive_datetime, geog, created_datetime, created_by) ",
                 "SELECT CAST(location_boundary_id AS UUID), CAST(location_id AS UUID), ",
                 "boundary_code, boundary_name, ",
                 "CAST(active_datetime AS timestamptz), ",
                 "CAST(inactive_datetime AS timestamptz), ",
                 "geometry as geog, ",
                 "CAST(created_datetime AS timestamptz), created_by ",
                 "FROM bounds_temp")

# Insert select to ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, qry)
DBI::dbDisconnect(pg_con)

# Drop temp
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, "DROP TABLE bounds_temp")
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("bloc", "bloc_new", "blocc", "blocc_st", "blocc_new",
            "ploc", "ploc_new", "plocc", "plocc_new", "plocc_st"))

# ======================================================================================================
# Copy data tables
# ======================================================================================================

# beach table becomes tide_correction ===============

# Read beach table, output to tide_correction table
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "beach")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Pull out bloc
dat = dat |>
  mutate(tide_correction_id = get_uuid(nrow(dat))) |>
  mutate(comment_text = NA_character_) |>
  select(tide_correction_id, location_id = beach_id,
         tide_station_location_id, low_tide_correction_minutes,
         low_tide_correction_feet, high_tide_correction_minutes,
         high_tide_correction_feet,
         created_datetime, created_by,
         modified_datetime, modified_by)

# Check
unique(dat$created_by)

# Get correction data already existing in ps_shellfish
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "tide_correction")
tide_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join existing data
new_dat = dat |> 
  anti_join(tide_dat, by = "location_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "tide_correction")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# beach_allowance becomes location_quota ==============================

# Read beach table, output to tide_correction table
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "beach_allowance")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Pull out bloc
dat = dat |>
  select(location_quota_id = beach_allowance_id, location_id = beach_id,
         regulatory_status_id = beach_status_id, effort_estimate_type_id,
         egress_model_type_id, species_group_id, report_type_id,
         harvest_unit_type_id, quota_year = allowance_year, allowable_harvest,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

# Verify location_id is present in location table
qry = glue("select location_id ",
           "from location")

# Get values from shellfish
db_con = pg_con_local(dbname = "ps_shellfish")
loc_ids = dbGetQuery(db_con, qry) |>
  pull(location_id)
dbDisconnect(db_con)

# Test...All there
all(dat$location_id %in% loc_ids)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location_quota")
quota_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(quota_dat, by = "location_quota_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "location_quota")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# beach_season becomes season ===============

# Read beach table, output to tide_correction table
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "beach_season")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Pull out bloc
dat = dat |>
  mutate(season_type_id = "4aafd052-ec4b-4e15-b205-6f12fff5a066") |>   # ClamOyster
  select(season_id = beach_season_id, location_id = beach_id,
         season_type_id, season_status_id, species_group_id,
         season_start_datetime, season_end_datetime, season_description,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

# Verify location_id is present in location table
qry = glue("select location_id ",
           "from location")

# Get values from shellfish
db_con = pg_con_local(dbname = "ps_shellfish")
loc_ids = dbGetQuery(db_con, qry) |>
  pull(location_id)
dbDisconnect(db_con)

# Test...All there
all(dat$location_id %in% loc_ids)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "season")
season_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(season_dat, by = "season_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "season")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# survey ==============================================

# Read
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "survey")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Check if any values in sampling unit
unique(dat$sampling_program_id)

# Rearrange and rename fields
dat = dat |>
  select(survey_id, survey_type_id, organizational_unit_id = sampling_program_id,
         location_id = point_location_id, area_surveyed_id, data_review_status_id,
         survey_completion_status_id, survey_datetime, start_datetime, end_datetime,
         comment_text, created_datetime, created_by, modified_datetime,
         modified_by)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey")
survey_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(survey_dat, by = "survey_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("dat"))

# survey_event ===============

# Read
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "survey_event")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Rearrange and rename fields
dat = dat |>
  select(survey_event_id, survey_id, event_location_id, harvester_type_id,
         harvest_method_id, harvest_gear_type_id, harvest_depth_range_id,
         event_number, event_datetime, harvester_count, harvest_gear_count,
         harvester_zip_code, comment_text, created_datetime, created_by,
         modified_datetime, modified_by)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey_event")
event_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(event_dat, by = "survey_event_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "survey_event")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("dat"))

# species_encounter ===============

pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "species_encounter")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "species_encounter")
species_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(species_dat, by = "species_encounter_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "species_encounter")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("dat"))

# # individual_species =============== Currently no data in any DB
# 
# # Read
# pg_con = pg_con_local(dbname = "current_shellfish")
# tbl = Id(schema = "shellfish", table = "individual_species")
# dat = dbReadTable(pg_con, tbl)
# dbDisconnect(pg_con)
# 
# # Read data already in ps_shellfish for anti-join
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "individual_species")
# indiv_dat = dbReadTable(pg_con, tbl)
# dbDisconnect(pg_con)
# 
# # Anti-join to only load new data
# new_dat = dat |> 
#   anti_join(indiv_dat, by = "individual_species_id")
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "individual_species")
# DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))

# # tide =============== No new data
# 
# # Read
# pg_con = pg_con_local(dbname = "shellfish")
# dat = dbReadTable(pg_con, 'tide')
# dbDisconnect(pg_con)
# 
# # Write
# pg_con = pg_con_local(dbname = "ps_shellfish")
# tbl = Id(schema = "public", table = "tide")
# DBI::dbWriteTable(pg_con, tbl, dat, row.names = FALSE, append = TRUE, copy = TRUE)
# DBI::dbDisconnect(pg_con)
# 
# # Clean up
# rm(list = c("dat"))

# mean_cpue_estimate ===============

# Read
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "mean_cpue_estimate")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Rearrange and rename fields
dat = dat |>
  select(mean_cpue_estimate_id, location_id = beach_id,
         location_code = beach_number, location_name = beach_name,
         estimation_year, flight_season, species_code,
         survey_count, mean_cpue, created_datetime, created_by,
         modified_datetime, modified_by)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "mean_cpue_estimate")
cpue_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(cpue_dat, by = "mean_cpue_estimate_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "mean_cpue_estimate")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("dat"))

# mean_effort_estimate ===============

# Read
pg_con = pg_con_local(dbname = "current_shellfish")
tbl = Id(schema = "shellfish", table = "mean_effort_estimate")
dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Rearrange and rename fields
dat = dat |>
  select(mean_effort_estimate_id, location_id = beach_id,
         location_code = beach_number, location_name = beach_name,
         estimation_year, tide_strata, flight_season, mean_effort,
         tide_count, created_datetime, created_by, modified_datetime,
         modified_by)

# Read data already in ps_shellfish for anti-join
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "mean_effort_estimate")
effort_dat = dbReadTable(pg_con, tbl)
dbDisconnect(pg_con)

# Anti-join to only load new data
new_dat = dat |> 
  anti_join(effort_dat, by = "mean_effort_estimate_id")

# Write
pg_con = pg_con_local(dbname = "ps_shellfish")
tbl = Id(schema = "public", table = "mean_effort_estimate")
DBI::dbWriteTable(pg_con, tbl, new_dat, row.names = FALSE, append = TRUE, copy = TRUE)
DBI::dbDisconnect(pg_con)

# Clean up
rm(list = c("dat"))

# ==============================================================================
# Reset gid sequences. Needed after bulk copy uploads
# ==============================================================================

# Get the current max_gid from the location_boundary table
qry = "select max(gid) from location_boundary"
pg_con = pg_con_local(dbname = "ps_shellfish")
max_gid = DBI::dbGetQuery(pg_con, qry)
DBI::dbDisconnect(pg_con)

# Code to reset sequence
qry = glue("SELECT setval('location_boundary_gid_seq', {max_gid}, true)")
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, qry)
DBI::dbDisconnect(pg_con)

# Get the current max_gid from the location_coordinates table
qry = "select max(gid) from location_coordinates"
pg_con = pg_con_local(dbname = "ps_shellfish")
max_gid = DBI::dbGetQuery(pg_con, qry)
DBI::dbDisconnect(pg_con)

# Code to reset sequence
qry = glue("SELECT setval('location_coordinates_gid_seq', {max_gid}, true)")
pg_con = pg_con_local(dbname = "ps_shellfish")
DBI::dbExecute(pg_con, qry)
DBI::dbDisconnect(pg_con)

# # Get the current max_gid from the location_route table...None for now!!!
# qry = "select max(gid) from location_route"
# pg_con = pg_con_local(dbname = "ps_shellfish")
# max_gid = DBI::dbGetQuery(pg_con, qry)
# DBI::dbDisconnect(pg_con)
#
# # Code to reset sequence
# qry = glue("SELECT setval('location_route_gid_seq', {max_gid}, true)")
# pg_con = pg_con_local(dbname = "ps_shellfish")
# DBI::dbExecute(pg_con, qry)
# DBI::dbDisconnect(pg_con)

#==========================================================
# Done
#==========================================================











