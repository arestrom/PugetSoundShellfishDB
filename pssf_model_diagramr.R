#=================================================================
# Functions to create table outputs for data dictionary
# 
# NOTES: 
#  1. Should now be able to do using only DM. Need to test
#
# AS 2023-01-12
#=================================================================

# Clear workspace
rm(list = ls(all.names = TRUE))

# Package no longer maintained...recommends dm
# remotes::install_github("bergant/datamodelr")

# Libraries
library(DBI)
library(RPostgres)
library(dplyr)
library(DiagrammeR)
library(datamodelr)
library(DiagrammeRsvg)
library(rsvg)

# Keep connections pane from opening
options("connectionObserver" = NULL)

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
  con <- dbConnect(
    RPostgres::Postgres(),
    host = "localhost",
    dbname = dbname,
    user = pg_user("pg_user"),
    password = pg_pw("pg_pwd_local"),
    port = port)
  con
}

#=====================================================================================
# Test create and export functions
#=====================================================================================

# Define connection
db_con = pg_con_local(dbname = "ps_shellfish")

# Pull out subset of tables for model
sf_tables = dbListTables(db_con)

# Get query for the re-engineering function
sQuery = dm_re_query("postgres")

# Pull out data to create model
dm_sf = dbGetQuery(db_con, sQuery)
dbDisconnect(db_con)

#=====================================================================================
# Get rid of spatial_ref_sys table...or any unneeded tables
#=====================================================================================

# Define vector of tables to drop
drop_tables = c("spatial_ref_sys")

# Dump unneeded tables
dm_sf = dm_sf %>% 
  filter(!table %in% drop_tables)

# Set as a data_model object
dm_sf_model = as.data_model(dm_sf)

#=====================================================================================
# Create segments
# Left out: agency_lut,
#=====================================================================================

table_segments = list(
  Survey = c("survey", "survey_protocol", "protocol", "survey_person", "person", 
             "survey_type_lut", "organizational_unit_lut", "area_surveyed_lut", 
             "data_review_status_lut", "survey_completion_status_lut", 
             "agency_lut", "protocol_type_lut"),
  Location = c("location", "location_type_lut", "location_boundary", 
               "location_coordinates", "location_route", "tide_correction",
               "media_location", "media_type_lut"),
  Estimation = c("season", "season_status_lut", "location_quota", 
                 "regulatory_status_lut","effort_estimate_type_lut", "egress_model",
                 "egress_model_type_lut", "species_group_lut", "report_type_lut", 
                 "harvest_unit_type_lut","egress_model_version", "mean_cpue_estimate", 
                 "mean_effort_estimate", "tide", "tide_strata_lut", "location_resources"),
  CrabShrimp = c("harvest_depth_range_lut", "harvest_gear_type_lut", 
                 "season_type_lut", "harvest_method_lut"),
  MobileData = c("survey_mobile_device", "mobile_device", "mobile_device_type_lut",
                 "mobile_survey_form"),
  SurveyEvent = c("survey_event", "species_encounter", "species_lut", 
                  "catch_result_type_lut", "shell_condition_lut", 
                  "individual_species", "sex_lut", "harvester_type_lut")
)

# Add segment info
dm_sf_seg_model = dm_set_segment(dm_sf_model, table_segments)

#=====================================================================================
# Create display colors
#=====================================================================================

# Add display color info
display <- list(
  accent5 = c("survey", "survey_protocol", "protocol", "survey_person", "person"),
  accent6 = c("mobile_survey_form", "mobile_device", "survey_mobile_device"),
  accent1 = c("survey_event" ,"species_encounter", "individual_species"),
  accent3 = c("location", "location_boundary", "location_coordinates", 
              "location_route", "location_resources", "tide_correction",
              "media_location"),
  accent4 = c("season", "location_quota", "location_resources", 
              "egress_model", "egress_model_version", "mean_cpue_estimate", 
              "mean_effort_estimate", "tide")
)

# Add color info
dm_sf_seg_col_model = dm_set_display(dm_sf_seg_model, display)

#=====================================================================================
# Generate titles only diagram
#=====================================================================================

# Create diagram with graphvis attributes
sf_title_diagram = dm_create_graph(dm_sf_seg_col_model,
                                   graph_attrs = "rankdir = 'RL', bgcolor = '#f2f3f5' ",
                                   edge_attrs = "dir = both, arrowtail = crow, arrowhead = odiamond",
                                   node_attrs = "fontname = 'Arial'",
                                   view_type = "title_only")

# Export graph
dm_export_graph(sf_title_diagram, file_name = "PSSFTitleDiagram.pdf")

#=====================================================================================
#  Generate location tables diagram
#=====================================================================================

# Define location segments
location_segments = list(
  Survey = c("survey", "survey_event"),
  Location = c("location", "location_boundary", "location_coordinates", 
               "location_route", "location_resources", "tide_correction",
               "media_location"),
  SpeciesEncounter = c("species_encounter", "individual_species")
)

# Add segment info
dm_sf_loc_seg_model = dm_set_segment(dm_sf_model, location_segments)

# Set colors
location_display <- list(
  accent5 = c("survey", "survey_event"),
  accent3 = c("location", "location_boundary", "location_coordinates", 
              "location_route", "location_resources", "tide_correction",
              "media_location"),
  accent1 = c("species_encounter", "individual_species")
)

# Add color info
dm_sf_loc_seg_col_model = dm_set_display(dm_sf_loc_seg_model, location_display)

# Set focus on location tables
location_focus = list(
  tables = c("location", "location_boundary", "location_coordinates", 
             "location_route", "location_resources", "tide_correction",
             "media_location", "species_encounter", "survey", 
             "survey_event")
)

# Inspect type values
unique(dm_sf_loc_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_loc_seg_col_model[["columns"]]$type[dm_sf_loc_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geography"
dm_sf_loc_seg_col_model[["columns"]]$type[dm_sf_loc_seg_col_model[["columns"]]$type == "timestamp with time zone"] = "timestamptz"

# Inspect type values
unique(dm_sf_loc_seg_col_model[["columns"]]$type)

# Inspect mandatory values
unique(dm_sf_loc_seg_col_model[["columns"]]$mandatory)

# Update some column values
dm_sf_loc_seg_col_model[["columns"]]$mandatory[dm_sf_loc_seg_col_model[["columns"]]$mandatory == 1L] = "not null"
dm_sf_loc_seg_col_model[["columns"]]$mandatory[dm_sf_loc_seg_col_model[["columns"]]$mandatory == "0"] = "nullable"

# Inspect type values
unique(dm_sf_loc_seg_col_model[["columns"]]$mandatory)

# Create location diagram
sf_location_diagram = dm_create_graph(dm_sf_loc_seg_col_model, 
                                      focus = location_focus,
                                      graph_attrs = "rankdir = 'RL', bgcolor = '#f2f3f5' ", 
                                      edge_attrs = "dir = both, arrowtail = crow, arrowhead = odiamond",
                                      node_attrs = "fontname = 'Arial'",
                                      view_type = "all",
                                      col_attr = c("column", "type", "mandatory"))

# Export graph
dm_export_graph(sf_location_diagram, file_name = "PSSFLocationDiagram.pdf")

#=====================================================================================
#  Generate survey level diagram
#=====================================================================================

# Define location segments
survey_segments = list(
  Survey = c("survey", "survey_type_lut", "organizational_unit_lut", 
             "data_review_status_lut", "area_surveyed_lut", 
             "survey_completion_status_lut", "agency_lut"),
  Location = c("location"),
  Protocol = c("protocol", "survey_protocol", "protocol_type_lut"),
  Surveyor = c("survey_person", "person"),
  MobileData = c("survey_mobile_device", "mobile_device", "mobile_device_type_lut",
                 "mobile_survey_form")
)

# Add segment info
dm_sf_survey_seg_model = dm_set_segment(dm_sf_model, survey_segments)

# Set colors
survey_display <- list(
  accent5 = c("survey"),
  accent3 = c("location"),
  accent1 = c("protocol", "survey_protocol"),
  accent4 = c("survey_person", "person"),
  accent6 = c("survey_mobile_device", 
              "mobile_device", "mobile_survey_form")
)

# Add color info
dm_sf_survey_seg_col_model = dm_set_display(dm_sf_survey_seg_model, survey_display)

# Set focus on survey tables
survey_focus = list(
  tables = c("survey", "survey_type_lut", "organizational_unit_lut", 
             "data_review_status_lut", "area_surveyed_lut", 
             "survey_completion_status_lut", "location",
             "survey_person", "person", "survey_mobile_device",
             "protocol", "survey_protocol", "protocol_type_lut", 
             "mobile_device", "mobile_device_type_lut", 
             "mobile_survey_form")
)

# Inspect type values
unique(dm_sf_survey_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_survey_seg_col_model[["columns"]]$type[dm_sf_survey_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geography"
dm_sf_survey_seg_col_model[["columns"]]$type[dm_sf_survey_seg_col_model[["columns"]]$type == "timestamp with time zone"] = "timestamptz"

# Inspect type values
unique(dm_sf_survey_seg_col_model[["columns"]]$type)

# Inspect mandatory values
unique(dm_sf_survey_seg_col_model[["columns"]]$mandatory)

# Update some column values
dm_sf_survey_seg_col_model[["columns"]]$mandatory[dm_sf_survey_seg_col_model[["columns"]]$mandatory == 1L] = "not null"
dm_sf_survey_seg_col_model[["columns"]]$mandatory[dm_sf_survey_seg_col_model[["columns"]]$mandatory == "0"] = "nullable"

# Inspect type values
unique(dm_sf_survey_seg_col_model[["columns"]]$mandatory)

# Create location diagram
sf_survey_level_diagram = dm_create_graph(dm_sf_survey_seg_col_model, 
                                          focus = survey_focus,
                                          graph_attrs = "rankdir = 'RL', bgcolor = '#f2f3f5' ", 
                                          edge_attrs = "dir = both, arrowtail = crow, arrowhead = odiamond",
                                          node_attrs = "fontname = 'Arial'",
                                          view_type = "all",
                                          col_attr = c("column", "type", "mandatory"))

# Export graph
dm_export_graph(sf_survey_level_diagram, file_name = "PSSFSurveyDiagram.pdf")


#=====================================================================================
# Generate Survey Event diagram
#=====================================================================================

# Define event segments
event_segments = list(
  Survey = c("survey"),
  Location = c("location"),
  SurveyEvent = c("survey_event", "harvest_depth_range_lut", "harvest_gear_type_lut", 
                  "harvester_type_lut", "harvest_method_lut"),
  SpeciesEncounter = c("species_encounter", "species_lut", "catch_result_type_lut", 
                       "shell_condition_lut"),
  IndividualSpecies = c("individual_species", "sex_lut")
)

# Add segment info
dm_sf_event_seg_model = dm_set_segment(dm_sf_model, event_segments)

# Set colors
event_display <- list(
  accent5 = c("survey"),
  accent3 = c("location"),
  accent4 = c("species_encounter"),
  accent1 = c("survey_event"),
  accent7 = c("individual_species")
)

# Add color info
dm_sf_event_seg_col_model = dm_set_display(dm_sf_event_seg_model, event_display)

# Set focus on event tables
event_focus = list(
  tables = c("survey", "survey_event", "location", 
             "harvest_depth_range_lut", "harvest_gear_type_lut", 
             "harvester_type_lut", "harvest_method_lut", 
             "species_encounter", "species_lut", "catch_result_type_lut", 
             "shell_condition_lut","individual_species", "sex_lut")
)

# Inspect type values
unique(dm_sf_event_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_event_seg_col_model[["columns"]]$type[dm_sf_event_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geography"
dm_sf_event_seg_col_model[["columns"]]$type[dm_sf_event_seg_col_model[["columns"]]$type == "timestamp with time zone"] = "timestamptz"

# Inspect type values
unique(dm_sf_event_seg_col_model[["columns"]]$type)

# Inspect mandatory values
unique(dm_sf_event_seg_col_model[["columns"]]$mandatory)

# Update some column values
dm_sf_event_seg_col_model[["columns"]]$mandatory[dm_sf_event_seg_col_model[["columns"]]$mandatory == 1L] = "not null"
dm_sf_event_seg_col_model[["columns"]]$mandatory[dm_sf_event_seg_col_model[["columns"]]$mandatory == "0"] = "nullable"

# Inspect type values
unique(dm_sf_event_seg_col_model[["columns"]]$mandatory)

# Create location diagram
sf_event_encounter_diagram = dm_create_graph(dm_sf_event_seg_col_model, 
                                             focus = event_focus,
                                             graph_attrs = "rankdir = 'RL', bgcolor = '#f2f3f5' ", 
                                             edge_attrs = "dir = both, arrowtail = crow, arrowhead = odiamond",
                                             node_attrs = "fontname = 'Arial'",
                                             view_type = "all",
                                             col_attr = c("column", "type", "mandatory"))

# Export graph
dm_export_graph(sf_event_encounter_diagram, file_name = "PSSFSurveyEventDiagram.pdf")

#=====================================================================================
# Generate Harvest Estimate diagram
#=====================================================================================

# Define harvest estimation segments
harvest_segments = list(
  Location = c("location", "tide_correction"),
  Estimation = c("season", "location_quota", "egress_model", 
                 "egress_model_version", "tide", "tide_strata_lut",
                 "egress_model_type_lut","regulatory_status_lut", 
                 "report_type_lut", "harvest_unit_type_lut",
                 "effort_estimate_type_lut", "species_group_lut", 
                 "season_status_lut"),
  ArchivedEstimates = c("location_resources", "mean_cpue_estimate", 
                     "mean_effort_estimate")
)

# Add segment info
dm_sf_harvest_seg_model = dm_set_segment(dm_sf_model, harvest_segments)

# Set colors
harvest_display <- list(
  accent3 = c("location", "tide_correction"),
  accent4 = c("season", "location_quota", "egress_model", 
              "egress_model_version", "tide"),
  accent6 = c("location_resources", "mean_cpue_estimate", 
              "mean_effort_estimate")
)

# Add color info
dm_sf_harvest_seg_col_model = dm_set_display(dm_sf_harvest_seg_model, harvest_display)

# Set focus on estimation tables
harvest_focus = list(
  tables = c("location", "tide_correction", "season", 
             "location_quota", "egress_model", 
             "egress_model_version", "tide", "tide_strata_lut",
             "egress_model_type_lut","regulatory_status_lut", 
             "report_type_lut", "harvest_unit_type_lut",
             "effort_estimate_type_lut", "species_group_lut", 
             "season_status_lut", "location_resources", 
             "mean_cpue_estimate", "mean_effort_estimate")
)

# Inspect type values
unique(dm_sf_harvest_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_harvest_seg_col_model[["columns"]]$type[dm_sf_harvest_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geography"
dm_sf_harvest_seg_col_model[["columns"]]$type[dm_sf_harvest_seg_col_model[["columns"]]$type == "timestamp with time zone"] = "timestamptz"

# Inspect type values
unique(dm_sf_harvest_seg_col_model[["columns"]]$type)

# Inspect mandatory values
unique(dm_sf_harvest_seg_col_model[["columns"]]$mandatory)

# Update some column values
dm_sf_harvest_seg_col_model[["columns"]]$mandatory[dm_sf_harvest_seg_col_model[["columns"]]$mandatory == 1L] = "not null"
dm_sf_harvest_seg_col_model[["columns"]]$mandatory[dm_sf_harvest_seg_col_model[["columns"]]$mandatory == "0"] = "nullable"

# Inspect type values
unique(dm_sf_harvest_seg_col_model[["columns"]]$mandatory)

# Create location diagram
sf_harvest_estimate_diagram = dm_create_graph(dm_sf_harvest_seg_col_model, 
                                              focus = harvest_focus,
                                              graph_attrs = "rankdir = 'RL', bgcolor = '#f2f3f5' ", 
                                              edge_attrs = "dir = both, arrowtail = crow, arrowhead = odiamond",
                                              node_attrs = "fontname = 'Arial'",
                                              view_type = "all",
                                              col_attr = c("column", "type", "mandatory"))

# Export graph
dm_export_graph(sf_harvest_estimate_diagram, file_name = "PSSFHarvestEstimateDiagram.pdf")



