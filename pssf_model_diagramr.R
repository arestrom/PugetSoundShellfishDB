#=================================================================
# Functions to create table outputs for data dictionary
# 
# NOTES: 
#  1. Should now be able to do using only DM. Need to test
#
# AS 2022-10-25
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
db_con = pg_con_local(dbname = "shellfish")

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
drop_tables = c("spatial_ref_sys", "agency_lut")

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
  Survey = c("survey", "survey_sampler", "sampler", "survey_type_lut", 
             "sampling_program_lut", "area_surveyed_lut", "data_review_status_lut",
             "survey_completion_status_lut"),
  Location = c("point_location", "location_type_lut", "management_region_lut",
               "shellfish_management_area_lut", "beach", "beach_boundary_history",
               "beach_intertidal_area"),
  Estimation = c("beach_season", "season_status_lut", "beach_allowance", 
                 "beach_status_lut","effort_estimate_type_lut", "egress_model",
                 "egress_model_type_lut", "species_group_lut", "report_type_lut", 
                 "harvest_unit_type_lut","egress_model_version", "mean_cpue_estimate", 
                 "mean_effort_estimate", "tide", "tide_strata_lut", 
                 "beach_inventory"),
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
  accent5 = c("survey", "survey_sampler", "sampler"),
  accent6 = c("mobile_survey_form", "mobile_device", "survey_mobile_device"),
  accent1 = c("survey_event" ,"species_encounter", "individual_species"),
  accent3 = c("point_location", "management_region_lut", "beach", 
              "beach_boundary_history", "beach_intertidal_area",
              "shellfish_management_area_lut", "beach_inventory"),
  accent4 = c("beach_season", "beach_allowance", "beach_inventory", 
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
dm_export_graph(sf_title_diagram, file_name = "Storage/DataDictionary/SFTitleDiagram.pdf")

#=====================================================================================
#  Generate location tables diagram
#=====================================================================================

# Define location segments
location_segments = list(
  Survey = c("survey", "survey_event"),
  Location = c("point_location", "beach", "beach_intertidal_area", 
               "location_type_lut", "beach_boundary_history", 
               "management_region_lut", "shellfish_management_area_lut"),
  SpeciesEncounter = c("species_encounter")
)

# Add segment info
dm_sf_loc_seg_model = dm_set_segment(dm_sf_model, location_segments)

# Set colors
location_display <- list(
  accent5 = c("survey", "survey_event"),
  accent3 = c("point_location", "beach", "beach_intertidal_area",
              "beach_boundary_history", "management_region_lut", 
              "shellfish_management_area_lut"),
  accent1 = c("species_encounter", "individual_species")
)

# Add color info
dm_sf_loc_seg_col_model = dm_set_display(dm_sf_loc_seg_model, location_display)

# Set focus on location tables
location_focus = list(
  tables = c("point_location", "beach", "beach_intertidal_area", 
             "beach_boundary_history", "management_region_lut",
             "shellfish_management_area_lut", "location_type_lut", 
             "species_encounter", "survey", "survey_event")
)

# Inspect type values
unique(dm_sf_loc_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_loc_seg_col_model[["columns"]]$type[dm_sf_loc_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geometry"
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
dm_export_graph(sf_location_diagram, file_name = "Storage/DataDictionary/SFLocationDiagram.pdf")

#=====================================================================================
#  Generate survey level diagram
#=====================================================================================

# Define location segments
survey_segments = list(
  Survey = c("survey", "survey_type_lut", "sampling_program_lut", 
             "data_review_status_lut", "area_surveyed_lut", 
             "survey_completion_status_lut"),
  Location = c("point_location", "beach"),
  Sampler = c("survey_sampler", "sampler"),
  MobileData = c("survey_mobile_device", "mobile_device", "mobile_device_type_lut",
                 "mobile_survey_form")
)

# Add segment info
dm_sf_survey_seg_model = dm_set_segment(dm_sf_model, survey_segments)

# Set colors
survey_display <- list(
  accent5 = c("survey"),
  accent3 = c("point_location", "beach"),
  accent4 = c("survey_sampler", "sampler"),
  accent6 = c("survey_mobile_device", 
              "mobile_device", "mobile_survey_form")
)

# Add color info
dm_sf_survey_seg_col_model = dm_set_display(dm_sf_survey_seg_model, survey_display)

# Set focus on survey tables
survey_focus = list(
  tables = c("survey", "survey_type_lut", "sampling_program_lut", 
             "data_review_status_lut", "area_surveyed_lut", 
             "survey_completion_status_lut", "point_location", "beach",
             "survey_sampler", "sampler", "survey_mobile_device", 
             "mobile_device", "mobile_device_type_lut", "mobile_survey_form")
)

# Inspect type values
unique(dm_sf_survey_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_survey_seg_col_model[["columns"]]$type[dm_sf_survey_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geometry"
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
dm_export_graph(sf_survey_level_diagram, file_name = "Storage/DataDictionary/SFSurveyDiagram.pdf")


#=====================================================================================
# Generate Survey Event diagram
#=====================================================================================

# Define event segments
event_segments = list(
  Survey = c("survey"),
  Location = c("point_location", "beach"),
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
  accent3 = c("point_location", "beach"),
  accent4 = c("species_encounter"),
  accent1 = c("survey_event"),
  accent7 = c("individual_species")
)

# Add color info
dm_sf_event_seg_col_model = dm_set_display(dm_sf_event_seg_model, event_display)

# Set focus on event tables
event_focus = list(
  tables = c("survey", "survey_event", "point_location", "beach", 
             "harvest_depth_range_lut", "harvest_gear_type_lut", 
             "harvester_type_lut", "harvest_method_lut", 
             "species_encounter", "species_lut", "catch_result_type_lut", 
             "shell_condition_lut","individual_species", "sex_lut")
)

# Inspect type values
unique(dm_sf_event_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_event_seg_col_model[["columns"]]$type[dm_sf_event_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geometry"
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
dm_export_graph(sf_event_encounter_diagram, file_name = "Storage/DataDictionary/SFSurveyEventDiagram.pdf")

#=====================================================================================
# Generate Harvest Estimate diagram
#=====================================================================================

# Define harvest estimation segments
harvest_segments = list(
  Location = c("point_location", "beach"),
  Estimation = c("beach_season", "beach_allowance", "egress_model", 
                 "egress_model_version", "tide", "tide_strata_lut",
                 "egress_model_type_lut","beach_status_lut", 
                 "report_type_lut", "harvest_unit_type_lut",
                 "effort_estimate_type_lut", "species_group_lut", 
                 "season_status_lut"),
  PriorEstimates = c("beach_inventory", "mean_cpue_estimate", 
                     "mean_effort_estimate")
)

# Add segment info
dm_sf_harvest_seg_model = dm_set_segment(dm_sf_model, harvest_segments)

# Set colors
harvest_display <- list(
  accent3 = c("point_location", "beach"),
  accent4 = c("beach_season", "beach_allowance", "egress_model", 
              "egress_model_version", "tide"),
  accent6 = c("beach_inventory", "mean_cpue_estimate", 
              "mean_effort_estimate")
)

# Add color info
dm_sf_harvest_seg_col_model = dm_set_display(dm_sf_harvest_seg_model, harvest_display)

# Set focus on estimation tables
harvest_focus = list(
  tables = c("point_location", "beach", "beach_season", 
             "beach_allowance", "egress_model", 
             "egress_model_version", "tide", "tide_strata_lut",
             "egress_model_type_lut","beach_status_lut", 
             "report_type_lut", "harvest_unit_type_lut",
             "effort_estimate_type_lut", "species_group_lut", 
             "season_status_lut", "beach_inventory", 
             "mean_cpue_estimate", "mean_effort_estimate")
)

# Inspect type values
unique(dm_sf_harvest_seg_col_model[["columns"]]$type)

# Update some column values
dm_sf_harvest_seg_col_model[["columns"]]$type[dm_sf_harvest_seg_col_model[["columns"]]$type == "USER-DEFINED"] = "geometry"
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
dm_export_graph(sf_harvest_estimate_diagram, file_name = "Storage/DataDictionary/SFHarvestEstimateDiagram.pdf")



