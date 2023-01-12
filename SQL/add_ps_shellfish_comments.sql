
-- Create comments to tables and columns
-- Created 2023-01-09

-- agency_lut
COMMENT ON TABLE public.agency_lut IS 'The internal or external agency that conducted the survey';
COMMENT ON COLUMN agency_lut.agency_id IS 'Universally unique primary key';
COMMENT ON COLUMN agency_lut.agency_code IS 'The agency code';
COMMENT ON COLUMN agency_lut.agency_name IS 'The name of the agency';

-- area_surveyed_lut
COMMENT ON TABLE public.area_surveyed_lut IS 'Description of the extent of a site that was surveyed';
COMMENT ON COLUMN area_surveyed_lut.area_surveyed_id IS 'Universally unique primary key';
COMMENT ON COLUMN area_surveyed_lut.area_surveyed_description IS 'Description of area categories (entire beach, most of beach, etc.)';

-- catch_result_type_lut
COMMENT ON TABLE public.catch_result_type_lut IS 'End result of species encounter (kept, released, etc.)';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_code IS 'Short code for catch result categories';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_description IS 'Description of catch result categories';

-- data_review_status_lut
COMMENT ON TABLE public.data_review_status_lut IS 'Status of data review process';
COMMENT ON COLUMN data_review_status_lut.data_review_status_id IS 'Universally unique primary key';
COMMENT ON COLUMN data_review_status_lut.data_review_status_description IS 'Description of review categories (Preliminary, Final, etc.)';

-- effort_estimate_type_lut
COMMENT ON TABLE public.effort_estimate_type_lut IS 'Categories of estimate types (Harvester-day, Point count, etc)';
COMMENT ON COLUMN effort_estimate_type_lut.effort_estimate_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN effort_estimate_type_lut.effort_estimate_type_description IS 'Description of the estimate type categories';

-- egress_model
COMMENT ON TABLE public.egress_model IS 'Egress model values applied to expand harvester counts';
COMMENT ON COLUMN egress_model.egress_model_id IS 'Universally unique primary key';
COMMENT ON COLUMN egress_model.egress_model_version_id IS 'The version of the model applied';
COMMENT ON COLUMN egress_model.egress_model_interval IS 'Model interval in increments of one minute. Typically 180 minutes either side of low tide';
COMMENT ON COLUMN egress_model.egress_model_ratio IS 'The model ratio used to expand aerial harvester counts';
COMMENT ON COLUMN egress_model.egress_model_variance IS 'The variance of the model';

-- egress_model_type_lut
COMMENT ON TABLE public.egress_model_type_lut IS 'Categories of egress models used to expand harvester counts (Normal, Early peak, etc)';
COMMENT ON COLUMN egress_model_type_lut.egress_model_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN egress_model_type_lut.egress_model_type_code IS 'Code for the egress model types';
COMMENT ON COLUMN egress_model_type_lut.egress_model_type_description IS 'Description of the egress model categories';

-- egress_model_version
COMMENT ON TABLE public.egress_model_version IS 'All versions of egress models, current and historical, applied to expand harvester counts.';
COMMENT ON COLUMN egress_model_version.egress_model_version_id IS 'Universally unique primary key';
COMMENT ON COLUMN egress_model_version.egress_model_type_id IS 'Categories of egress models (Normal, Early peak, etc)';
COMMENT ON COLUMN egress_model_version.tide_strata_id IS 'Type of tide strata (HIGH, LOW, etc.)';
COMMENT ON COLUMN egress_model_version.egress_model_name IS 'Name of the egress model';
COMMENT ON COLUMN egress_model_version.egress_model_description IS 'Description of the egress model';
COMMENT ON COLUMN egress_model_version.egress_model_source IS 'Publication or report where egress model was defined';
COMMENT ON COLUMN egress_model_version.inactive_indicator IS 'Has the model been inactivated or superceeded?';
COMMENT ON COLUMN egress_model_version.inactive_datetime IS 'Date when model was inactivated';
COMMENT ON COLUMN egress_model_version.inactive_reason IS 'The reason model was inactivated';
COMMENT ON COLUMN egress_model_version.comment_text IS 'Additional comments on egress model';

-- harvest_depth_range_lut
COMMENT ON TABLE public.harvest_depth_range_lut IS 'Approximate depth fished. Intended for non-bivalve fisheries (crab, shrimp)';
COMMENT ON COLUMN harvest_depth_range_lut.harvest_depth_range_id IS 'Universally unique primary key';
COMMENT ON COLUMN harvest_depth_range_lut.harvest_depth_range IS 'Depth range';
COMMENT ON COLUMN harvest_depth_range_lut.depth_range_description IS 'Description of depth range';

-- harvest_gear_type_lut
COMMENT ON TABLE public.harvest_gear_type_lut IS 'Categories of harvest gear type. Intended for non-bivalve fisheries (crab, shrimp)';
COMMENT ON COLUMN harvest_gear_type_lut.harvest_gear_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN harvest_gear_type_lut.harvest_gear_type_code IS 'Code for gear type';
COMMENT ON COLUMN harvest_gear_type_lut.gear_type_description IS 'Full description of gear type';

-- harvest_method_lut
COMMENT ON TABLE public.harvest_method_lut IS 'Categories of harvest method. Intended for non-bivalve fisheries (crab, shrimp)';
COMMENT ON COLUMN harvest_method_lut.harvest_method_id IS 'Universally unique primary key';
COMMENT ON COLUMN harvest_method_lut.harvest_method_code IS 'Code for harvest method';
COMMENT ON COLUMN harvest_method_lut.harvest_method_description IS 'Full description of harvest method';

-- harvest_unit_type_lut
COMMENT ON TABLE public.harvest_unit_type_lut IS 'Categories of harvest unit types reported (Counts, Pounds, etc)';
COMMENT ON COLUMN harvest_unit_type_lut.harvest_unit_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN harvest_unit_type_lut.harvest_unit_type_code IS 'Code for the harvest unit types';
COMMENT ON COLUMN harvest_unit_type_lut.harvest_unit_type_description IS 'Description of the harvest unit categories';

-- harvester_type_lut
COMMENT ON TABLE public.harvester_type_lut IS 'Categories of harvester type (rec-clam, tribe-clam, etc.)';
COMMENT ON COLUMN harvester_type_lut.harvester_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN harvester_type_lut.harvester_type_code IS 'Code for harvester type';
COMMENT ON COLUMN harvester_type_lut.harvester_type_description IS 'Full description of harvester type';

-- individual_species
COMMENT ON TABLE public.individual_species IS 'Data for individual species';
COMMENT ON COLUMN individual_species.individual_species_id IS 'Universally unique primary key';
COMMENT ON COLUMN individual_species.species_encounter_id IS 'Link to species_encounter table';
COMMENT ON COLUMN individual_species.sex_id IS 'Sex of the species. Intended for non-bivalve fisheries';
COMMENT ON COLUMN individual_species.species_sample_number IS 'Unique sample number';
COMMENT ON COLUMN individual_species.weight_measurement_gram IS 'Weight of species in grams';
COMMENT ON COLUMN individual_species.length_measurement_millimeter IS 'Length of species in millimeters';
COMMENT ON COLUMN individual_species.comment_text IS 'Additional comments on individual species';

-- location
COMMENT ON TABLE public.location IS 'Data associated with locations';
COMMENT ON COLUMN location.location_id IS 'Universally unique primary key';
COMMENT ON COLUMN location.location_type_id IS 'Type of location (harvester count location, tide station, etc.)';
COMMENT ON COLUMN location.location_code IS 'Short code to identify location';
COMMENT ON COLUMN location.location_name IS 'Name of location (place name, etc.)';
COMMENT ON COLUMN location.location_description IS 'Extended description of location';
COMMENT ON COLUMN location.comment_text IS 'Additional comments regarding location';

-- location_coordinates
COMMENT ON TABLE public.location_coordinates IS 'Spatial table holding coordinates for point location records';
COMMENT ON COLUMN location_coordinates.location_coordinates_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_coordinates.location_id IS 'Link to location table';
COMMENT ON COLUMN location_coordinates.horizontal_accuracy IS 'Estimated accuracy of the point in meters';
COMMENT ON COLUMN location_coordinates.gid IS 'Unique integer ID for geography';
COMMENT ON COLUMN location_coordinates.geog IS 'Coordinates stored in binary format. EPSG:4326';

-- location_boundary
COMMENT ON TABLE public.location_boundary IS 'Spatial table holding polygons for beaches, management areas, etc.';
COMMENT ON COLUMN location_boundary.location_boundary_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_boundary.location_id IS 'Link to location table';
COMMENT ON COLUMN location_boundary.boundary_code IS 'Short code to identify area';
COMMENT ON COLUMN location_boundary.boundary_name IS 'Name of the enclosed area';
COMMENT ON COLUMN location_boundary.active_datetime IS 'DateTime when polygon became active';
COMMENT ON COLUMN location_boundary.inactive_datetime IS 'DateTime when polygon became inactive';
COMMENT ON COLUMN location_boundary.inactive_reason IS 'The reason the polygon became inactive';
COMMENT ON COLUMN location_boundary.gid IS 'Unique integer ID for geography';
COMMENT ON COLUMN location_boundary.geog IS 'Polygon features stored in binary format. EPSG:4326';

-- location_route
COMMENT ON TABLE public.location_route IS 'Spatial table holding line features for flight routes, transects, etc.';
COMMENT ON COLUMN location_route.location_route_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_route.location_id IS 'Link to location table';
COMMENT ON COLUMN location_route.route_code IS 'Short code to identify line feature';
COMMENT ON COLUMN location_route.route_name IS 'Name of the enclosed line feature';
COMMENT ON COLUMN location_route.active_datetime IS 'DateTime when line feature became active';
COMMENT ON COLUMN location_route.inactive_datetime IS 'DateTime when line feature became inactive';
COMMENT ON COLUMN location_route.inactive_reason IS 'The reason the line feature became inactive';
COMMENT ON COLUMN location_boundary.gid IS 'Unique integer ID for geography';
COMMENT ON COLUMN location_boundary.geog IS 'LineString features stored in binary format. EPSG:4326';

-- location_inventory
COMMENT ON TABLE public.location_inventory IS 'Placeholder for historical assessments of resources on Puget Sound beaches and other areas';
COMMENT ON COLUMN location_inventory.location_inventory_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_inventory.location_id IS 'Link to the location table';
COMMENT ON COLUMN location_inventory.survey_type_id IS 'The type of survey conducted (Clam, Oyster, Walkthrough, etc.)';
COMMENT ON COLUMN location_inventory.species_id IS 'List of shellfish species';
COMMENT ON COLUMN location_inventory.population_estimate_type IS 'The type of population estimate';
COMMENT ON COLUMN location_inventory.population_estimate_unit IS 'The unit (count, pounds, etc.) of the estimate';
COMMENT ON COLUMN location_inventory.survey_completed_datetime IS 'The date the survey was completed?';
COMMENT ON COLUMN location_inventory.population_estimate IS 'The numeric value of the estimate';
COMMENT ON COLUMN location_inventory.comment_text IS 'Additional comments on the resources inventoried';

-- location_quota
COMMENT ON TABLE public.location_quota IS 'Harvest quota data for specific beaches or management areas';
COMMENT ON COLUMN location_quota.location_quota_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_quota.location_id IS 'Link to the location table';
COMMENT ON COLUMN location_quota.regulatory_status_id IS 'Regulatory status of beach or management area (Active, Passive, etc)';
COMMENT ON COLUMN location_quota.effort_estimate_type_id IS 'The type of estimate that needs to be reported (Harvester-day, Point, etc)';
COMMENT ON COLUMN location_quota.egress_model_type_id IS 'The type of egress model applied to harvester counts (Normal, Early peak, etc)';
COMMENT ON COLUMN location_quota.species_group_id IS 'The group of species the quota applies to (clams, oysters, both)';
COMMENT ON COLUMN location_quota.report_type_id IS 'The type of report that needs to be generated (External harvest, Internal effort, etc.)';
COMMENT ON COLUMN location_quota.harvest_unit_type_id IS 'The type of harvest unit that needs to be reported (Counts, Pounds, etc)';
COMMENT ON COLUMN location_quota.quota_year IS 'The year that the allowed quota applies to';
COMMENT ON COLUMN location_quota.allowable_harvest IS 'The allowed quota';
COMMENT ON COLUMN location_quota.comment_text IS 'Additional comments on the quota allowance';

-- location_type_lut
COMMENT ON TABLE public.location_type_lut IS 'Type of location (harvester count location, tide station, etc.)';
COMMENT ON COLUMN location_type_lut.location_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN location_type_lut.location_type_description IS 'Description of location type';

-- media_location
COMMENT ON TABLE public.media_location IS 'Identifies where to retrieve media (photo, video, audio files, etc.) recorded at specific locations';
COMMENT ON COLUMN media_location.media_location_id IS 'Universally unique primary key';
COMMENT ON COLUMN media_location.location_id IS 'Link to the location table';
COMMENT ON COLUMN media_location.media_type_id IS 'The type of media that was recorded (photo, video, audio, etc.)';
COMMENT ON COLUMN media_location.media_url IS 'The secure file or web location where media files are stored';

-- media_type_lut
COMMENT ON TABLE public.media_type_lut IS 'Type of media that was recorded (photo, video, audio, etc.)';
COMMENT ON COLUMN media_type_lut.media_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN media_type_lut.media_type_code IS 'Code describing the type of media';
COMMENT ON COLUMN media_type_lut.media_type_description IS 'Description of the media type';

-- mean_effort_estimate
COMMENT ON TABLE public.mean_effort_estimate IS 'Storage table for annual recreational clam and oyster harvest effort estimates';
COMMENT ON COLUMN mean_effort_estimate.mean_effort_estimate_id IS 'Universally unique primary key';
COMMENT ON COLUMN mean_effort_estimate.location_id IS 'Link to the location table. Specifies the beach';
COMMENT ON COLUMN mean_effort_estimate.location_code IS 'The Beach Identification Number (BIDN) of the beach';
COMMENT ON COLUMN mean_effort_estimate.location_name IS 'Name of the beach';
COMMENT ON COLUMN mean_effort_estimate.estimation_year IS 'The year for which the estimate was calculated';
COMMENT ON COLUMN mean_effort_estimate.tide_strata IS 'The tide strata for the estimate (ELOW, HIGH, LOW, etc.)';
COMMENT ON COLUMN mean_effort_estimate.flight_season IS 'The flight season designation (OO, OC, etc.)';
COMMENT ON COLUMN mean_effort_estimate.mean_effort IS 'Numeric value of the mean effort';
COMMENT ON COLUMN mean_effort_estimate.tide_count IS 'The number of tide events included in calculation of the mean';

-- mean_cpue_estimate
COMMENT ON TABLE public.mean_cpue_estimate IS 'Storage table for annual recreational Catch Per Unit Effort (CPUE) estimates';
COMMENT ON COLUMN mean_cpue_estimate.mean_cpue_estimate_id IS 'Universally unique primary key';
COMMENT ON COLUMN mean_cpue_estimate.location_id IS 'Link to the location table. Specifies the beach';
COMMENT ON COLUMN mean_cpue_estimate.location_code IS 'The Beach Identification Number (BIDN) of the beach';
COMMENT ON COLUMN mean_cpue_estimate.location_name IS 'Name of the beach';
COMMENT ON COLUMN mean_cpue_estimate.estimation_year IS 'The year for which the estimate was calculated';
COMMENT ON COLUMN mean_cpue_estimate.flight_season IS 'The flight season designation (OO, OC, etc.)';
COMMENT ON COLUMN mean_cpue_estimate.species_code IS 'The species for which the estimate was calculated';
COMMENT ON COLUMN mean_cpue_estimate.survey_count IS 'The number of surveys included in calculation of the mean';
COMMENT ON COLUMN mean_cpue_estimate.mean_cpue IS 'Numeric value of the mean CPUE';

-- mobile_device
COMMENT ON TABLE public.mobile_device IS 'Stores data for tracking mobile devices';
COMMENT ON COLUMN mobile_device.mobile_device_id IS 'Universally unique primary key';
COMMENT ON COLUMN mobile_device.mobile_device_type_id IS 'Type of device (Pad, GPS, etc.)';
COMMENT ON COLUMN mobile_device.mobile_equipment_identifier IS 'Typically the MEID number printed on device';
COMMENT ON COLUMN mobile_device.mobile_device_name IS 'Name of device';
COMMENT ON COLUMN mobile_device.mobile_device_description IS 'Description of device';

-- mobile_device_type_lut
COMMENT ON TABLE public.mobile_device_type_lut IS 'Type of mobile device';
COMMENT ON COLUMN mobile_device_type_lut.mobile_device_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN mobile_device_type_lut.mobile_device_type_description IS 'Type of device (Pad, GPS, etc.)';

-- mobile_survey_form
COMMENT ON TABLE public.mobile_survey_form IS 'Table for tracking source of data collected with mobile device';
COMMENT ON COLUMN mobile_survey_form.mobile_survey_form_id IS 'Universally unique primary key';
COMMENT ON COLUMN mobile_survey_form.survey_id IS 'Link to survey table';
COMMENT ON COLUMN mobile_survey_form.parent_form_name IS 'Name of the parent form used to record data';
COMMENT ON COLUMN mobile_survey_form.parent_form_id IS 'ID of the parent form used to record data';

-- regulatory_status_lut
COMMENT ON TABLE public.regulatory_status_lut IS 'Regulatory status categories (Active, Passive, etc)';
COMMENT ON COLUMN regulatory_status_lut.regulatory_status_id IS 'Universally unique primary key';
COMMENT ON COLUMN regulatory_status_lut.status_code IS 'Code for the regulatory status categories';
COMMENT ON COLUMN regulatory_status_lut.status_description IS 'Description of the regulatory status categories';

-- report_type_lut
COMMENT ON TABLE public.report_type_lut IS 'Categories of required reporting types (External harvest, Internal effort, etc.)';
COMMENT ON COLUMN report_type_lut.report_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN report_type_lut.report_type_code IS 'Code for the egress model types';
COMMENT ON COLUMN report_type_lut.report_type_description IS 'Description of the egress model categories';

-- sampler
COMMENT ON TABLE public.sampler IS 'Base information for samplers';
COMMENT ON COLUMN sampler.sampler_id IS 'Universally unique primary key';
COMMENT ON COLUMN sampler.first_name IS 'First name of sampler';
COMMENT ON COLUMN sampler.last_name IS 'Last name of sampler';
COMMENT ON COLUMN sampler.active_indicator IS 'Flag to indicate if sampler is still employed';

-- sampling_program_lut
COMMENT ON TABLE public.sampling_program_lut IS 'The organizational unit that conducted the survey';
COMMENT ON COLUMN sampling_program_lut.sampling_program_id IS 'Universally unique primary key';
COMMENT ON COLUMN sampling_program_lut.agency_id IS 'Link to table listing external agencies. Not currently used';
COMMENT ON COLUMN sampling_program_lut.sampling_program_code IS 'Short code for unit';
COMMENT ON COLUMN sampling_program_lut.sampling_program_name IS 'Full name of organizational unit';

-- season
COMMENT ON TABLE public.season IS 'Data for open and closed harvest seasons on recreational beaches';
COMMENT ON COLUMN season.season_id IS 'Universally unique primary key';
COMMENT ON COLUMN season.location_id IS 'Link to location table';
COMMENT ON COLUMN season.season_status_id IS 'Status of the harvest season (open, closed, etc.)';
COMMENT ON COLUMN season.species_group_id IS 'The group of species the season regulation applies to (clams, oysters, both)';
COMMENT ON COLUMN season.season_start_datetime IS 'Start date of the season';
COMMENT ON COLUMN season.season_end_datetime IS 'End date of the season';
COMMENT ON COLUMN season.season_description IS 'Description of the season designation';
COMMENT ON COLUMN season.comment_text IS 'Additional comments on the season designation';

-- season_status_lut
COMMENT ON TABLE public.season_status_lut IS 'Status of the season designation (open, closed, etc)';
COMMENT ON COLUMN season_status_lut.season_status_id IS 'Universally unique primary key';
COMMENT ON COLUMN season_status_lut.season_status_code IS 'Code for the season status categories';
COMMENT ON COLUMN season_status_lut.season_status_description IS 'Description of the season status categories';

-- season_type_lut
COMMENT ON TABLE public.season_type_lut IS 'Type of season. Intended for non-bivalve fisheries (crab, shrimp)';
COMMENT ON COLUMN season_type_lut.season_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN season_type_lut.season_type_code IS 'Code for season type';
COMMENT ON COLUMN season_type_lut.season_type_description IS 'Full description of season type';

-- sex_lut
COMMENT ON TABLE public.sex_lut IS 'Sex of species (male, female, unknown, etc). Intended for non-bivalve fisheries';
COMMENT ON COLUMN sex_lut.sex_id IS 'Universally unique primary key';
COMMENT ON COLUMN sex_lut.sex_code IS 'Code for sex categories';
COMMENT ON COLUMN sex_lut.sex_description IS 'Description of sex categories';

-- shell_condition_lut
COMMENT ON TABLE public.shell_condition_lut IS 'Condition of the shell (broken, unbroken, hardness category, etc.)';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_code IS 'Short code for shell condition categories';
COMMENT ON COLUMN catch_result_type_lut.catch_result_type_description IS 'Description of shell condition categories';

-- species_encounter
COMMENT ON TABLE public.species_encounter IS 'Data for species encounters';
COMMENT ON COLUMN species_encounter.species_encounter_id IS 'Universally unique primary key';
COMMENT ON COLUMN species_encounter.survey_event_id IS 'Link to survey_event table';
COMMENT ON COLUMN species_encounter.species_id IS 'Species that was encountered';
COMMENT ON COLUMN species_encounter.species_location_id IS 'Link to location table';
COMMENT ON COLUMN species_encounter.catch_result_type_id IS 'Was species kept or released';
COMMENT ON COLUMN species_encounter.shell_condition_id IS 'Categories of shell condition (broken, intact, etc.)';
COMMENT ON COLUMN species_encounter.species_count IS 'Number of the species counted';
COMMENT ON COLUMN species_encounter.species_weight_gram IS 'Weight of the species in grams';
COMMENT ON COLUMN species_encounter.no_head_indicator IS 'Intended for shrimp fisheries. Was the head removed?';
COMMENT ON COLUMN species_encounter.comment_text IS 'Additional comment on species encounter';

-- species_group_lut
COMMENT ON TABLE public.species_group_lut IS 'The group of species affected by the season regulation (clams, oysters, both)';
COMMENT ON COLUMN species_group_lut.species_group_id IS 'Universally unique primary key';
COMMENT ON COLUMN species_group_lut.species_group_code IS 'Code for the species group categories';
COMMENT ON COLUMN species_group_lut.species_group_description IS 'Description of the species group categories';

-- species_lut
COMMENT ON TABLE public.species_lut IS 'List of shellfish species';
COMMENT ON COLUMN species_lut.species_id IS 'Universally unique primary key';
COMMENT ON COLUMN species_lut.taxo_name_id IS 'Link to the WDFW Taxo_Name table';
COMMENT ON COLUMN species_lut.species_code IS 'Short code for species';
COMMENT ON COLUMN species_lut.common_name IS 'Common name of species';
COMMENT ON COLUMN species_lut.genus IS 'The genus name';
COMMENT ON COLUMN species_lut.species IS 'The species name';

-- survey
COMMENT ON TABLE public.survey IS 'Top level header information for surveys';
COMMENT ON COLUMN survey.survey_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey.survey_type_id IS 'The type of survey conducted (aerial, creel, etc.)';
COMMENT ON COLUMN survey.sampling_program_id IS 'The organizational unit that conducted the survey';
COMMENT ON COLUMN survey.location_id IS 'Link to the location table, describes where the survey occurred';
COMMENT ON COLUMN survey.area_surveyed_id IS 'Was entire beach surveyed (all, portion, etc.)';
COMMENT ON COLUMN survey.data_review_status_id IS 'Status of data review (final, preliminary, etc.)';
COMMENT ON COLUMN survey.survey_completion_status_id IS 'Was survey completed (partial, completed, etc.)';
COMMENT ON COLUMN survey.survey_datetime IS 'Date of the survey';
COMMENT ON COLUMN survey.start_datetime IS 'Time of day when survey started';
COMMENT ON COLUMN survey.end_datetime IS 'Time of day when survey ended';
COMMENT ON COLUMN survey.comment_text IS 'Comments on survey';

-- survey_completion_status_lut
COMMENT ON TABLE public.survey_completion_status_lut IS 'Was the survey completed?';
COMMENT ON COLUMN survey_completion_status_lut.survey_completion_status_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey_completion_status_lut.completion_status_description IS 'Description of status categories';

-- survey_event
COMMENT ON TABLE public.survey_event IS 'Data for events occurring during a survey';
COMMENT ON COLUMN survey_event.survey_event_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey_event.survey_id IS 'Link to survey table';
COMMENT ON COLUMN survey_event.event_location_id IS 'Link to location table';
COMMENT ON COLUMN survey_event.harvester_type_id IS 'Type of harvester counted (rec-clam, tribe-clam, etc.)';
COMMENT ON COLUMN survey_event.harvest_method_id IS 'Type of method used by harvester. Placeholder for non-bivalve fisheries';
COMMENT ON COLUMN survey_event.harvest_gear_type_id IS 'Type of gear used by harvester. Placeholder for non-bivalve fisheries';
COMMENT ON COLUMN survey_event.harvest_depth_range_id IS 'The approximate depth fished. Placeholder for non-bivalve fisheries';
COMMENT ON COLUMN survey_event.event_number IS 'Sequence number for interview or encounter';
COMMENT ON COLUMN survey_event.event_datetime IS 'Time that interview or encounter occurred';
COMMENT ON COLUMN survey_event.harvester_count IS 'Number of harvesters counted';
COMMENT ON COLUMN survey_event.harvest_gear_count IS 'The number of units of gear counted. Placeholder for non-bivalve fisheries';
COMMENT ON COLUMN survey_event.harvester_zip_code IS 'The zip-code of the harvester. Intended for shrimp fisheries';
COMMENT ON COLUMN survey_event.comment_text IS 'Additional comment on survey event';

-- survey_mobile_device
COMMENT ON TABLE public.survey_mobile_device IS 'Associative table between survey table and mobile_device table';
COMMENT ON COLUMN survey_mobile_device.survey_mobile_device_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey_mobile_device.survey_id IS 'Link to survey table';
COMMENT ON COLUMN survey_mobile_device.mobile_device_id IS 'Link to mobile_device table';

-- survey_sampler
COMMENT ON TABLE public.survey_sampler IS 'Associative table between survey and sampler tables';
COMMENT ON COLUMN survey_sampler.survey_sampler_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey_sampler.survey_id IS 'Link to survey table';
COMMENT ON COLUMN survey_sampler.sampler_id IS 'Link to sampler table';

-- survey_type_lut
COMMENT ON TABLE public.survey_type_lut IS 'The type of survey conducted (aerial, creel, etc.)';
COMMENT ON COLUMN survey_type_lut.survey_type_id IS 'Universally unique primary key';
COMMENT ON COLUMN survey_type_lut.survey_type_code IS 'Short code for survey type';
COMMENT ON COLUMN survey_type_lut.survey_type_description IS 'Description of survey type';

-- tide
COMMENT ON TABLE public.tide IS 'Tide data (highs and lows only) for primary Puget Sound reference stations 1970-2056';
COMMENT ON COLUMN tide.tide_id IS 'Universally unique primary key';
COMMENT ON COLUMN tide.tide_strata_id IS 'WDFW intertidal project tide strata used for harvest estimation (HIGH, LOW, etc.)';
COMMENT ON COLUMN tide.tide_station_location_id IS 'Link to the location table';
COMMENT ON COLUMN tide.low_tide_datetime IS 'Date and time of the high and low tide';
COMMENT ON COLUMN tide.tide_time_minutes IS 'Time of the tide in minutes from midnight';
COMMENT ON COLUMN tide.tide_height_feet IS 'Height of the tide in feet';

-- tide_correction
COMMENT ON TABLE public.tide_correction IS 'Tide corrections and descriptive information for specific locations such as beaches';
COMMENT ON COLUMN tide_correction.location_id IS 'Universally unique primary key';
COMMENT ON COLUMN tide_correction.tide_station_location_id IS 'Link to the location table';
COMMENT ON COLUMN tide_correction.low_tide_correction_minutes IS 'Low tide correction factor in minutes';
COMMENT ON COLUMN tide_correction.low_tide_correction_feet IS 'Low tide correction factor in feet';
COMMENT ON COLUMN tide_correction.high_tide_correction_minutes IS 'High tide correction factor in minutes';
COMMENT ON COLUMN tide_correction.high_tide_correction_feet IS 'High tide correction factor in feet';

-- tide_strata_lut
COMMENT ON TABLE public.tide_strata_lut IS 'Categories of tide strata used for harvest estimation (HIGH, LOW, etc.)';
COMMENT ON COLUMN tide_strata_lut.tide_strata_id IS 'Universally unique primary key';
COMMENT ON COLUMN tide_strata_lut.tide_strata_code IS 'Code for the tide strata';
COMMENT ON COLUMN tide_strata_lut.tide_strata_description IS 'Description of the tide strata categories';




