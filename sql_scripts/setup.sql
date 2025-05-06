-- =========================
-- Define Databases
-- =========================
create or replace database SALESFORCE_LAI_DEMO_DB comment = 'used for demonstrating Snowflake for Salesforce Agentforce demo';
create or replace schema SALESFORCE_LAI_DEMO_DB.DEMO;

-- =========================
-- Define stages
-- =========================
use schema SALESFORCE_LAI_DEMO_DB.DEMO;

create or replace stage lib_stg 
	directory = ( enable = true )
    comment = 'used for holding udfs and procs.';

create or replace stage data_stg directory = (enable = true)
    comment = 'used for holding data.';

create or replace stage scripts_stg 
    comment = 'used for holding scripts.';


-- --------------------------------------

use database SALESFORCE_LAI_DEMO_DB;
use schema demo;