-- Set up script for the Hello Snowflake! application.
CREATE APPLICATION ROLE IF NOT EXISTS APP_PUBLIC;
CREATE SCHEMA IF NOT EXISTS CORE;
GRANT USAGE ON SCHEMA CORE TO APPLICATION ROLE APP_PUBLIC;

CREATE SCHEMA IF NOT EXISTS TASKS;
GRANT USAGE ON SCHEMA TASKS TO APPLICATION ROLE APP_PUBLIC;

-- 2nd Part
CREATE OR ALTER VERSIONED SCHEMA CRM;
GRANT USAGE ON SCHEMA CRM TO APPLICATION ROLE APP_PUBLIC;

CREATE OR ALTER VERSIONED SCHEMA PYTHON_FUNCTIONS;
GRANT USAGE ON SCHEMA PYTHON_FUNCTIONS TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE STAGE CORE.LIB_STG directory = (enable = true)
comment = 'used for holding udfs and procs.';

grant read, write on stage CORE.LIB_STG 
    to application role APP_PUBLIC;

create or replace procedure PYTHON_FUNCTIONS.sp_init(app_database varchar)
returns string
language python 
runtime_version = 3.11
handler = 'main'
packages = ('snowflake-snowpark-python', 'yaml')
as 
$$

from io import BytesIO
import snowflake.snowpark
import snowflake.snowpark.types as T
import yaml
import os

def main(session: snowflake.snowpark.Session, app_database: str):

    try:
        # move this to a parameter into the SP
        output_list = []
        output_list.append('Begin')
        
        output_list.append('file read attempt')
        input_file = session.file.get_stream('/salesforce_lai_agent.yaml')
        output_list.append('file read complete')
        output_list.append('yml file creation attempt')
        session.file.put_stream(
            input_stream=input_file,
            stage_location='@CORE.LIB_STG/salesforce_lai_agent.yaml',
            auto_compress = False,
            source_compression = 'NONE',
            parallel = 1, 
            overwrite = True)
        output_list.append('yml file create complete')
        output_list.append('process complete')
        return_str = str(output_list)
    except Exception as e:
        return_str = str(output_list)
    return return_str

$$
;

grant usage on procedure PYTHON_FUNCTIONS.sp_init(varchar) 
    to application role app_public;

CREATE OR REPLACE STREAMLIT CORE.RESORT_AGENT
  FROM '/streamlit'
  MAIN_FILE = '/Home.py'
;

GRANT USAGE ON STREAMLIT CORE.RESORT_AGENT TO APPLICATION ROLE APP_PUBLIC;

CREATE OR REPLACE SECURE VIEW CRM.RESORT_EVENT_UNIFIED_TABLE AS SELECT * FROM SHARED_CONTENT.RESORT_EVENT_UNIFIED_TABLE;
GRANT SELECT ON VIEW CRM.RESORT_EVENT_UNIFIED_TABLE TO APPLICATION ROLE APP_PUBLIC;

