USE ROLE ACCOUNTADMIN;

use warehouse compute_wh;

CREATE OR REPLACE USER  dc_user 
  PASSWORD = 'welcome1'
  LOGIN_NAME = dc_user
  DEFAULT_ROLE = PUBLIC;
  
CREATE or REPLACE security integration salesforce_dc_integration
type = oauth
oauth_client = custom
oauth_client_type = 'CONFIDENTIAL'
OAUTH_ALLOW_NON_TLS_REDIRECT_URI=true
oauth_redirect_uri
='https://login.salesforce.com/services/cdpSnowflakeOAuthCallback'
enabled = true
oauth_issue_refresh_tokens = true
oauth_refresh_token_validity = 86400;

-- The above query will show the client secret for the integration created above.
select 
 key, trim(value,'""')
FROM TABLE (
    FLATTEN(
     input => PARSE_JSON(SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SALESFORCE_DC_INTEGRATION') )
    )
) where key != 'OAUTH_CLIENT_SECRET_2';


