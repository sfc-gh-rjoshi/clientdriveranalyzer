-- Setup script for the Hello Snowflake! application.
CREATE APPLICATION ROLE IF NOT EXISTS admin;
CREATE APPLICATION ROLE IF NOT EXISTS appuser;
create schema if not exists config;
GRANT USAGE ON SCHEMA config TO APPLICATION ROLE admin;
CREATE  schema if not exists data;
GRANT USAGE ON SCHEMA data TO APPLICATION ROLE appuser;
GRANT USAGE ON SCHEMA data TO APPLICATION ROLE admin;
CREATE OR ALTER VERSIONED SCHEMA app;
GRANT USAGE ON SCHEMA app TO APPLICATION ROLE appuser;
GRANT USAGE ON SCHEMA app TO APPLICATION ROLE admin;
create table if not exists config.cda_config 
    (
         conf_name varchar,
         conf_value varchar,
         last_updated_date  DATE
    );
    --sessions_last_update

-- Cannot use insert as it will cause duplicates.  We could have create or replace the table but that not what we want.  Also, I did not want to move this inside create_sessions_objects()
merge into config.cda_config t using (select 'sessions_last_update' as conf_name,LOCALTIMESTAMP()::STRING as conf_value,current_date() as last_updated_date) s
  on s.conf_name=t.conf_name
  WHEN MATCHED THEN UPDATE SET conf_value = LOCALTIMESTAMP()::STRING,last_updated_date=current_date()
  WHEN NOT MATCHED THEN INSERT (conf_name,conf_value,last_updated_date) VALUES ('sessions_last_update',LOCALTIMESTAMP()::STRING,current_date())
;
GRANT SELECT ON table config.cda_config TO APPLICATION ROLE admin;

-- It does not matter it we create it here or inside but safer to create it outside.
create table if not exists data.tbl_deprecation_history 
    (
         WEEK_OF DATE,
         CLIENT_NAME varchar,
         DEPRECATED_COUNT number,
         SUPPORTED_COUNT number
    );
GRANT SELECT,INSERT,UPDATE,DELETE ON table data.tbl_deprecation_history TO APPLICATION ROLE admin;
GRANT SELECT,INSERT,UPDATE,DELETE ON table data.tbl_deprecation_history TO APPLICATION ROLE appuser;
--Need to change this after PoC
CREATE OR REPLACE PROCEDURE app.create_sessions_objects()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
  AS
  BEGIN
    create table if not exists data.tbl_supported_clients
    (
          APP_NAME varchar,
          is_supported boolean
    );
    GRANT SELECT,INSERT,UPDATE,DELETE ON table data.tbl_supported_clients  TO APPLICATION ROLE admin;
    GRANT SELECT ON table data.tbl_supported_clients  TO APPLICATION ROLE appuser;

    truncate table data.tbl_supported_clients;
    insert into data.tbl_supported_clients values 
    ('SnowSQL',TRUE),    ('.NET',TRUE),    ('Go',TRUE),    ('JDBC',TRUE),    ('JavaScript',TRUE),    ('ODBC',TRUE),    ('PHP',TRUE),
    ('PythonConnector',TRUE),    ('PythonSnowpark',TRUE),    ('Snowpark',TRUE),    ('Kafka',FALSE),    ('Spark',FALSE),    ('SnowflakeSQLAlchemy',FALSE),
    ('SNOWPIPE_STREAMING',FALSE),    ('SQLAPI',FALSE),    ('C API',FALSE)
    ;

    create table if not exists data.tbl_client_version 
    (
         APP_NAME varchar,
         client_id varchar,
         recommended_version varchar,
         minimum_version varchar,
         near_end_of_support_version varchar,
         eol_date  DATE
    );
    truncate table data.tbl_client_version;
    insert into data.tbl_client_version  
    (WITH output AS (SELECT    PARSE_JSON(SYSTEM$CLIENT_VERSION_INFO()) a)
    SELECT
        value:clientAppId::STRING AS APP_NAME,
        value:clientId::STRING AS client_id,
        value:recommendedVersion::STRING AS recommended_version,
        value:minimumSupportedVersion::STRING AS minimum_version,
        value:minimumNearingEndOfSupportVersion::STRING AS near_end_of_support_version,
        current_date() as eol_date
      FROM output r,
        LATERAL FLATTEN(INPUT => r.a, MODE =>'array'))
    
    -- ('SnowSQL','SnowSQL','1.2.30','1.2.20','1.2.20',TRUE,'28-Feb-2024'), --1.2.20
    -- ('.NET','.NET Driver','2.1.3','2.0.7','2.0.9',TRUE,'28-Feb-2024'),
    -- ('Go','Go Lang','1.7.0','1.6.3','1.6.6',TRUE,'28-Feb-2024'),
    -- ('JDBC','JDBC','3.14.3','3.13.10','3.13.14',TRUE,'28-Feb-2024'), --3.13.10
    -- ('JavaScript','Node JS','1.9.1','1.6.4','1.6.6',TRUE,'28-Feb-2024'),
    -- ('ODBC','ODBC','3.1.3','2.24.2','2.24.5',TRUE,'28-Feb-2024'),
    -- ('PHP','PHP','2.1.0','1.2.0','1.2.0',TRUE,'28-Feb-2024'),
    -- ('Kafka','Snowflake Connector for Kafka','2.1.1','1.6.0','1.6.5',FALSE,'28-Feb-2024'),
    -- ('PythonConnector','Snowflake Python Connector','3.5.0','2.7.0','2.7.3',TRUE,'28-Feb-2024'),
    -- ('Spark','Snowflake Connector for Spark','2.12.0','2.9.1','2.9.3',FALSE,'28-Feb-2024'),
    -- ('SnowflakeSQLAlchemy','SnowflakeSQLAlchemy','1.5.1','1.3.2','1.3.3',FALSE,'28-Feb-2024'),
    -- ('PythonSnowpark','Snowpark Library for Python','1.10.0','1.0.0','1.0.0',TRUE,'28-Feb-2024'),
    -- ('Snowpark','Snowpark Library for Java/Scala','1.9.0','1.0.0','1.0.0',TRUE,'28-Feb-2024'),
    -- ('SNOWPIPE_STREAMING','SNOWPIPE_STREAMING','1.1.1','1.1.1','1.1.1',FALSE,'28-Feb-2024'),
    -- ('SQLAPI','SQL REST API','1.1.1','1.1.1','1.1.1',FALSE,'28-Feb-2024'),
    -- ('C API','C API','1.1.1','1.1.1','1.1.1',FALSE,'28-Feb-2024')
    ;
    GRANT SELECT,INSERT,UPDATE,DELETE  ON table data.tbl_client_version  TO APPLICATION ROLE admin;
    GRANT SELECT ON table data.tbl_client_version  TO APPLICATION ROLE appuser;
    
    create or replace view data.vw_client_version as 
    select  cv.APP_NAME , client_id,  EOL_DATE, recommended_version as "Recommended Version", minimum_version as "Minimum Supported Version", near_end_of_support_version as "Nearing End of Support", is_supported, try_cast(split_part(near_end_of_support_version, '.' ,1 )::string as INT) as eol_a, try_cast(split_part(near_end_of_support_version, '.' , 2 )::string as INT) as eol_b, try_cast(split_part(near_end_of_support_version, '.' ,3 )::string as INT) as eol_c 
    from data.tbl_client_version cv join data.tbl_supported_clients sc on cv.APP_NAME = sc.APP_NAME;
    
    GRANT SELECT,INSERT,UPDATE,DELETE on view data.vw_client_version  TO APPLICATION ROLE admin;
    GRANT SELECT ON view data.vw_client_version  TO APPLICATION ROLE appuser;
    -- Cannot create outside as we wont have grants to snowflake db on first run
    
    create transient table if not exists data.tbl_sessions as (
    WITH SESH as 
    (SELECT CREATED_ON, CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, PARSE_JSON(CLIENT_ENVIRONMENT):OS AS OS, PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION AS Application,USER_NAME,LOGIN_EVENT_ID
    FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS 
    WHERE 
    DATEDIFF('MONTH', CREATED_ON, CURRENT_DATE() ) <= 3
    AND CLIENT_APPLICATION_ID NOT ILIKE '%SNOWFLAKE UI%' AND CLIENT_APPLICATION_ID NOT ILIKE '%SQLAPI%' 
    AND PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION NOT ILIKE '%Snowflake Web App%'
    ),
    DISTSESH as
    (SELECT CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, OS, Application, USER_NAME,max(LOGIN_EVENT_ID) as M_LOGIN_EVENT_ID, max(CREATED_ON::DATE) as last_seen
    from SESH
    group by CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, OS, Application, USER_NAME
    )
    select CLIENT_APPLICATION_ID as CLIENT_APP_ID, REGEXP_SUBSTR(CLIENT_APPLICATION_ID, '(\\D+)\\s\\d+',1,1,'e',1) as APP_NAME, CLIENT_APPLICATION_VERSION as APP_VERSION, last_seen, OS, Application as TOOL, DISTSESH.USER_NAME, LH.CLIENT_IP as IP_ADDRESS, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' ,1 )::string as INT) as ver_a, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' , 2 )::string as INT) as ver_b, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' ,3 )::string as INT) as ver_c
    from DISTSESH LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY LH
    WHERE M_LOGIN_EVENT_ID = LH.EVENT_ID
    );
    
    GRANT SELECT,INSERT,UPDATE,DELETE  ON table data.tbl_sessions  TO APPLICATION ROLE admin;
    GRANT SELECT ON table data.tbl_sessions  TO APPLICATION ROLE appuser;
    
    create or replace view data.vw_sessions as 
    select CLIENT_APP_ID, s.APP_NAME, client_id, APP_VERSION, LAST_SEEN, TOOL,
    (s.APP_NAME is null or s.ver_a is null or s.ver_b is null or s.ver_c is null or not cv.is_supported or cv."Nearing End of Support" is null or cv."Nearing End of Support" = '') as is_unknown_driver,
        iff(not is_unknown_driver,
            (s.ver_a < cv.eol_a)
            or (s.ver_a = cv.eol_a and s.ver_b < cv.eol_b)
            or (s.ver_a = cv.eol_a and s.ver_b = cv.eol_b and s.ver_c < cv.eol_c), False) as is_deprecated, 
        OS, USER_NAME, IP_ADDRESS
    from data.tbl_sessions s LEFT join data.vw_client_version cv on s.APP_NAME = cv.APP_NAME
    ;
    GRANT SELECT,INSERT,UPDATE,DELETE  ON view data.vw_sessions  TO APPLICATION ROLE admin;
    GRANT SELECT ON view data.vw_sessions  TO APPLICATION ROLE appuser;
    
    UPDATE config.cda_config set conf_value=LOCALTIMESTAMP()::STRING,last_updated_date=current_date() where conf_name='sessions_last_update';

    RETURN 'Table and View Created!';
  END;
GRANT USAGE ON PROCEDURE app.create_sessions_objects() TO APPLICATION ROLE appuser;
GRANT USAGE ON PROCEDURE app.create_sessions_objects() TO APPLICATION ROLE admin;
CREATE OR REPLACE PROCEDURE app.update_sessions_table()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS OWNER
  AS
  DECLARE
    rs resultset;
    ins int;
    upd int;
  BEGIN  
    rs := (merge into data.tbl_sessions t USING (WITH SESH as 
      (SELECT CREATED_ON, CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, PARSE_JSON(CLIENT_ENVIRONMENT):OS AS OS, PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION AS Application,USER_NAME,LOGIN_EVENT_ID
      FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS 
      WHERE 
      DATEDIFF('HOUR', (select TRY_TO_TIMESTAMP(conf_value) from dataschema.cda_config where conf_name='sessions_last_update'), CREATED_ON ) >= -2
      AND CLIENT_APPLICATION_ID NOT ILIKE '%SNOWFLAKE UI%' AND CLIENT_APPLICATION_ID NOT ILIKE '%SQLAPI%' 
      AND PARSE_JSON(CLIENT_ENVIRONMENT):APPLICATION NOT ILIKE '%Snowflake Web App%'
      ),
      DISTSESH as
      (SELECT CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, OS, Application, SESH.USER_NAME as USER_NAME, LH.CLIENT_IP as IP_ADDRESS, max(LOGIN_EVENT_ID) as M_LOGIN_EVENT_ID, max(CREATED_ON::DATE) as last_seen
      from SESH LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY LH
      WHERE SESH.LOGIN_EVENT_ID = LH.EVENT_ID
      group by CLIENT_APPLICATION_ID, CLIENT_APPLICATION_VERSION, OS, Application, SESH.USER_NAME, LH.CLIENT_IP
      )
      select CLIENT_APPLICATION_ID as CLIENT_APP_ID, REGEXP_SUBSTR(CLIENT_APPLICATION_ID, '(\\D+)\\s\\d+',1,1,'e',1) as APP_NAME, CLIENT_APPLICATION_VERSION as APP_VERSION, last_seen, OS, Application as TOOL, DISTSESH.USER_NAME, IP_ADDRESS, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' ,1 )::string as INT) as ver_a, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' , 2 )::string as INT) as ver_b, try_cast(split_part(CLIENT_APPLICATION_VERSION, '.' ,3 )::string as INT) as ver_c
      from DISTSESH) s ON t.CLIENT_APP_ID = s.CLIENT_APP_ID AND t.OS = s.OS AND t.TOOL = s.TOOL AND t.IP_ADDRESS = s.IP_ADDRESS
      WHEN MATCHED THEN UPDATE SET t.last_seen = s.last_seen
      WHEN NOT MATCHED THEN INSERT (CLIENT_APP_ID,APP_NAME,APP_VERSION,LAST_SEEN,OS,TOOL,USER_NAME,IP_ADDRESS,VER_A,VER_B,VER_C) VALUES (s.CLIENT_APP_ID,s.APP_NAME,s.APP_VERSION,s.LAST_SEEN,s.OS,s.TOOL,s.USER_NAME,s.IP_ADDRESS,s.VER_A,s.VER_B,s.VER_C))
    ;
    if (DATEDIFF('DAY', (select conf_value::DATE from config.cda_config where conf_name='sessions_last_update' limit 1), current_date()::DATE ) >= 6) then
      INSERT INTO data.tbl_deprecation_history (WEEK_OF,CLIENT_NAME,DEPRECATED_COUNT,SUPPORTED_COUNT)
          WITH groupedsessions AS
            (select app_name, is_deprecated, count(app_version) as rcount from data.vw_sessions where (not IS_UNKNOWN_DRIVER) group by app_name, is_deprecated )
          select current_date()::DATE, app_name, "TRUE" as  DEPRECATED_COUNT, "FALSE" as  SUPPORTED_COUNT from groupedsessions 
          PIVOT(sum(rcount) FOR is_deprecated IN (TRUE, FALSE));
    end if;
    
    let cur cursor for rs;
    open cur;
    fetch cur into ins, upd;
    if ((ins+upd > 1) and (DATEDIFF('DAY', (select conf_value::DATE from config.cda_config where conf_name='sessions_last_update' limit 1), current_date()::DATE ) > 6)) then 
      INSERT INTO data.tbl_deprecation_history (WEEK_OF,CLIENT_NAME,DEPRECATED_COUNT,SUPPORTED_COUNT)
            WITH groupedsessions AS
            (select app_name, is_deprecated, count(app_version) as rcount from config.vw_sessions  where (not IS_UNKNOWN_DRIVER) group by app_name, is_deprecated )
                select current_date()::DATE, app_name, ifnull("TRUE",0) as  DEPRECATED_COUNT, ifnull("FALSE",0) as  SUPPORTED_COUNT from groupedsessions  
                    PIVOT(sum(rcount) FOR is_deprecated IN (TRUE, FALSE));
    end if;
    if ((ins+upd > 1) or (DATEDIFF('DAY', (select conf_value::DATE from config.cda_config where conf_name='sessions_last_update' limit 1), current_date()::DATE ) > 6)) then 
          UPDATE config.cda_config set conf_value = current_date()::STRING,last_updated_date=current_date() where conf_name='sessions_last_update';
    end if;
    RETURN concat(ins, ' rows inserted, and ', upd, ' rows updated in sessions.');
END;
GRANT USAGE ON PROCEDURE app.update_sessions_table() TO APPLICATION ROLE appuser;
GRANT USAGE ON PROCEDURE app.update_sessions_table() TO APPLICATION ROLE admin;
CREATE OR REPLACE PROCEDURE app.create_task(WH_NAME string)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS OWNER
  AS  
  $$
    var create_task_cmd = "CREATE TASK IF NOT EXISTS config.task_update_sessions_table WAREHOUSE = ?"
      + " SCHEDULE = \'USING CRON 0 1 * * 0 UTC\'"
      + " as call app.update_sessions_table();"
    var statement2 =  "ALTER TASK config.task_update_sessions_table RESUME;";

    snowflake.execute({ sqlText: create_task_cmd, binds: [WH_NAME] });
    snowflake.execute({ sqlText: statement2 });
    return 'TASK CREATED AND RESUMED';
  $$;
GRANT USAGE ON PROCEDURE app.create_task(string) TO APPLICATION ROLE admin;
CREATE STREAMLIT if not exists app.SnowClientDriverAnanlyzer FROM '/streamlit' MAIN_FILE = '/main.py';
GRANT USAGE ON STREAMLIT app.SnowClientDriverAnanlyzer TO APPLICATION ROLE admin;
GRANT USAGE ON STREAMLIT app.SnowClientDriverAnanlyzer TO APPLICATION ROLE appuser;
CREATE OR REPLACE PROCEDURE app.setup_application(NATIVE_APP_NAME string, WH_NAME string)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS OWNER
  AS  
  $$
    var statement1 = "grant imported privileges on database snowflake to application ?;";
    snowflake.execute({ sqlText: statement1, binds: [NATIVE_APP_NAME] });

    var statement2 = "grant usage on warehouse ? to application ?;";
    snowflake.execute({ sqlText: statement2, binds: [WH_NAME, NATIVE_APP_NAME] });

    var statement3 = "grant execute task on account to application ?;";
    snowflake.execute({ sqlText: statement3, binds: [NATIVE_APP_NAME] });

    var statement4 = "CALL ?.app.CREATE_SESSIONS_OBJECTS();";
    snowflake.execute({ sqlText: statement4, binds: [NATIVE_APP_NAME] });

    var statement5 = "CALL ?.app.create_task(\'?\');";
    snowflake.execute({ sqlText: statement5, binds: [NATIVE_APP_NAME, WH_NAME] });

    return 'INITIAL SETUP COMPLETE';
  $$;
GRANT USAGE ON PROCEDURE app.setup_application(string, string) TO APPLICATION ROLE admin;
GRANT USAGE ON PROCEDURE app.setup_application(string, string) TO APPLICATION ROLE appuser;