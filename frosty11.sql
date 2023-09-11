-- Set the datab
use database frosty_friday;

use schema challenges;

create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 1 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

-- Create the stage that points at the data.
create or replace stage week_11_frosty_stage
    url = 's3://frostyfridaychallenges/challenge_11/'
    file_format = csv;

-- Create the table as a CTAS statement.
create or replace table frosty_friday.challenges.week11 as
select m.$1 as milking_datetime,
        m.$2 as cow_number,
        m.$3 as fat_percentage,
        m.$4 as farm_code,
        m.$5 as centrifuge_start_time,
        m.$6 as centrifuge_end_time,
        m.$7 as centrifuge_kwph,
        m.$8 as centrifuge_electricity_used,
        m.$9 as centrifuge_processing_time,
        m.$10 as task_used
from @week_11_frosty_stage (file_format => 'csv', pattern => '.*milk_data.*[.]csv') m;



-- TASK 1: Remove all the centrifuge dates and centrifuge kwph and replace them with NULLs WHERE fat = 3. 
-- Add note to task_used.
create or replace task whole_milk_updates
    schedule = '1400 minutes'
as
    UPDATE week11 SET 
        centrifuge_start_time = null, 
        centrifuge_end_time = null, 
        centrifuge_kwph = null, 
        task_used = CONCAT(SYSTEM$CURRENT_USER_TASK_NAME(),' at ', CURRENT_TIMESTAMP) 
    WHERE fat_percentage = 3;

--SELECT centrifuge_start_time,centrifuge_end_time,DATEDIFF(minute, CAST(centrifuge_start_time AS datetime), CAST(centrifuge_end_time AS datetime)) AS diferencia
--FROM week11 WHERE fat_percentage != 3;


-- TASK 2: Calculate centrifuge processing time (difference between start and end time) WHERE fat != 3. 
-- Add note to task_used.
create or replace task skim_milk_updates
    after frosty_friday.challenges.whole_milk_updates
as
    UPDATE week11 SET 
        centrifuge_processing_time = DATEDIFF(minute, CAST(centrifuge_start_time AS datetime), CAST(centrifuge_end_time AS datetime)),
        task_used = CONCAT(SYSTEM$CURRENT_USER_TASK_NAME(),' at ', CURRENT_TIMESTAMP)
    WHERE fat_percentage != 3;


-- Manually execute the task.
execute task whole_milk_updates;

-- Check that the data looks as it should.
select * from week11;

-- Check that the numbers are correct.
select task_used, count(*) as row_count from week11 group by task_used;

-- Manually execute the task.
SELECT SYSTEM$TASK_DEPENDENTS_DISABLE( 'frosty_friday.challenges.whole_milk_updates' );

execute task whole_milk_updates;

-- Check that the data looks as it should.
select * from week11;

-- Check that the numbers are correct.
select task_used, count(*) as row_count from week11 group by task_used;