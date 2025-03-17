create database project3;
use project3;

#Table1 users
create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_atactivated_at varchar(100),
state varchar(50)
);

SHOW Variables like "secure_file_priv";
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE Users
FIELDS Terminated By','
ENCLOSED BY '"'
LINES TERMINATED BY'\n'
IGNORE 1 ROWS;

SELECT * FROM Users;
Alter TABLE users add column temp_created_at datetime;
SET SQL_SAFE_UPDATES = 0;
UPDATE USERS SET temp_created_at = STR_TO_DATE(created_at,'%d-%m-%Y %H:%i');
SET SQL_SAFE_UPDATES = 1;
Alter TABLE users DROP column created_at;
Alter TABLE users change column temp_created_at created_at datetime;

Alter TABLE users add column temp_activated_at datetime;
UPDATE USERS SET temp_activated_at = STR_TO_DATE(activated_atactivated_at,'%d-%m-%Y %H:%i');
Alter TABLE users DROP column activated_atactivated_at;
Alter TABLE users change column temp_activated_at activated_at datetime;

#Table2 events
#user_id	occurred_at	event_type	event_name	location	device	user_type
create table events(
user_id int,
occurred_at varchar(100),
event_type Varchar(50),
event_name varchar(50),
location varchar(50),
device varchar(50),
user_type int
);

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS Terminated By','
ENCLOSED BY '"'
LINES TERMINATED BY'\n'
IGNORE 1 ROWS;
select * from events;
Alter TABLE events add column temp_occurred_at datetime;
UPDATE events SET temp_occurred_at = STR_TO_DATE(occurred_at,'%d-%m-%Y %H:%i');
Alter TABLE events DROP column occurred_at;
Alter TABLE events change column temp_occurred_at occurred_at datetime;

#Table 3 email_events
#user_id	occurred_at	action	user_type
create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(50),
user_type int
);
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE email_events
FIELDS Terminated By','
ENCLOSED BY '"'
LINES TERMINATED BY'\n'
IGNORE 1 ROWS;
select * from email_events;
Alter TABLE email_events add column temp_occurred_at datetime ;
UPDATE email_events SET temp_occurred_at = STR_TO_DATE(occurred_at,'%d-%m-%Y %H:%i');
Alter TABLE email_events DROP column occurred_at;
Alter TABLE email_events change column temp_occurred_at occurred_at datetime;

#case Study 1
#ds	job_id	actor_id	event	language	time_spent	org
create table job_data(
ds varchar(100),
job_id int,
actor_id varchar(50),
event varchar(50),
language varchar(50),
time_spent int,
org Varchar(10)
);
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv"
INTO TABLE job_data
FIELDS Terminated By','
ENCLOSED BY '"'
LINES TERMINATED BY'\n'
IGNORE 1 ROWS;
select * from job_data;
Alter TABLE job_data add column temp_ds date ;
UPDATE job_data SET temp_ds = STR_TO_DATE(ds,'%m/%d/%Y');
Alter TABLE job_data DROP column ds;
Alter TABLE job_data change column temp_ds ds date;

#case study 1 tasks
#Task1
select
    ds as date_of_review,
    count(*) as no_of_jobs_reviewed
from 
    job_data
where 
    ds between '2020/11/01' and '2020/11/30'
group by
    ds
order by
    date_of_review;
    # task2
    with daily_throughput as (
    select 
        ds as event_date,
        count(*) as total_events,
        count(*) * 1.0 / 86400.0 as daily_throughput 
    from 
        job_data
    group by 
        ds
),
rolling_average as (
    select 
        event_date,
        daily_throughput,
        avg(daily_throughput) over (
            order by event_date 
            rows between 6 preceding and current row
        ) as rolling_7_day_avg
    from 
        daily_throughput
)
select 
    event_date,
    daily_throughput,
    rolling_7_day_avg
from 
    rolling_average
order by 
    event_date;
# task3
Select 
    language,
    COUNT(*) as language_count,
    (COUNT(*) * 100.0 / (Select COUNT(*) from job_data)) as percentage
From 
    job_data
group by 
    language
order by
    percentage DESC;
#task 4
select 
    job_id, 
    actor_id, 
    event, 
    language, 
    time_spent, 
    org, 
    ds,
    count(*) as count_of_duplicates
from 
    job_data
group by 
    job_id, 
    actor_id, 
    event, 
    language, 
    time_spent, 
    org, 
    ds
having 
    count(*) > 1;


#case study 2
#Task1
SELECT 
    DATE_FORMAT(occurred_at, '%Y-%u') AS week,
    user_id,
    COUNT(*) AS events_count
FROM 
    events
GROUP BY 
    week, user_id
ORDER BY 
    week, user_id;
#Task2
SELECT 
    DATE_FORMAT(created_at, '%Y-%m') AS month,
    COUNT(*) AS new_users
FROM 
    users
GROUP BY 
    month
ORDER BY 
    month;
# Task3
WITH signup_week AS (
    SELECT 
        user_id,
        DATE_FORMAT(created_at, '%Y-%u') AS signup_week
    FROM 
        users
), weekly_activity AS (
    SELECT 
        user_id,
        DATE_FORMAT(occurred_at, '%Y-%u') AS activity_week
    FROM 
        events
)
SELECT 
    sw.signup_week,
    wa.activity_week,
    COUNT(DISTINCT wa.user_id) AS retained_users
FROM 
    signup_week sw
JOIN 
    weekly_activity wa ON sw.user_id = wa.user_id
WHERE 
    sw.signup_week <> wa.activity_week
GROUP BY 
    sw.signup_week, wa.activity_week
ORDER BY 
    sw.signup_week, wa.activity_week;
# Task4
select 
    yearweek(occurred_at) as week,
    device,
    count(distinct user_id) as active_users_per_device
from 
    events
group by 
    week, device
order by 
    week desc, active_users_per_device desc;

#Task5
SELECT 
    DATE_FORMAT(occurred_at, '%Y-%u') AS week,
    action,
    COUNT(*) AS action_count
FROM 
    email_events
GROUP BY 
    week, action
ORDER BY 
    week, action;



select * FROM events;
