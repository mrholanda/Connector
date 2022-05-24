# This is called from wfmmotivateconnector.py to create temp tables at start up of the Service.

DROP_CREATE_TMP_TABLES="""

 SET ROLE TO noble_restricted;
 DROP TABLE IF EXISTS tmp_adh_emp_basedata_sched;
 CREATE TABLE tmp_adh_emp_basedata_sched (
   employee_sk text,
   emp_start text,
   emp_stop text,
   sched_start text,
   sched_stop text,
   sched_state_sk text);

 DROP TABLE IF EXISTS tmp_adh_emp_basedata_actual;
 CREATE TABLE tmp_adh_emp_basedata_actual (
   employee_sk text,
   emp_start text,
   emp_stop text,
   actual_start text,
   actual_stop text,
   actual_state_sk text);

 DROP TABLE IF EXISTS tmp_adh_emp_basedata_adherence;
 CREATE TABLE tmp_adh_emp_basedata_adherence (
   Employee_sk text,
   emp_start text,
   emp_stop text,
   iswithinscheduled text,
   adh_start text,
   adh_stop text,
   scheduled_sk text,
   actual_sk text);

 DROP TABLE IF EXISTS tmp_adh_emp_basedata_ststat;
 CREATE TABLE tmp_adh_emp_basedata_ststat (
   employee_sk text,
   emp_start text,
   emp_stop text,
   state_sk text,
   scheduled text,
   actual text,
   inadherence text,
   outunscheduled text,
   outscheduled text,
   totalinOut text,
   percent text);
   CREATE INDEX idxxtmp_adh_emp_bta_sta6 ON tmp_adh_emp_basedata_ststat(employee_sk);

 DROP TABLE IF EXISTS tmp_suppergrp;
 CREATE TABLE tmp_suppergrp (
    emp_supergroup_sk text,
    code text,
    description text,
    updatedby_sk text,
    snowflakeid text,
    updatedon timestamp without time zone,
    employee_group_sk text
 );


 DROP TABLE IF EXISTS tmp_employeelookup;
 CREATE TABLE tmp_employeelookup (
    employee_sk text,
    id text,
    name text,
    lastname text,
    firstname text,
    shortname text,
    sortname text,
    updatedby_sk text,
    updatedby_snowflakeid text,
    updatedon text,
    site_id text,
    timezone_sk text,
    snowflake text,
    user_sk text);
 CREATE INDEX idxxtmp_emp_lookup ON tmp_employeelookup(employee_sk);

 DROP TABLE IF EXISTS tmp_schedulesegmentsummary;
 CREATE TABLE tmp_schedulesegmentsummary (
   nominaldate text,
   employee_sk text,
   earlieststart text,
   lateststop text,
   duration text,
   generalsegmentcode text,
   worksegmentcode_sk text);
 CREATE INDEX idxxtmp_schedsegsu3 ON tmp_schedulesegmentsummary(employee_sk);
 CREATE INDEX idxxtmp_schedseg012 ON tmp_schedulesegmentsummary(earlieststart);
 CREATE INDEX idxxtmp_schedseg035 ON tmp_schedulesegmentsummary(latestStop);

 DROP TABLE IF EXISTS tmp_scheduleperspectivelookup;
 CREATE TABLE tmp_scheduleperspectivelookup (
   perspective_sk text,
   code text,
   description text,
   updatedBy_sk text,
   snowflakeid text,
   updatedon text,
   state_sk text,
   xdefault text,
   code2 text,
   description2 text);

 DROP TABLE IF EXISTS tmp_scheduletimeperiodres;
 CREATE TABLE tmp_scheduletimeperiodres (
   employee_sk text,
   perspective_sk text,
   start text,
   stop text,
   state_sk text,
   nominaldate text);

 DROP TABLE IF EXISTS tmp_today_roster;
 CREATE TABLE tmp_today_roster (
   employee_sk text,
   start text,
   stop text,
   snowflake text,
   nominaldate text,
   earlieststart text,
   lateststop text);
 CREATE INDEX idxtmp_today_roster923 ON tmp_today_roster(employee_sk);

 DROP TABLE IF EXISTS tmp_today_roster_shift;
 CREATE TABLE tmp_today_roster_shift (
   employee_sk text,
   nominaldate text,
   earlieststart text,
   lateststop text);
 CREATE INDEX idxtmp_today_rer123 ON tmp_today_roster_shift(employee_sk, nominaldate);

 DROP TABLE IF EXISTS tmp_employeeinfo;
 CREATE TABLE tmp_employeeinfo (
   employee_sk text,
   id text,
   lastname text,
   firstname text,
   effhiredate text,
   termdate text,
   active text,
   shortname text,
   sortname text,
   timezone_sk text,
   seniority text,
   emailadr text,
   imadr text,
   updatedby_sk text,
   updatedby_snowflakeid text,
   updatedon text,
   memo text);

 DROP TABLE IF EXISTS tmp_adminuserlookup;
 CREATE TABLE tmp_adminuserlookup (
   user_sk text,
   snowflakeid text,
   active text,
   username text,
   altusername text,
   systemadministrator text,
   filterprofile_sk text,
   securityprofile_sk text,
   seatfilterprofile text,
   employee_sk text,
   firstname text,
   lastname text,
   firstdayofweek text,
   timezone_sk text,
   fullname text,
   secondaryloginid text);
   CREATE INDEX idxxtmp_adminookup ON tmp_adminuserlookup(employee_sk);

 DROP TABLE IF EXISTS tmp_adh_emp_roster_sched;
 CREATE TABLE tmp_adh_emp_roster_sched (
   employee_sk text,
   emp_start text,
   emp_stop text,
   sched_start text,
   sched_stop text,
   sched_state_sk text);
   CREATE INDEX idxxtmpadhemprsche4 ON tmp_adh_emp_roster_sched(employee_sk);
   CREATE INDEX idxxtmpadhemprsc005 ON tmp_adh_emp_roster_sched(employee_sk, sched_state_sk);

 DROP TABLE IF EXISTS tmp_adh_emp_roster_actual;
 CREATE TABLE tmp_adh_emp_roster_actual (
   employee_sk text,
   emp_start text,
   emp_stop text,
   actual_start text,
   actual_stop text,
   actual_state_sk text);
   CREATE INDEX idxxtmpadhempractual01 ON tmp_adh_emp_roster_actual(employee_sk);
   CREATE INDEX idxxtmpadhempractu0002 ON tmp_adh_emp_roster_actual(employee_sk, actual_state_sk);

 DROP TABLE IF EXISTS tmp_adh_emp_roster_adherence;
 CREATE TABLE tmp_adh_emp_roster_adherence (
   Employee_sk text,
   emp_start text,
   emp_stop text,
   iswithinscheduled text,
   adh_start text,
   adh_stop text,
   scheduled_sk text,
   actual_sk text);

 DROP TABLE IF EXISTS tmp_adh_emp_roster_ststat;
 CREATE TABLE tmp_adh_emp_roster_ststat (
   employee_sk text,
   emp_start text,
   emp_stop text,
   state_sk text,
   scheduled text,
   actual text,
   inadherence text,
   outunscheduled text,
   outscheduled text,
   totalinOut text,
   percent text);

"""

def create_tmp_tables(db, log):
    FUNC="Function create_tmp_tables: "
    log.info("Entering {} ".format(FUNC,))
    try:
       cur = db.cursor()
       cur.execute(DROP_CREATE_TMP_TABLES)
       log.info("Exiting {} ".format(FUNC,))
    except psycopg2.Error as e:
       log.error("{} Drop/Create error of tmp tables:  '{}' ".format(FUNC,e))
