#!/usr/bin/python3

import sys
import os
import time
import requests
import xml.etree.ElementTree as ET
import psycopg2
import logging
from logging.handlers import RotatingFileHandler
from datetime import date, datetime, timedelta
from wfmmotivateconnector_tabs import create_tmp_tables

APP = os.path.splitext(os.path.basename(__file__))[0]
log = logging.getLogger(APP)

DD_INSTALLED = True
DD_LOGGED = False
try:
    from datadog import initialize, statsd
    datadogoptions = {
        'statsd_host':'127.0.0.1',
        'statsd_port':8125,
        'statsd_namespace': 'WFMMotivateConnector'
    }
    initialize(**datadogoptions)
except ModuleNotFoundError:
    DD_INSTALLED = False
    DD_LOGGED = False

#------------------------------------------------------------------------------------------------
SELECT_GAME_SETTING="""
SET ROLE TO noble_restricted;
SELECT TRIM(value) FROM game_setting WHERE name = 'via_deployment' limit 1;
"""
#------------------------------------------------------------------------------------------------
SELECT_GAME_SETTING_DATADOG="""
SET ROLE TO noble_restricted;
SELECT TRIM(value) FROM game_setting WHERE name = 'use_datadog' limit 1;
"""
#------------------------------------------------------------------------------------------------
SELECT_LAST_RUN_TIME="""
SET ROLE TO noble_restricted;
SELECT to_char(lasttime, 'YYYY-MM-DD'), rerun_from, rerun_to FROM wfmc_repull limit 1;
"""
#------------------------------------------------------------------------------------------------
SELECT_NOW="""
SET ROLE TO noble_restricted;
SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'),
       replace(to_char(now(), 'YYYY-MM-DD T HH24:MI:SS'), ' ', ''),
       to_char(now()::date - interval '2 days', 'YYYY-MM-DD'),
       to_char(now()::date + interval '2 days', 'YYYY-MM-DD'),
       to_char(now()::date, 'YYYY-MM-DD'),
       to_char(now()::date, 'YYYY-MM-DDT00:00:00'),
       to_char(now()::date, 'MMDDYY'),
       to_char(now(), 'HH24')
"""
#------------------------------------------------------------------------------------------------
INSERT_NSC_BASEDATA="""
 SET ROLE TO noble_restricted;
 INSERT INTO nsc_basedata (session_id, date_orig, date_proc, agent, campaign, agt_in_adherence, agt_out_adherence)
 VALUES (%s,%s,%s,%s,%s,%s,%s)
 ON CONFLICT (session_id) DO UPDATE SET (date_orig, date_proc, agent, campaign, agt_in_adherence, agt_out_adherence) =
 (EXCLUDED.date_orig, EXCLUDED.date_proc, EXCLUDED.agent, EXCLUDED.campaign, EXCLUDED.agt_in_adherence, EXCLUDED.agt_out_adherence)
"""
#------------------------------------------------------------------------------------------------

INITIAL_INSERT_WFM_DAILY_ROSTER="""
 SET ROLE TO noble_restricted;
 INSERT INTO wfm_daily_roster (session_id, date_orig, date_proc, agent, campaign, wfm_rostered, wfm_absent, wfm_tardy)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""
#------------------------------------------------------------------------------------------------
INSERT_WFM_DAILY_ROSTER="""
 SET ROLE TO noble_restricted;
 INSERT INTO wfm_daily_roster (session_id, date_orig, date_proc, agent, campaign, wfm_rostered, wfm_absent, wfm_tardy)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
 ON CONFLICT (session_id) DO UPDATE SET (date_orig, date_proc, agent, campaign, wfm_rostered, wfm_absent, wfm_tardy) =
 (EXCLUDED.date_orig, EXCLUDED.date_proc, EXCLUDED.agent, EXCLUDED.campaign, EXCLUDED.wfm_rostered, EXCLUDED.wfm_absent, EXCLUDED.wfm_tardy)
"""
#------------------------------------------------------------------------------------------------
INSERT_SUPPERGRP="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_suppergrp (emp_supergroup_sk, code, description, updatedby_sk, snowflakeid, updatedon, employee_group_sk)
 VALUES (%s,%s,%s,%s,%s,%s,%s)
"""
#------------------------------------------------------------------------------------------------
INSERT_EMPLOYEELOOKUP="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_employeelookup (employee_sk, id, name, lastname, firstname, shortname, sortname, updatedby_sk, updatedby_snowflakeid, updatedon, site_id)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
#-------------------------------------------------------------------------
INSERT_EMPLOYEEINFO="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_employeeinfo_tz (employee_sk, id, lastname, firstname, effhiredate, termdate, active, shortname, sortname, timezone_sk, seniority, emailadr, imAdr, updatedby_sk, updatedby_snowflakeid, updatedon, memo)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
#-------------------------------------------------------------------------
INSERT_ADMINUSERlOOKUP="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adminuserlookup (
   user_sk, snowflakeid, active, username, altusername, systemadministrator, filterprofile_sk, securityprofile_sk, seatfilterprofile, employee_sk, firstname, lastname, firstdayofweek, timezone_sk, fullname, secondaryloginid)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
#-------------------------------------------------------------------------
INSERT_SCHEDULEPERSPECTIVELOOKUP="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_scheduleperspectivelookup (perspective_sk, code, description, updatedBy_sk, snowflakeid, updatedon, state_sk, xdefault, code2, description2)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_SCHEDULETIMEPERIODRES="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_scheduletimeperiodres (employee_sk, perspective_sk, start, stop, state_sk, nominaldate)
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_SCHEDULESEGMENTSUMMARY="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_schedulesegmentsummary (
   nominaldate, employee_sk, earlieststart, latestStop, duration, generalsegmentcode, worksegmentcode_sk)
 VALUES (%s,%s,%s,%s,%s,%s,%s)
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_SCHED="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_sched (employee_sk, emp_start, emp_stop, sched_start, sched_stop, sched_state_sk) 
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_ACTUAL="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_actual (employee_sk, emp_start, emp_stop, actual_start, actual_stop, actual_state_sk)
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_ADHERENCE="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_adherence (Employee_sk, emp_start, emp_stop, iswithinscheduled, adh_start, adh_stop, scheduled_sk, actual_sk)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_STSTAT="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_ststat (employee_sk, emp_start, emp_stop, state_sk, scheduled, actual, inadherence, outunscheduled, outscheduled, totalinOut, percent)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_BASEDATA_SCHED="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_basedata_sched (employee_sk, emp_start, emp_stop, sched_start, sched_stop, sched_state_sk) 
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_BASEDATA_ACTUAL="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_basedata_actual (employee_sk, emp_start, emp_stop, actual_start, actual_stop, actual_state_sk)
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_BASEDATA_ADHERENCE="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_basedata_adherence (Employee_sk, emp_start, emp_stop, iswithinscheduled, adh_start, adh_stop, scheduled_sk, actual_sk)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_BASEDATA_STSTAT="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_basedata_ststat (employee_sk, emp_start, emp_stop, state_sk, scheduled, actual, inadherence, outunscheduled, outscheduled, totalinOut, percent)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_SCHED="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_sched (employee_sk, emp_start, emp_stop, sched_start, sched_stop, sched_state_sk)
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_ACTUAL="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_actual (employee_sk, emp_start, emp_stop, actual_start, actual_stop, actual_state_sk)
 VALUES (%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_ADHERENCE="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_adherence (Employee_sk, emp_start, emp_stop, iswithinscheduled, adh_start, adh_stop, scheduled_sk, actual_sk)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------
INSERT_ADH_EMP_ROSTER_STSTAT="""
 SET ROLE TO noble_restricted;
 INSERT INTO tmp_adh_emp_roster_ststat (employee_sk, emp_start, emp_stop, state_sk, scheduled, actual, inadherence, outunscheduled, outscheduled, totalinOut, percent)
 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""
#-------------------------------------------------------------------------

def get_employeeadherence_basedata(db, l_list_of_employees, l_perspective_sk, l_fromdate, l_todate):
    """ Get the adherence information """

    FUNC="Function get_employeeadherence_basedata: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_adh_emp_basedata_sched")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_basedata_actual")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_basedata_adherence")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_basedata_ststat")

    base_url  = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/AdherenceEmployeeAdherence.ewfm'
    base_data = 'data_in=<Adherence Source="RTA"><EmployeeSelector>' +l_list_of_employees+ '</EmployeeSelector><DateTimeRange><Start>' +l_fromdate+ '</Start><Stop>' +l_todate+ '</Stop></DateTimeRange><Interval>1440</Interval><Perspective SK="' +l_perspective_sk+ '"/></Adherence>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        CSpans = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if CSpans.tag != 'ContainerSpans':
        log.error("{} Did not find required parent 'ContainerSpans' tag.  Please check the Web Server: '{}' and that Method: AdherenceEmployeeAdherence.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent ContainerSpans tag from Method: AdherenceEmployeeAdherence.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return
    else:
        cur = db.cursor()
        for CSpan in CSpans:
            ContainerSpan = {'Employee_SK':'',
                             'Start':'',
                             'Stop':''}

            if CSpan.tag == 'ContainerSpan':
               for ftag in CSpan:
                   if ftag.tag == 'Employee':
                      ContainerSpan['Employee_SK'] = ftag.attrib['SK'].strip()
                   elif ftag.tag in ('Start','Stop'):
                      try:
                          ContainerSpan[ftag.tag] = ftag.text.strip()
                      except (KeyError, TypeError, ValueError, AttributeError):
                          log.info("{} XML tag '{}' expected but not found".format(FUNC,ftag.tag))

            for item in CSpan:
               if item.tag == 'ScheduledSpans':
                  for ScheduledSpans in item:
                     ScheduledSp = {'Start':'',
                                    'Stop':'',
                                    'State_SK':''}
                     for ScheduledSpan in ScheduledSpans:
                         if ScheduledSpan.tag == 'State':
                            ScheduledSp['State_SK'] = ScheduledSpan.attrib['SK'].strip()
                         elif ScheduledSpan.tag in ('Start','Stop'):
                            try:
                                ScheduledSp[ScheduledSpan.tag] = ScheduledSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,ScheduledSpan.tag))

                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             ScheduledSp['Start'], ScheduledSp['Stop'], ScheduledSp['State_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_BASEDATA_SCHED, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, ScheduledSp, e))

               if item.tag == 'ActualSpans':
                  for ActualSpans in item:
                     ActualSp = {'Start':'',
                                 'Stop':'',
                                 'State_SK':''}
                     for ActualSpan in ActualSpans:
                         if ActualSpan.tag == 'State':
                            ActualSp['State_SK'] = ActualSpan.attrib['SK'].strip()
                         elif ActualSpan.tag in ('Start','Stop'):
                            try:
                                ActualSp[ActualSpan.tag] = ActualSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,ActualSpan.tag))

                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             ActualSp['Start'], ActualSp['Stop'], ActualSp['State_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_BASEDATA_ACTUAL, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, ActualSp, e))

               if item.tag == 'AdherenceSpans':
                  for AdherenceSpans in item:
                     AdherenceSp = {'IsWithinScheduled':'',
                                    'Start':'',
                                    'Stop':'',
                                    'Scheduled_SK':'',
                                    'Actual_SK':''}

                     if AdherenceSpans.tag == 'AdherenceSpan':
                        try:
                            AdherenceSp['IsWithinScheduled'] = AdherenceSpans.attrib['IsWithinScheduled'].strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            log.error("{} IsWithinScheduled attribute not found".format(FUNC,))

                     for AdherenceSpan in AdherenceSpans:
                         if AdherenceSpan.tag == 'Scheduled':
                            AdherenceSp['Scheduled_SK'] = AdherenceSpan.attrib['SK'].strip()
                         elif AdherenceSpan.tag == 'Actual':
                            AdherenceSp['Actual_SK'] = AdherenceSpan.attrib['SK'].strip()
                         elif AdherenceSpan.tag in ('Start','Stop'):
                            try:
                                AdherenceSp[AdherenceSpan.tag] = AdherenceSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,AdherenceSpan.tag))

                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             AdherenceSp['IsWithinScheduled'], AdherenceSp['Start'], AdherenceSp['Stop'],
                             AdherenceSp['Scheduled_SK'], AdherenceSp['Actual_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_BASEDATA_ADHERENCE, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, AdherenceSp, e))

               if item.tag == 'Statistics':
                  for Statistics in item:
                     StateStatistics = {'State_SK':'',
                                        'Scheduled':'',
                                        'Actual':'',
                                        'InAdherence':'',
                                        'OutUnscheduled':'',
                                        'OutScheduled':'',
                                        'TotalInOut':'',
                                        'Percent':''}
                     for StateS in Statistics:
                         if StateS.tag == 'State':
                            StateStatistics['State_SK'] = StateS.attrib['SK'].strip()
                         elif StateS.tag in ('Scheduled','Actual','InAdherence','OutUnscheduled','OutScheduled','TotalInOut','Percent'):
                            try:
                                StateStatistics[StateS.tag] = StateS.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                if StateS.tag not in ('Percent'):
                                   log.info("{} XML tag '{}' expected but not found".format(FUNC,StateS.tag))

                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             StateStatistics['State_SK'], StateStatistics['Scheduled'], StateStatistics['Actual'], StateStatistics['InAdherence'],
                             StateStatistics['OutUnscheduled'], StateStatistics['OutScheduled'], StateStatistics['TotalInOut'], StateStatistics['Percent'])
                     try:
                        cur.execute(INSERT_ADH_EMP_BASEDATA_STSTAT, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, StateStatistics, e))

    log.info("Exiting {} ".format(FUNC,))

#-X-----------------------------------------------------------------------

def get_scheduletimeperiodres(db, l_perspective_sk, l_today):
    """ Resolve employee schedule information for specified perspectives in 24 hour period """

    FUNC="Function get_scheduletimeperiodres: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_scheduletimeperiodres")

    cur.execute("SELECT MIN(earlieststart), MAX(lateststop) \
                   FROM tmp_schedulesegmentsummary B \
                  WHERE earlieststart::date = %s OR lateststop::date = %s", (l_today, l_today))
    for l_fromdate, l_todate in cur:
        l_fromdate = "%s" % (l_fromdate)
        l_todate = "%s" % (l_todate)

    ret_employee_sk = ''
    ret_employee_sk2 = ''
    cur.execute("SELECT distinct employee_sk FROM tmp_schedulesegmentsummary \
                  WHERE earlieststart::date = %s OR lateststop::date = %s", (l_today, l_today))

    for ret_employee_sk2 in cur:
        ret_employee_sk2 = "%s" % (ret_employee_sk2)
        ret_employee_sk += "<Employee SK=\""
        ret_employee_sk += ret_employee_sk2
        ret_employee_sk += "\"/>"

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/ScheduleTimePeriodResolution.ewfm'
    base_data = 'data_in=<TPR><EmployeeSelector>' +ret_employee_sk+ '</EmployeeSelector><Perspective SK="' +l_perspective_sk+ '"/><DateTimeRange><Start>' +l_fromdate+ '</Start><Stop>' +l_todate+ '</Stop></DateTimeRange></TPR>&NoStyle=&NoErrorStyle=&Timeout=5'

    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        TimePeriodResolution = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if TimePeriodResolution.tag != 'TimePeriodResolution':
        log.error("{} Did not find required parent 'TimePeriodResolution' tag.  Please check the Web Server: '{}' and that Method: ScheduleTimePeriodResolution.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent TimePeriodResolution tag from Method: ScheduleTimePeriodResolution.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return ''
    else:
        for EmployeeResolution in TimePeriodResolution:
            AllStateSpan = {'Employee_SK':'',
                            'Perspective_SK':'',
                            'Start':'',
                            'Stop':'',
                            'State_SK':'',
                            'NominalDate':''}

            for PerspectiveResolution in EmployeeResolution:
               if PerspectiveResolution.tag == 'Employee':
                  AllStateSpan['Employee_SK'] = PerspectiveResolution.attrib['SK'].strip()

               for StateSpan in PerspectiveResolution:
                   if StateSpan.tag == 'Perspective':
                      AllStateSpan['Perspective_SK'] = StateSpan.attrib['SK'].strip()
                   if StateSpan.tag == 'StateSpan':
                      for item in StateSpan:
                         if item.tag in ('Start', 'Stop', 'NominalDate'):
                             try:
                                 AllStateSpan[item.tag] = item.text.strip()
                             except (KeyError, TypeError, ValueError, AttributeError):
                                 log.error("{} XML tag '{}' expected but not found".format(FUNC,item.tag))
                         elif item.tag == 'State':
                              AllStateSpan['State_SK'] = item.attrib['SK'].strip()

                      vars = (AllStateSpan['Employee_SK'], AllStateSpan['Perspective_SK'], AllStateSpan['Start'], AllStateSpan['Stop'], AllStateSpan['State_SK'], AllStateSpan['NominalDate'])

                      try:
                          cur = db.cursor()
                          cur.execute(INSERT_SCHEDULETIMEPERIODRES, vars)
                      except psycopg2.Error as e:
                          log.error("{} Insert error for values {}:{}".format(FUNC, empdict, e))

    log.info("Exiting {} ".format(FUNC,))
    return ret_employee_sk

#-X-----------------------------------------------------------------------

def get_adherenceemployeeadherence_roster(db, l_list_of_employees, l_perspective_sk, l_fromdate, l_todate):
    """ Test whether an employee is tardy or absent for the day """

    FUNC="Function get_adherenceemployeeadherence_roster: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_adh_emp_roster_sched")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_roster_actual")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_roster_adherence")
    cur.execute("TRUNCATE TABLE tmp_adh_emp_roster_ststat")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/AdherenceEmployeeAdherence.ewfm'
    base_data = 'data_in=<Adherence Source="RTA"><EmployeeSelector>' +l_list_of_employees+ '</EmployeeSelector><DateTimeRange><Start>' +l_fromdate+ '</Start><Stop>' +l_todate+ '</Stop></DateTimeRange><Interval>1440</Interval><Perspective SK="' +l_perspective_sk+ '"/></Adherence>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        CSpans = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if CSpans.tag != 'ContainerSpans':
        log.error("{} Did not find required parent 'ContainerSpans' tag.  Please check the Web Server: '{}' and that Method: AdherenceEmployeeAdherence.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent ContainerSpans tag from Method: AdherenceEmployeeAdherence.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return ''
    else:
        cur = db.cursor()
        for CSpan in CSpans:
            ContainerSpan = {'Employee_SK':'',
                             'Start':'',
                             'Stop':''}

            if CSpan.tag == 'ContainerSpan':
               for ftag in CSpan:
                   if ftag.tag == 'Employee':
                      ContainerSpan['Employee_SK'] = ftag.attrib['SK'].strip()
                   elif ftag.tag in ('Start','Stop'):
                      try:
                          ContainerSpan[ftag.tag] = ftag.text.strip()
                      except (KeyError, TypeError, ValueError, AttributeError):
                          log.info("{} XML tag '{}' expected but not found".format(FUNC,ftag.tag))

            for item in CSpan:
               if item.tag == 'ScheduledSpans':
                  for ScheduledSpans in item:
                     ScheduledSp = {'Start':'',
                                    'Stop':'',
                                    'State_SK':''}
                     for ScheduledSpan in ScheduledSpans:
                         if ScheduledSpan.tag == 'State':
                            ScheduledSp['State_SK'] = ScheduledSpan.attrib['SK'].strip()
                         elif ScheduledSpan.tag in ('Start','Stop'):
                            try:
                                ScheduledSp[ScheduledSpan.tag] = ScheduledSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,ScheduledSpan.tag))
                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             ScheduledSp['Start'], ScheduledSp['Stop'], ScheduledSp['State_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_ROSTER_SCHED, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, ScheduledSp, e))

               if item.tag == 'ActualSpans':
                  for ActualSpans in item:
                     ActualSp = {'Start':'',
                                 'Stop':'',
                                 'State_SK':''}
                     for ActualSpan in ActualSpans:
                         if ActualSpan.tag == 'State':
                            ActualSp['State_SK'] = ActualSpan.attrib['SK'].strip()
                         elif ActualSpan.tag in ('Start','Stop'):
                            try:
                                ActualSp[ActualSpan.tag] = ActualSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,ActualSpan.tag))
                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             ActualSp['Start'], ActualSp['Stop'], ActualSp['State_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_ROSTER_ACTUAL, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, ActualSp, e))

               if item.tag == 'AdherenceSpans':
                  for AdherenceSpans in item:
                     AdherenceSp = {'IsWithinScheduled':'',
                                    'Start':'',
                                    'Stop':'',
                                    'Scheduled_SK':'',
                                    'Actual_SK':''}

                     if AdherenceSpans.tag == 'AdherenceSpan':
                        try:
                            AdherenceSp['IsWithinScheduled'] = AdherenceSpans.attrib['IsWithinScheduled'].strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            log.error("{} IsWithinScheduled attribute not found".format(FUNC,))

                     for AdherenceSpan in AdherenceSpans:
                         if AdherenceSpan.tag == 'Scheduled':
                            AdherenceSp['Scheduled_SK'] = AdherenceSpan.attrib['SK'].strip()
                         elif AdherenceSpan.tag == 'Actual':
                            AdherenceSp['Actual_SK'] = AdherenceSpan.attrib['SK'].strip()
                         elif AdherenceSpan.tag in ('Start','Stop'):
                            try:
                                AdherenceSp[AdherenceSpan.tag] = AdherenceSpan.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,AdherenceSpan.tag))
                  
                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             AdherenceSp['IsWithinScheduled'], AdherenceSp['Start'], AdherenceSp['Stop'],
                             AdherenceSp['Scheduled_SK'], AdherenceSp['Actual_SK'])
                     try:
                        cur.execute(INSERT_ADH_EMP_ROSTER_ADHERENCE, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, AdherenceSp, e))

               if item.tag == 'Statistics':
                  for Statistics in item:
                     StateStatistics = {'State_SK':'',
                                        'Scheduled':'',
                                        'Actual':'',
                                        'InAdherence':'',
                                        'OutUnscheduled':'',
                                        'OutScheduled':'',
                                        'TotalInOut':'',
                                        'Percent':''}
                     for StateS in Statistics:
                         if StateS.tag == 'State':
                            StateStatistics['State_SK'] = StateS.attrib['SK'].strip()
                         elif StateS.tag in ('Scheduled','Actual','InAdherence','OutUnscheduled','OutScheduled','TotalInOut','Percent'):
                            try:
                                StateStatistics[StateS.tag] = StateS.text.strip()
                            except (KeyError, TypeError, ValueError, AttributeError):
                                if StateS.tag not in ('Percent'):
                                   log.info("{} XML tag '{}' expected but not found".format(FUNC,StateS.tag))

                     vars = (ContainerSpan['Employee_SK'], ContainerSpan['Start'], ContainerSpan['Stop'],
                             StateStatistics['State_SK'], StateStatistics['Scheduled'], StateStatistics['Actual'],
                             StateStatistics['InAdherence'], StateStatistics['OutUnscheduled'], StateStatistics['OutScheduled'],
                             StateStatistics['TotalInOut'], StateStatistics['Percent'])
                     try:
                        cur.execute(INSERT_ADH_EMP_ROSTER_STSTAT, vars)
                     except psycopg2.Error as e:
                        log.error("{} Insert error for values {}:{}".format(FUNC, StateStatistics, e))

    log.info("Exiting {} ".format(FUNC,))
    return 'ok'

#-------------------------------------------------------------------------

def get_schedulesegmentsummary(db, l_list_of_employees, l_fromdate, l_todate, l_today):
    """ Load daily summary information for employee schedules """

    FUNC="Function get_schedulesegmentsummary: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_schedulesegmentsummary")
    cur.execute("TRUNCATE TABLE tmp_today_roster_shift")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/ScheduleSegmentSummary.ewfm'
    base_data = 'data_in=<SegmentSummary><EmployeeSelector HideInactiveEmployees="true">' +l_list_of_employees+ '</EmployeeSelector><DateRange><Start>' +l_fromdate+ '</Start><Stop>' +l_todate+ '</Stop></DateRange></SegmentSummary>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to: '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        Summaries = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if Summaries.tag != 'Summaries':
        log.error("{} Did not find required parent 'Summaries' tag.  Please check the Web Server: '{}' and that Method: ScheduleSegmentSummary.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent Summaries tag from Method: ScheduleSegmentSummary.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return
    else:

        for summary in Summaries:
            AllSummaries = {'NominalDate':'',
                        'Employee_SK':'',
                        'EarliestStart':'',
                        'LatestStop':'',
                        'Duration':'',
                        'GeneralSegmentCode':'',
                        'WorkSegmentCode_SK':''}

            if summary.tag == 'Summary':
                for item in summary:
                    if item.tag in ('NominalDate', 'EarliestStart', 'LatestStop','Duration','GeneralSegmentCode'):
                        try:
                            AllSummaries[item.tag] = item.text.strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            if item.tag not in ('GeneralSegmentCode'):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,item.tag))
                    elif item.tag in ('Employee','WorkSegmentCode'):
                        try:
                            if item.tag == 'Employee':
                                AllSummaries['Employee_SK'] = item.attrib['SK'].strip()

                            #elif item.tag == 'WorkSegmentCode':
                            #    AllSummaries['WorkSegmentCode_SK'] = item.attrib['SK'].strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            log.info("{} Item Employee SK attribute not found".format(FUNC,))
                    else:
                        log.info("{} Found unused tag '{}', skipping".format(FUNC, item.tag))
            else:
                log.info("{} unknown unused tag '{}', skipping".format(FUNC, summary.tag))

            vars = (AllSummaries['NominalDate'], AllSummaries['Employee_SK'], AllSummaries['EarliestStart'], AllSummaries['LatestStop'], AllSummaries['Duration'], AllSummaries['GeneralSegmentCode'], AllSummaries['WorkSegmentCode_SK'])

            try:
                cur.execute(INSERT_SCHEDULESEGMENTSUMMARY, vars)
            except psycopg2.Error as e:
                log.error("{} Insert error for values {}:{}".format(FUNC, AllSummaries, e))
    try:
        cur.execute("INSERT INTO tmp_today_roster_shift (employee_sk, nominaldate, earlieststart, lateststop) \
                     SELECT employee_sk, nominaldate, earlieststart, lateststop \
                       FROM tmp_schedulesegmentsummary \
                      WHERE (earlieststart::date = %s OR lateststop::date = %s) GROUP BY 1,2,3,4", (l_today, l_today))
    except (psycopg2.Error, OSError, TypeError, IndexError, ValueError) as e:
            log.error("{} INSERT tmp_today_roster_shift error from tmp_schedulesegmentsummary: '{}'  ".format(FUNC, e))
            log_msg = FUNC + ' INSERT tmp_today_roster_shift error from tmp_schedulesegmentsummary:: ' + str(e)
            datadog_log('error', log_msg, db)

    log.info("Exiting {} ".format(FUNC,))

#-------------------------------------------------------------------------

def get_scheduleperspectivelookup(db):
    """ Employee schedule infomation fpr the perspective  """

    FUNC="Function get_scheduleperspectivelookup: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_scheduleperspectivelookup")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/SchedulePerspectiveLookup.ewfm'
    base_data = 'data_in=<PerspectiveLookup><Code>' +wfm_supergroup+ '</Code></PerspectiveLookup>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    l_perspective_sk = ""
    l_state_sk = ""
    try:
        Perspectives = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if Perspectives.tag != 'Perspectives':
        log.error("{} Did not find required parent 'Perspectives' tag.  Please check the Web Server: '{}' and that Method: SchedulePerspectiveLookup.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent Perspectives tag from Method: SchedulePerspectiveLookup.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return '', ''
    else:

        for Perspective in Perspectives:
            AllState1 = {'Perspective_SK':'',
                         'Code':'',
                         'Description':'',
                         'UpdatedBy_SK':'',
                         'SnowflakeID':'',
                         'UpdatedOn':''}

            AllState2 = {'State_SK':'',
                         'Default':'',
                         'Code':'',
                         'Description':''}

            if Perspective.tag == 'Perspective':
               AllState1['Perspective_SK'] = Perspective.attrib['SK'].strip()
               if not l_perspective_sk:
                  l_perspective_sk = AllState1['Perspective_SK']

            for item in Perspective:
               if item.tag in ('Code', 'Description', 'UpdatedOn'):
                  try:
                      AllState1[item.tag] = item.text.strip()
                  except (KeyError, TypeError, ValueError, AttributeError):
                      log.info("{} XML tag '{}' expected but not found".format(FUNC,item.tag))

               elif item.tag == 'UpdatedBy':
                      AllState1['UpdatedBy_SK'] = item.attrib['SK'].strip()
                      AllState1['SnowflakeID'] = item.attrib['SnowflakeID'].strip()
               elif item.tag == 'State':
                  AllState2['State_SK'] = item.attrib['SK'].strip()
                  AllState2['Default'] = item.get('Default')
                  if AllState2['Default']:
                     l_state_sk=AllState2['State_SK']

                  for subitem in item:
                     if subitem.tag in ('Code', 'Description'):
                        AllState2[subitem.tag] = subitem.text.strip()
                     else:
                        continue

                  vars = (AllState1['Perspective_SK'], AllState1['Code'], AllState1['Description'], AllState1['UpdatedBy_SK'], AllState1['SnowflakeID'], AllState1['UpdatedOn'], AllState2['State_SK'], AllState2['Default'], AllState2['Code'], AllState2['Description'])

                  try:
                     cur = db.cursor()
                     cur.execute(INSERT_SCHEDULEPERSPECTIVELOOKUP, vars)
                  except psycopg2.Error as e:
                     log.error("{} Insert error for values {} or {}:{}".format(FUNC, AllState1,AllState2, e))

    log.info("Exiting {} ".format(FUNC,))

    if not l_state_sk:
        log.error("{} Did not find required parent 'State SK' tag.  Please check the Web Server: '{}' and that Method: SchedulePerspectiveLookup.ewfm is providing a response.  WFM Motivate Connector will try again in 60 seconds.".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent State SK tag from Method: SchedulePerspectiveLookup.ewfm is not sending data.'
        datadog_log('error', log_msg, db)
        return '', ''

    return l_perspective_sk, l_state_sk

#-------------------------------------------------------------------------

def get_adminuserlookup(db):
    """ Used to get the SnowflakeID if via """

    FUNC="Function get_adminuserlookup: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_adminuserlookup")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/AdminUserLookup.ewfm'
    base_data = 'data_in=<UserLookup HideInactiveUsers="true"/>& NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        Users = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if Users.tag != 'Users':
        log.error("{} Did not find required parent 'Users' tag.  Please check the Web Server: '{}' and that Method: AdminUserLookup.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent Users tag from Method: AdminUserLookup.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return
    else:
        for user in Users:
            AllUsers = {'User_SK':'',
                        'SnowflakeID':'',
                        'Active':'',
                        'UserName':'',
                        'AltUserName':'',
                        'SystemAdministrator':'',
                        'FilterProfile_SK':'',
                        'SecurityProfile_SK':'',
                        'SeatFilterProfile':'',
                        'Employee_SK':'',
                        'FirstName':'',
                        'LastName':'',
                        'FirstDayOfWeek':'',
                        'TimeZone_SK':'',
                        'FullName':'',
                        'SecondaryLoginID':''}

            if user.tag == 'User':
                try:
                    AllUsers['User_SK'] = user.attrib['SK'].strip()
                    AllUsers['SnowflakeID'] = user.attrib['SnowflakeID'].strip()
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.error("{} User SK and/or SnowflakeID attribute not found".format(FUNC,))

                for item in user:
                    if item.tag in ('Active', 'UserName', 'AltUserName','SystemAdministrator','FirstName','LastName','FirstDayOfWeek','FullName','SecondaryLoginID','SeatFilterProfile'):
                        try:
                            AllUsers[item.tag] = item.text.strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            if item.tag not in ('AltUserName','SecondaryLoginID','SeatFilterProfile'):
                                log.info("{} XML tag '{}' expected but not found".format(FUNC,item.tag))

                    elif item.tag in ('FilterProfile','SecurityProfile','Employee','TimeZone'):
                        try:
                            if item.tag == 'FilterProfile':
                                AllUsers['FilterProfile_SK'] = item.attrib['SK'].strip()
                            elif item.tag == 'SecurityProfile':
                                AllUsers['SecurityProfile_SK'] = item.attrib['SK'].strip()
                            elif item.tag == 'Employee':
                                AllUsers['Employee_SK'] = item.attrib['SK'].strip()
                            elif item.tag == 'TimeZone':
                                AllUsers['TimeZone_SK'] = item.attrib['SK'].strip()
                        except (KeyError, TypeError, ValueError, AttributeError):
                            log.info("{} Item SK attribute not found".format(FUNC,))
                    else:
                        log.info("{} Found unused tag '{}', skipping".format(FUNC, item.tag))
            else:
                log.info("{} Found unknown tag '{}' ".format(FUNC, user.tag))

            vars = (AllUsers['User_SK'], AllUsers['SnowflakeID'], AllUsers['Active'], AllUsers['UserName'], AllUsers['AltUserName'], AllUsers['SystemAdministrator'], AllUsers['FilterProfile_SK'], AllUsers['SecurityProfile_SK'], AllUsers['SeatFilterProfile'], AllUsers['Employee_SK'], AllUsers['FirstName'], AllUsers['LastName'], AllUsers['FirstDayOfWeek'], AllUsers['TimeZone_SK'], AllUsers['FullName'], AllUsers['SecondaryLoginID'])

            try:
                cur.execute(INSERT_ADMINUSERlOOKUP, vars)
            except psycopg2.Error as e:
                log.error("{} Insert error for values {}:{}".format(FUNC, AllUsers, e))

    try:
        cur.execute("UPDATE tmp_employeelookup \
                        SET snowflake = tmp_adminuserlookup.snowflakeid, \
                            user_sk   = tmp_adminuserlookup.user_sk \
                       FROM tmp_adminuserlookup \
                      WHERE tmp_employeelookup.employee_sk = tmp_adminuserlookup.employee_sk")
    except (psycopg2.Error, OSError, TypeError, IndexError, ValueError) as e:
            log.error("{} UPDATE tmp_employeelookup error: '{}'  ".format(FUNC, e))
            log_msg = FUNC + ' UPDATE tmp_employeelookup error: ' + str(e)
            datadog_log('error', log_msg, db)

    log.info("Exiting {} ".format(FUNC,))

#-------------------------------------------------------------------------

def get_employees(db, l_employees_grp, l_fromdate, l_todate):
    """ Load employees from the WFM database """

    FUNC="Function get_employees: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_employeelookup")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/EmployeeLookup.ewfm'
    base_data = 'data_in=<EmployeeSelector HideInactiveEmployees="true"><EmployeeFilter><EmployeeGroups>' +l_employees_grp+ '<FromDate>' +l_fromdate+ '</FromDate><ToDate>' +l_todate+ '</ToDate></EmployeeGroups></EmployeeFilter></EmployeeSelector>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        emps = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if emps.tag != 'Employees':
        log.error("{} Did not find required parent 'Employees' tag.  Please check the Web Server: '{}' and that Method: EmployeeLookup.ewfm is providing a response. ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent Employees tag from Method: EmployeeLookup.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return ''

    ret_employee_sk = ""
    for emp in emps:
        if emp.tag != 'Employee':
            log.info("{} Found child tag '{}' instead of Employee tag, skipping".format(FUNC, emp.tag))
            continue

        empdict = {'employee_sk':'',
                   'ID':'',
                   'Name':'',
                   'LastName':'',
                   'FirstName':'',
                   'ShortName':'',
                   'SortName':'',
                   'UpdatedBy_SK':'',
                   'UpdatedBy_SnowflakeID':'',
                   'UpdatedOn':''}

        try:
            empdict['employee_sk'] = emp.attrib['SK'].strip()
        except (KeyError, TypeError, ValueError, AttributeError):
            log.info("{} Employee SK attribute not found".format(FUNC,))

        for item in emp:
            if item.tag == 'UpdatedBy':
                try:
                    empdict['UpdatedBy_SK'] = item.attrib['SK'].strip()
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.info("{} UpdatedBy SK attribute not found".format(FUNC,))

                try:
                    empdict['UpdatedBy_SnowflakeID'] = item.attrib['SnowflakeID'].strip()
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.info("{} UpdatedBy SnowflakeID attribute not found".format(FUNC,))
            else:
                try:
                    empdict[item.tag] = item.text.strip()
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.info("{} XML tag '{}' expected but not found".format(FUNC, item.tag))

        vars = (empdict['employee_sk'], empdict['ID'], empdict['Name'], empdict['LastName'], empdict['FirstName'], empdict['ShortName'], empdict['SortName'], empdict['UpdatedBy_SK'], empdict['UpdatedBy_SnowflakeID'], empdict['UpdatedOn'], l_siteid)

        ret_employee_sk += "<Employee SK=\""
        ret_employee_sk += empdict['employee_sk']
        ret_employee_sk += "\"/>"

        try:
            cur.execute(INSERT_EMPLOYEELOOKUP, vars) 
        except psycopg2.Error as e:
            log.error("{} Insert error for values {}:{}".format(FUNC, empdict, e))

    log.info("Exiting {} ".format(FUNC,))
    return ret_employee_sk

#------------------------------------------------------------------------------------------------

def get_employeesupergroups(db):
    """ Query employee supergroups defined in the WFM database """

    FUNC="Function get_employeesupergroups: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_suppergrp")

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/EmployeeSuperGroups.ewfm'
    base_data = 'data_in=<EmployeeSuperGroupLookup><Code>' +wfm_supergroup+ '</Code></EmployeeSuperGroupLookup>&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        emps = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if emps.tag != 'EmployeeSuperGroups':
        log.error("{} Did not find required parent 'EmployeeSuperGroups' tag.  Please check the Web Server: '{}' and that Method: EmployeeSuperGroups.ewfm is providing a response.  ".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent EmployeeSuperGroups tag from Method: EmployeeSuperGroups.ewfm is not sending data.'
        datadog_log('info', log_msg, db)
        return ''

    for emp in emps:
        if emp.tag != 'EmployeeSuperGroup':
            log.info("{} Found child tag '{}' instead of EmployeeSuperGroup tag, skipping".format(FUNC, emp.tag))
            continue

        metadata = {'EmployeeSuperGroup_SK':'',
                    'Code':'',
                    'Description':'',
                    'UpdatedBy_SK':'',
                    'UpdatedBy_SnowflakeID':'',
                    'UpdatedOn':''}

        employees = []

        try:
            metadata['EmployeeSuperGroup_SK'] = emp.attrib['SK'].strip()
        except (KeyError, TypeError, ValueError, AttributeError):
            log.info("{} EmployeeSuperGroup SK attribute not found".format(FUNC,))

        for item in emp:
            if item.tag in ('Code', 'Description', 'UpdatedBy', 'UpdatedOn'):
                if item.tag == 'UpdatedBy':
                    try:
                        metadata['UpdatedBy_SK'] = item.attrib['SK'].strip()
                    except (KeyError, TypeError, ValueError, AttributeError):
                        log.info("{} UpdatedBy SK attribute not found".format(FUNC,))
            
                    try:
                        metadata['UpdatedBy_SnowflakeID'] = item.attrib['SnowflakeID'].strip()
                    except (KeyError, TypeError, ValueError, AttributeError):
                        log.info("{} UpdatedBy SnowflakeID attribute not found".format(FUNC,))
                else:
                    try:
                        metadata[item.tag] = item.text.strip()
                    except (KeyError, TypeError, ValueError, AttributeError):
                        log.info("{} XML tag {} expected but not found".format(FUNC, item.tag))
            elif item.tag == 'EmployeeGroup':
                try:
                    employees.append(item.attrib['SK'].strip())
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.error("{} UpdatedBy SnowflakeID attribute not found".format(FUNC,))

        ret_employee_group_sk = ""
        for x in range(len(employees)):
            vars = (metadata['EmployeeSuperGroup_SK'], metadata['Code'], metadata['Description'], metadata['UpdatedBy_SK'], metadata['UpdatedBy_SnowflakeID'], metadata['UpdatedOn'], employees[x])
            ret_employee_group_sk += "<EmployeeGroup SK=\""
            ret_employee_group_sk += employees[x]
            ret_employee_group_sk += "\"/>"
            try:
                cur.execute(INSERT_SUPPERGRP, vars) 
            except psycopg2.Error as e:
                log.error("{} Insert error for values {}:{}".format(FUNC, empdict, e))

    log.info("Exiting {} ".format(FUNC,))
    return ret_employee_group_sk
 
#------------------------------------------------------------------------------------------------

def get_siteid(db, wfm_server_name, wfm_username, wfm_password, wfm_db_name, wfm_transport_timeout):
    """ Get site id if not via """

    FUNC="Function get_siteid: "
    log.info("Entering {} ".format(FUNC,))

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/AdminSysInfo.ewfm'
    base_data = 'data_in=&NoStyle=&NoErrorStyle=&Timeout=5'

    ##log.info("{} URL Call Line: '{}'    ".format(FUNC,url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = FUNC + ' Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        sysinfo = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = FUNC + ' XML parsing failure due to: ' + str(e) + ' Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    siteid = 'SiteID_not_found'
    if sysinfo.tag != 'SysInfo':
        log.error("{} Did not find required parent 'SysInfo' tag.  Please check the Web Server: '{}' and that Method: AdminSysInfo.ewfm is providing a response.  WFM Motivate Connector will try again in 60 seconds.".format(FUNC, wfm_server_name))
        log_msg = FUNC + ' Did not find required parent SysInfo tag from Method: AdminSysInfo.ewfm is not sending data.'
        datadog_log('error', log_msg, db)
    else:
        for item in sysinfo:
            if item.tag == 'SiteID':
                try:
                    siteid = item.text.strip()
                except (KeyError, TypeError, ValueError, AttributeError):
                    log.info("{} SiteID SK attribute not found!  ".format(FUNC,))

                if not siteid:
                    siteid = 'SiteID_not_found'

    if siteid == 'SiteID_not_found':
        log.error("{} Site Id is required for NON VIA systems, please check with the WFM web server. ".format(FUNC,))
        log_msg = FUNC + ' Site Id is required for NON VIA systems, please check with the WFM web server'
        datadog_log('error', log_msg, db)
        sys.exit(1)
    else:
        log.info("{} Found SiteID: '{}' ".format(FUNC, siteid))

    log.info("Exiting {} ".format(FUNC,))
    return siteid
                
#------------------------------------------------------------------------------------------------

def get_ping(db):
    FUNC="Function get_ping: "
    log.info("Entering {} ".format(FUNC,))

    base_url = GC_http + '://' +wfm_server_name+ '/WFMWebSvcs/' +wfm_db_name+ '/ENU/API/servlet/AdminPing.ewfm'
    base_data = 'NoErrorStyle=&NoStyle=&Timeout=5'

    log.info("{} URL Call Line: '{}'    ".format(FUNC,base_url))
    try:
        r = requests.post(url=base_url, data=base_data, headers=base_headers, auth=(wfm_username, wfm_password), timeout=wfm_transport_timeout)
    except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
        log.error("{} Web failure due to '{}' ".format(FUNC,e))
        log_msg = 'Function get_ping Web failure due to: ' + str(e)
        datadog_log('error', log_msg, db)
        sys.exit(1)

    try:
        ping_me = ET.fromstring(r.text)
    except ET.ParseError as e:
        log.error("{} XML parsing failure due to '{}' ".format(FUNC, e))
        log.error("{} Received unexpected response of: '{}' ".format(FUNC, r.text))
        log_msg = 'Function get_ping Received unexpected response of: ' + r.text
        datadog_log('error', log_msg, db)
        sys.exit(1)

    if ping_me.tag != 'Ping':
        log.info("{} Did not find parent Ping tag".format(FUNC,))
        log.error("{} Did not find required parent 'Ping' tag.  Please check the Web Server: '{}' and that Method: AdminPing.ewfm is providing a response.  WFM Motivate Connector will try again in 60 seconds.".format(FUNC, wfm_server_name))
        log_msg = 'Function get_ping Did not find parent Ping tag .  Please check the Web Server: ' + wfm_server_name 
        datadog_log('error', log_msg, db)
        sys.exit(1)
    else:
        pingdic = {'ComputerName':'',
                   'ProcessID':'',
                   'ThreadID':'',
                   'Module':'',
                   'CurrentDirectory':'',
                   'SystemDateTime':'',
                   'WindowsUserName':'',
                   'Library':''}

        for items in ping_me:
            try:
                pingdic[items.tag] = items.text.strip()
            except (KeyError, TypeError, ValueError, AttributeError):
                log.info("{} XML tag '{}' expected but not found!  ".format(FUNC, pingdic.tag))

    if not pingdic['ComputerName']:
        log.error("{} Unable to PING '{}' Web Server, aborting!  ".format(FUNC, wfm_server_name))
        log_msg = 'Function get_ping Unable to PING Web Server: ' + wfm_server_name 
        datadog_log('error', log_msg, db)
        sys.exit(1)

    log.info("{} PINGing: '{}' and it returned:  '{}'  ".format(FUNC, wfm_server_name, pingdic))
    log.info("Exiting {} ".format(FUNC,))
                
#------------------------------------------------------------------------------------------------

def add_to_nsc_basedata(db, l_time_now, l_today, l_actual_date, l_last_run_time, l_state_sk):
    """ This captures the Adherence information cummulative, runs ever X min , from 00 hour to now hour """ 

    FUNC="Function add_to_nsc_basedata: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db.cursor()
    cur2 = db.cursor()
    cur3 = db.cursor()
    l_date_proc_time = l_last_run_time[11:19]

    cur1.execute("SELECT employee_sk, snowflake, to_char(nominaldate::date, 'MMDDYY'), earlieststart, lateststop, to_char(earlieststart::timestamp, 'MM/DD/YYYY HH24:MI:SS') \
                    FROM tmp_today_roster ORDER BY employee_sk, nominaldate")

    for l_employee_sk, l_snowflake, l_nominaldate2, l_earlieststart, l_lateststop, l_proc_earlieststart in cur1:
        l_agt_in_adherence = 0
        l_agt_out_adherence = 0

        if l_siteid == 'via' and (l_snowflake == None or l_snowflake == ''):
            log.info("{} Snowflake is missing for employee_sk: '{}' NOT adding to nsc_basedata table. Snowflake is required for VIA systems, please check with the WFM web server. ".format(FUNC,l_employee_sk))
            continue

        cur2.execute("select adh_start, adh_stop, scheduled_sk, actual_sk, \
                             EXTRACT(EPOCH FROM (adh_stop::timestamp - adh_start::timestamp)) as seconds \
                        from tmp_adh_emp_basedata_adherence \
                       where employee_sk= %s \
                         and (adh_start::timestamp >= %s::timestamp \
                         and adh_stop::timestamp <= %s::timestamp) \
                    order by adh_start", (l_employee_sk, l_earlieststart, l_lateststop))

        for l_adh_start, l_adh_stop, l_scheduled_sk, l_actual_sk, l_seconds in cur2:
            if l_scheduled_sk == l_actual_sk:
               l_agt_in_adherence += int(l_seconds)
            else:
               l_agt_out_adherence += int(l_seconds)


        l_campaign = "XXXX"
        l_date_orig = l_time_now
        l_date_proc = l_proc_earlieststart
        if l_siteid == 'via':
               l_agent = l_snowflake
        else:
               l_agent = l_siteid + '_' + l_employee_sk
        l_session_id = l_agent + '_' + l_actual_date + '_' + l_nominaldate2 + '_' + l_campaign + '_0'

        cur3.execute(INSERT_NSC_BASEDATA, (l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_agt_in_adherence, l_agt_out_adherence))
        log.info("{} Adding/Update to nsc_basedata: {}, {}, {}, {}, {}, {}, {}  ".format(FUNC,l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_agt_in_adherence, l_agt_out_adherence))

    cur3.close()
    cur2.close()
    cur1.close()
    log.info("Exiting {} ".format(FUNC,))

#------------------------------------------------------------------------------------------------

def initial_roster(db, l_time_now, l_today, l_state_sk, wfm_tardy_wait, l_actual_date):
    """ Determine if Roster for the full day,  this is run every 4 hour, or at rerun time  """
 
    FUNC="Function initial_roster: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db.cursor()
    cur2 = db.cursor()
    cur3 = db.cursor()
    cur1.execute("select employee_sk, snowflake, \
                         to_char(nominaldate::date, 'MMDDYY'), \
                         earlieststart, stop, \
                         to_char(earlieststart::timestamp, 'HH24:MI:SS'), \
                         to_char(earlieststart::timestamp, 'MM/DD/YYYY HH24:MI:SS') \
                    from tmp_today_roster order by earlieststart")

    for l_employee_sk, l_snowflake, l_nominaldate2, l_earlieststart, l_lateststop, l_date_proc_time, l_proc_earlieststart in cur1:
        if l_siteid == 'via' and (l_snowflake == None or l_snowflake == ''):
            log.info("{} Snowflake is missing for employee_sk: '{}' NOT adding to wfm_daily_roster table.  Snowflake is required for VIA systems, please check with the WFM web server.  ".format(FUNC, l_employee_sk))
            continue

        l_wfm_rostered='true'
        l_wfm_tardy='false'
        l_wfm_absent='false'
        l_sched_start=''

        l_campaign = "XXXX"
        l_date_orig = l_time_now
        l_date_proc = l_proc_earlieststart
        if l_siteid == 'via':
           l_agent = l_snowflake
        else:
           l_agent = l_siteid + '_' + l_employee_sk

        l_session_id = l_agent + '_' + l_nominaldate2 + '_' + l_campaign + '_0'
        cur2.execute("select session_id from wfm_daily_roster where session_id = %s", (l_session_id,))
        if cur2.rowcount==0:
           cur3.execute(INITIAL_INSERT_WFM_DAILY_ROSTER, (l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))
           log.info("{} Adding to wfm_daily_roster: {}, {}, {}, {}, {}, {}, {}  ".format(FUNC,l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))

    cur3.close()
    cur2.close()
    cur1.close()
    log.info("Exiting {} ".format(FUNC,))

#------------------------------------------------------------------------------------------------

def add_to_wfm_daily_roster(db, l_time_now, l_today, l_state_sk, wfm_tardy_wait):
    """  Determine Tardy and Absent """

    FUNC="Function add_to_wfm_daily_roster: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db.cursor()
    cur2 = db.cursor()
    cur3 = db.cursor()
    cur4 = db.cursor()
    cur5 = db.cursor()

    cur1.execute("SELECT employee_sk, snowflake, \
                         to_char(nominaldate::date, 'MMDDYY'), \
                         earlieststart, stop, \
                         to_char(earlieststart::timestamp, 'HH24:MI:SS'), \
                         to_char(earlieststart::timestamp, 'MM/DD/YYYY HH24:MI:SS'), \
                         to_char(earlieststart::timestamp, 'YYYY-MM-DD'), \
                         to_char((earlieststart::timestamp + interval '%s minutes'), 'MM/DD/YYYY HH24:MI:SS'), \
                         to_char((earlieststart::timestamp - interval '180 minutes'), 'MM/DD/YYYY HH24:MI:SS'), \
                         to_char((earlieststart::timestamp + interval '180 minutes'), 'MM/DD/YYYY HH24:MI:SS') \
                    FROM tmp_today_roster ORDER BY employee_sk, earlieststart", (wfm_tardy_wait,))

    for l_employee_sk, l_snowflake, l_nominaldate2, l_earlieststart, l_lateststop, l_date_proc_time, l_proc_earlieststart, l_proc_date, l_earlieststart15, l_timebeg, l_timeend in cur1:

        l_campaign = "XXXX"
        l_wfm_rostered='false'
        l_wfm_tardy='false'
        l_wfm_absent='true'
        l_sched_start=''
        l_actual_start=''
        l_sched_start4=''
        l_actual_start2=''
        l_date_orig = l_time_now
        l_time_now2 = datetime.strptime(l_time_now, "%Y-%m-%d %H:%M:%S") 
        l_earlieststart2 = datetime.strptime(l_earlieststart15, "%m/%d/%Y %H:%M:%S") 
        l_date_proc = l_proc_earlieststart

        if l_siteid == 'via':
           l_agent = l_snowflake
        else:
           l_agent = l_siteid + '_' + l_employee_sk

        if l_siteid == 'via' and (l_snowflake == None or l_snowflake == ''):
            log.info("{} Snowflake is missing for employee_sk: '{}' NOT adding to wfm_daily_roster table. Snowflake is required for VIA systems, please check with the WFM web server. ".format(FUNC, l_employee_sk))
            continue

        l_session_id = l_agent + '_' + l_nominaldate2 + '_' + l_campaign + '_0'

        cur5.execute("SELECT session_id FROM wfm_daily_roster where session_id = %s AND (wfm_tardy = 'true' or wfm_absent= 'true')", (l_session_id,))
        if cur5.rowcount>0:
            log.info("{} The employee_sk: '{}',  session_id: '{}' already processed in wfm_daily_roster table, skipping.  ".format(FUNC,l_employee_sk, l_session_id))
            continue

        if l_time_now2 < l_earlieststart2:
           log.info("{} Snowflake: '{}', Not time yet to process employee_sk: '{}', time NOW: {} is LESS than EarliestStart: {}  ".format(FUNC, l_snowflake, l_employee_sk, l_time_now2, l_earlieststart2))
           continue

        l_sched_start = l_earlieststart
        l_wfm_rostered='true'

        cur3.execute("SELECT actual_start FROM tmp_adh_emp_roster_actual \
                       WHERE employee_sk= %s AND actual_state_sk != %s \
                         AND actual_start::timestamp between %s::timestamp AND %s::timestamp \
                    ORDER BY actual_start limit 1", (l_employee_sk, l_state_sk, l_timebeg, l_timeend))

        if cur3.rowcount>0:
            for l_actual_start in cur3:
                l_actual_start = "%s" % (l_actual_start)
                l_actual_start2 = datetime.strptime(l_actual_start, "%Y-%m-%dT%H:%M:%SZ") 
                l_sched_start2 = datetime.strptime(l_sched_start, "%Y-%m-%dT%H:%M:%SZ") 
                l_sched_start3 = (l_sched_start2 + timedelta(minutes=wfm_tardy_wait)).strftime('%Y-%m-%d %H:%M:%S')
                l_sched_start4 = datetime.strptime(l_sched_start3, "%Y-%m-%d %H:%M:%S") 
   
                if l_sched_start4 >= l_actual_start2:
                    l_wfm_absent='false'
                    l_wfm_tardy='false'
                else:
                    l_wfm_absent='false'
                    l_wfm_tardy='true'
        else:
            l_wfm_absent='true'
            l_wfm_tardy='false'

        if l_sched_start:
           cur4.execute(INSERT_WFM_DAILY_ROSTER, (l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))
           log.info("{} Adding/Update to wfm_daily_roster:{}, {}, {}, {}, {}, {}, {}  ".format(FUNC,l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))
        else:
           l_wfm_rostered='false'
           l_wfm_tardy='false'
           l_wfm_absent='false'
           cur4.execute(INSERT_WFM_DAILY_ROSTER, (l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))
           log.info("{} Set to FALSE wfm_daily_roster:{}, {}, {}, {}, {}, {}, {}  ".format(FUNC,l_session_id, l_date_orig, l_date_proc, l_agent, l_campaign, l_wfm_rostered, l_wfm_absent, l_wfm_tardy))

    cur5.close()
    cur4.close()
    cur3.close()
    cur2.close()
    cur1.close()
    log.info("Exiting {} ".format(FUNC,))

#------------------------------------------------------------------------------------------------
def get_create_today_roster(db, l_state_sk, l_today):
    FUNC="Function get_create_today_roster: "
    log.info("Entering {} ".format(FUNC,))
    cur = db.cursor()
    cur.execute("TRUNCATE TABLE tmp_today_roster")

    try:
        cur.execute("INSERT INTO tmp_today_roster (employee_sk, start, stop, nominaldate) \
                     SELECT employee_sk, min(start), max(stop), nominaldate \
                       FROM tmp_scheduletimeperiodres \
                      WHERE state_sk != %s \
                        AND (start::date = %s OR stop::date = %s) GROUP BY employee_sk, nominaldate", (l_state_sk, l_today, l_today))
    except (psycopg2.Error, OSError, TypeError, IndexError, ValueError) as e:
            log.error("{} INSERT tmp_today_roster error from tmp_scheduletimeperiodres: '{}'  ".format(FUNC, e))
            log_msg = FUNC + ' INSERT tmp_today_roster error from tmp_scheduletimeperiodres:: ' + str(e)
            datadog_log('error', log_msg, db)

    try:
        cur.execute("UPDATE tmp_today_roster \
                        SET snowflake = tmp_adminuserlookup.snowflakeid \
                       FROM tmp_adminuserlookup \
                      WHERE tmp_today_roster.employee_sk = tmp_adminuserlookup.employee_sk")
    except (psycopg2.Error, OSError, TypeError, IndexError, ValueError) as e:
            log.error("{} UPDATE tmp_today_roster error on snowflake: '{}'  ".format(FUNC, e))
            log_msg = FUNC + ' UPDATE tmp_today_roster error on snowflake from tmp_adminuserlookup: ' + str(e)
            datadog_log('error', log_msg, db)

    try:
        cur.execute("UPDATE tmp_today_roster \
                        SET earlieststart = tmp_today_roster_shift.earlieststart, \
                            lateststop = tmp_today_roster_shift.lateststop \
                       FROM tmp_today_roster_shift \
                      WHERE tmp_today_roster.employee_sk = tmp_today_roster_shift.employee_sk \
                        AND tmp_today_roster.nominaldate = tmp_today_roster_shift.nominaldate")
    except (psycopg2.Error, OSError, TypeError, IndexError, ValueError) as e:
            log.error("{} UPDATE tmp_today_roster error on earlieststart: '{}'  ".format(FUNC, e))
            log_msg = FUNC + ' UPDATE tmp_today_roster error on earlieststart from tmp_today_roster_shift: ' + str(e)
            datadog_log('error', log_msg, db)

    cur.execute("select min(earlieststart) from tmp_today_roster")
    for l_min_start_date in cur:
        l_min_start_date = "%s" % (l_min_start_date)

    log.info("Exiting {} ".format(FUNC,))
    return l_min_start_date

#------------------------------------------------------------------------------------------------

def calculate_days(l_time_now_date, l_cal):
    xdate = datetime.strptime(l_time_now_date, "%Y-%m-%d")
    if l_cal == 'less2':
       EndDate = xdate - timedelta(days=2)
    else:
       EndDate = xdate + timedelta(days=2)

    EndDate = str(EndDate)
    EndDate = EndDate[0:10]

    l_actual_date = xdate.strftime('%m%d%y')
    return EndDate, l_actual_date

#------------------------------------------------------------------------------------------------

def repopulate_tables(db_game, l_label):
    FUNC="Function repopulate_tables: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db_game.cursor()
    cur2 = db_game.cursor()
    cur3 = db_game.cursor()

    l_rerun_from = l_rerun_to = ''
    cur1.execute(SELECT_LAST_RUN_TIME)
    if cur1.rowcount>0:
       l_lasttime, l_rerun_from, l_rerun_to = cur1.fetchall()[0]

       if l_rerun_from and l_rerun_to:
           if l_rerun_from <= l_rerun_to:
              log.info("{} Date Ranges FROM: '{}' TO: '{}'.   ".format(FUNC, l_rerun_from, l_rerun_to ))
              cur2.execute("select to_char(now(), 'YYYY-MM-DD HH24:MI:SS'), \
                            generate_series(rerun_from, rerun_to, '1 day'::interval) as rerun_date from wfmc_repull")

              for l_time_now, l_rerun_date in cur2:
                  l_rerun_date = "%s" % (l_rerun_date)
                  l_time_now_date = l_rerun_date[0:10]
                  l_time_now_00 = l_time_now_date + 'T00:00:00'
                  l_time_now_24 = l_time_now_date + 'T23:59:59'
                  l_time_now_with_t = l_time_now_24
                  l_time_now_less_2d, l_actual_date = calculate_days(l_time_now_date, 'less2')
                  l_time_now_more_2d, l_actual_date = calculate_days(l_time_now_date, 'more2')

                  log.info("{} {}  ".format(FUNC, l_label))
                  log.info("{} Rerun Variables: l_rerun_date      : '{}'.   ".format(FUNC, l_rerun_date))
                  log.info("{}                  l_time_now        : '{}'.   ".format(FUNC, l_time_now))
                  log.info("{}                  l_actual_date     : '{}'.   ".format(FUNC, l_actual_date))
                  log.info("{}                  l_time_now_date   : '{}'.   ".format(FUNC, l_time_now_date))
                  log.info("{}                  l_time_now_00     : '{}'.   ".format(FUNC, l_time_now_00))
                  log.info("{}                  l_time_now_24     : '{}'.   ".format(FUNC, l_time_now_24))
                  log.info("{}                  l_time_now_with_t : '{}'.   ".format(FUNC, l_time_now_with_t))
                  log.info("{}                  l_time_now_less_2d: '{}'.   ".format(FUNC, l_time_now_less_2d))
                  log.info("{}                  l_time_now_more_2d: '{}'.   ".format(FUNC, l_time_now_more_2d))

                  l_ContainerSpans = ''
                  l_perspective_sk, l_state_sk = get_scheduleperspectivelookup(db_game)
                  l_emp_grp = get_employeesupergroups(db_game)
                  l_list_of_employees = get_employees(db_game,l_emp_grp, l_time_now_less_2d, l_time_now_more_2d)
                  if l_list_of_employees and l_perspective_sk and l_state_sk and l_emp_grp:
                     if l_siteid == "via":
                        get_adminuserlookup(db_game)
                     get_schedulesegmentsummary(db_game, l_list_of_employees, l_time_now_less_2d, l_time_now_more_2d, l_time_now_date)
                     l_list_of_employees = get_scheduletimeperiodres(db_game, l_perspective_sk, l_time_now_date)
                     if l_list_of_employees:
                        if l_label == 'This is a Crash Recovery':
                           log_msg = 'This is a Crash Recovery for date: ' + l_rerun_date
                           datadog_log('info', log_msg, db_game)
                        l_min_start_date = get_create_today_roster(db_game, l_state_sk, l_time_now_date) 
                        l_ContainerSpans = get_adherenceemployeeadherence_roster(db_game, l_list_of_employees, l_perspective_sk, l_min_start_date, l_time_now_with_t)
                        if l_ContainerSpans:
                           initial_roster(db_game, l_time_now, l_time_now_date, l_state_sk, wfm_tardy_wait, l_actual_date)
                           add_to_wfm_daily_roster(db_game, l_time_now, l_time_now_date, l_state_sk, wfm_tardy_wait)
                           get_employeeadherence_basedata(db_game, l_list_of_employees, l_perspective_sk, l_time_now_00, l_time_now_with_t)
                           add_to_nsc_basedata(db_game, l_time_now, l_time_now_date, l_actual_date, l_time_now_00, l_state_sk)
                     else:
                        log.info("{} No List of Employees to process on this Rerun from get_scheduletimeperiodres.  ".format(FUNC,))
                  else:
                     log.info("{} No List of Employees to process on this Rerun.  ".format(FUNC,))
           else:
              log.info("{} The rerun_from: '{}'  needs to be less than rerun_to: '{}'.   ".format(FUNC, l_rerun_from, l_rerun_to ))

    cur3.execute("update wfmc_repull set rerun_from = null, rerun_to = null")
    log.info("Exiting {} ".format(FUNC,))
    cur3.close()
    cur2.close()
    cur1.close()

#------------------------------------------------------------------------------------------------
def check_for_crash(db):
    FUNC="Function check_for_crash: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db.cursor()
    cur2 = db.cursor()
    cur3 = db.cursor()

    l_min = wfm_frequency_min+20
    l_crash1 = '0'
    l_crash2 = '0'
    cur1.execute("select count(*) from wfmc_repull where lasttime::date !=  now()::date")
    for l_crash1 in cur1:
        l_crash1 = "%s" % (l_crash1)
        if l_crash1 == '0':
            log.info("Exiting {} ".format(FUNC,))
            return
        else:
            cur2.execute("select count(*) from wfmc_repull where lasttime < now() - INTERVAL '%s minutes'", (l_min,))
            for l_crash2 in cur2:
                l_crash2 = "%s" % (l_crash2)
                if l_crash2 == '1':
                   cur3.execute("update wfmc_repull set rerun_from = lasttime::date, rerun_to = now()::date")
                   repopulate_tables(db, 'This is a Crash Recovery')

    log.info("Exiting {} ".format(FUNC,))
    cur3.close()
    cur2.close()
    cur1.close()

#------------------------------------------------------------------------------------------------

def check_for_reruns(db):
    FUNC="Function check_for_reruns: "
    log.info("Entering {} ".format(FUNC,))
    cur1 = db.cursor()
    cur2 = db.cursor()

    cur1.execute("SELECT rerun_from, (now()::date - interval '31 days')::date FROM wfmc_repull WHERE rerun_from IS NOT NULL AND rerun_to IS NOT NULL")
    if cur1.rowcount>0:
       for l_rerun, l_date_minus30 in cur1:
           if l_rerun < l_date_minus30:
               log.info("{} The rerun date: '{}' is older than 30 days. It needs to be: '{}' or higher.".format(FUNC,l_rerun, l_date_minus30))
               cur2.execute("update wfmc_repull set rerun_from = null, rerun_to = null")
               log.info("Exiting {} ".format(FUNC,))
               return
           else:
               repopulate_tables(db, 'This is a Repull')

    log.info("Exiting {} ".format(FUNC,))
    cur2.close()
    cur1.close()

#------------------------------------------------------------------------------------------------

def run_every_X_min(db_game, current_hour):
    FUNC="Function run_every_X_min: "
    log.info("Entering {} ".format(FUNC,))
    cur = db_game.cursor()

    cur.execute(SELECT_LAST_RUN_TIME)
    if cur.rowcount==0:
        cur.execute("INSERT INTO wfmc_repull (lasttime) SELECT now() - interval '%s minutes'", (wfm_frequency_min,))

    l_perspective_sk = l_state_sk = l_emp_grp = l_list_of_employees = l_ContainerSpans = ''

    while True:
        check_for_reruns(db_game)
        check_for_crash(db_game)
        cur.execute(SELECT_NOW)
        if cur.rowcount>0:
            l_time_now = l_time_now_with_t = l_time_now_less_2d = l_time_now_more_2d = l_time_now_date = l_time_now_00 = l_actual_date = l_hr = ''
            l_time_now, l_time_now_with_t, l_time_now_less_2d, l_time_now_more_2d, l_time_now_date, l_time_now_00, l_actual_date, l_hr = cur.fetchall()[0]

    #   Every 1 hour and at Start up
        if l_hr != current_hour:
            log.info("{} Start Every Hour on UTC hour = {}.   ".format(FUNC, l_hr))
            l_perspective_sk, l_state_sk = get_scheduleperspectivelookup(db_game)
            if l_perspective_sk:
               l_emp_grp = get_employeesupergroups(db_game)
               if l_emp_grp:
                  l_list_of_employees = get_employees(db_game,l_emp_grp, l_time_now_less_2d, l_time_now_more_2d)
                  if l_list_of_employees:
                     if l_siteid == "via":
                         get_adminuserlookup(db_game)
                     get_schedulesegmentsummary(db_game, l_list_of_employees, l_time_now_less_2d, l_time_now_more_2d, l_time_now_date)
                     l_list_of_employees = get_scheduletimeperiodres(db_game, l_perspective_sk, l_time_now_date)
                     if l_list_of_employees:
                        l_min_start_date = get_create_today_roster(db_game, l_state_sk, l_time_now_date) 
                        l_ContainerSpans = get_adherenceemployeeadherence_roster(db_game, l_list_of_employees, l_perspective_sk, l_min_start_date, l_time_now_with_t)
                        if l_ContainerSpans:
                           if current_hour == '-1' or l_hr in ('00','04','08','12','16','20'):
                              initial_roster(db_game, l_time_now, l_time_now_date, l_state_sk, wfm_tardy_wait, l_actual_date)
                           add_to_wfm_daily_roster(db_game, l_time_now, l_time_now_date, l_state_sk, wfm_tardy_wait)

            current_hour = l_hr
            log.info("{} End Every Hour on UTC hour = {}.   ".format(FUNC, l_hr))

            #log_msg = 'WFM Motivate Connector Heart Beat every hour = : ' + l_hr
            #datadog_log('info', log_msg, db_game)

    #   Every wfm_frequency_min minutes
        log.info("{} Start Every Minute frequency of: {}   ".format(FUNC, wfm_frequency_min))
        if l_list_of_employees and l_ContainerSpans:
           get_employeeadherence_basedata(db_game, l_list_of_employees, l_perspective_sk, l_time_now_00, l_time_now_with_t)
           add_to_nsc_basedata(db_game, l_time_now, l_time_now_date, l_actual_date, l_time_now_00, l_state_sk)
        else:
           log.info("{} No List of Employees to process. WFM Web Server most likely not setup.  ".format(FUNC,))

        cur.execute("update wfmc_repull set lasttime = %s", (l_time_now,))
        log.info("{} End Every Minute frequency of: {}     ".format(FUNC, wfm_frequency_min))
        time.sleep(60*wfm_frequency_min)

    log.info("Exiting {} ".format(FUNC,))

#------------------------------------------------------------------------------------------------
def get_setup_variables():
    FUNC="Function get_setup_variables: "
    log.info("Entering {} ".format(FUNC,))
    global wfm_server_name , wfm_username , wfm_password , wfm_db_name , wfm_supergroup , wfm_frequency_min , wfm_tardy_wait , wfm_transport_timeout, GC_http, base_headers

    base_headers = {'Content-Type':'application/x-www-form-urlencoded'}
    GC_hostname = GC_dbname = GC_dbuser = GC_passwd = GC_http = GC_port = ''
    wfm_server_name, wfm_username, wfm_password, wfm_db_name, wfm_supergroup, is_it_via, wfm_frequency_min, wfm_tardy_wait, wfm_transport_timeout = '', '', '', '', '', '', 5, 15, 20

    GC_hostname = os.getenv("WFMC_HOSTNAME")
    GC_dbname =  os.getenv("WFMC_DBNAME")
    GC_dbuser =  os.getenv("WFMC_DBUSER")
    GC_passwd =  os.getenv("WFMC_PASS")
    GC_port =  os.getenv("WFMC_PORT")
    GC_http = os.getenv("WFMC_HTTP")

    wfm_server_name = os.getenv("WFM_SERVER_NAME")
    wfm_username = os.getenv("WFM_USERNAME")
    wfm_password = os.getenv("WFM_PASSWORD")
    wfm_db_name = os.getenv("WFM_DB_NAME")
    wfm_supergroup = os.getenv("WFM_SUPERGROUP")
    wfm_frequency_min = os.getenv("WFM_FREQUENCY_MIN")
    wfm_tardy_wait = os.getenv("WFM_TARDY_WAIT")
    wfm_transport_timeout = os.getenv("WFM_TRANSPORT_TIMEOUT")

    if not GC_hostname:
        GC_hostname='localhost'
    if not GC_dbname:
        GC_dbname='gamification'
    if not GC_dbuser:
        GC_dbuser='gamification'
    if not GC_http:
        GC_http='https'

    if not wfm_frequency_min:
        wfm_frequency_min=5
    else:
        wfm_frequency_min = int(wfm_frequency_min)

    if not wfm_tardy_wait:
        wfm_tardy_wait=15
    else:
        wfm_tardy_wait = int(wfm_tardy_wait)

    if not wfm_transport_timeout:
        wfm_transport_timeout=20
    else:
        wfm_transport_timeout = int(wfm_transport_timeout)

    if GC_port:
        GC_port = int(GC_port)

    log.info("   Motivate Host Name  : {}".format(GC_hostname,))
    log.info("   Motivate Database   : {}".format(GC_dbname,))
    log.info("   Motivate User Name  : {}".format(GC_dbuser,))
    log.info("   Motivate Port Number: {}".format(GC_port,))
    log.info("   Motivate HTTP Label : {}".format(GC_http,))
    log.info("Exiting {} ".format(FUNC,))
    return GC_hostname,GC_dbname,GC_dbuser,GC_passwd,GC_port

#------------------------------------------------------------------------------------------------

def setup_logfile():
    global log
    log_name='/var/log/wfmmotivateconnector/wfmmotivateconnector.log'
    GC_backupCount = GC_maxBytes = 0
    GC_backupCount = os.getenv("WFMC_LOGROTATE")
    GC_maxBytes = os.getenv("WFMC_MAXBYTES")

    if not GC_maxBytes or GC_maxBytes == 0:
        GC_maxBytes=10000000
    else:
        GC_maxBytes = int(GC_maxBytes)

    if not GC_backupCount or GC_backupCount == 0:
        GC_backupCount=10
    else:
        GC_backupCount = int(GC_backupCount)

    rfh = RotatingFileHandler(filename=log_name, maxBytes=GC_maxBytes, backupCount=GC_backupCount)
    rfh.setFormatter(logging.Formatter("%(asctime)s %(name)-25s %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(rfh)

    l_ver = l_build = l_wfm_version = ''
    try:
        with open('/usr/local/wfmmotivateconnector/VERSION') as f:
           l_ver = f.read()
           f.close()
    except FileNotFoundError as e:
        pass

    try:
        with open('/usr/local/wfmmotivateconnector/BUILD') as f:
           l_build = f.read()
           f.close()
    except FileNotFoundError as e:
        pass

    if l_ver and l_build:
       l_wfm_version = l_ver + '-' + l_build
    else:
       l_wfm_version = 'Missing file /usr/local/wfmmotivateconnector/VERSION or /usr/local/wfmmotivateconnector/BUILD'

    log.info("   ALVARIA Motivate Version: {}   ".format(l_wfm_version,))
    log.info("   Default Log file '{}' count: {}".format(log_name, GC_backupCount))
    log.info("   Default Log file '{}' bytes: {}".format(log_name, GC_maxBytes))

#------------------------------------------------------------------------------------------------

def datadog_log(l_alert_type, l_err_msg, db):
    ddcur = db.cursor()
    global DD_INSTALLED
    global DD_LOGGED

    l_title = 'WFMMotivateConnector'
    is_datagod_on = '0'
    ddcur.execute(SELECT_GAME_SETTING_DATADOG)
    if ddcur.rowcount>0:
        is_datagod_on = "%s" % ddcur.fetchall()[0]
    else:
        is_datagod_on = '0'

    if is_datagod_on == '1':
       if DD_INSTALLED:
           statsd.event(title=l_title, text=l_err_msg, hostname=wfm_server_name, alert_type=l_alert_type)
           log.info("DataDog Alert: {} - Msg: {}".format(l_alert_type, l_err_msg))
       else:
           if not DD_LOGGED:
              log.info("DataDog is NOT installed on: {}".format(wfm_server_name,))
              DD_LOGGED = True
    else:
       log.info("DataDog is OFF based on table game_setting WHERE name = 'use_datadog'")

#------------------------------------------------------------------------------------------------

def main():
    setup_logfile()
    global l_siteid
    GC_hostname, GC_dbname, GC_dbuser, GC_passwd, GC_port = get_setup_variables()
    # Connect to the Motivate server database
    try:
        if not GC_passwd:
            db_game = psycopg2.connect(host=GC_hostname, database=GC_dbname, user=GC_dbuser, application_name=APP, port=GC_port)
        else:
            db_game = psycopg2.connect(host=GC_hostname, database=GC_dbname, user=GC_dbuser, application_name=APP,  password=GC_passwd, port=GC_port)
    except psycopg2.Error as e:
        log.error("MAIN function: Unable to connect to Motivate Database= '{}' - '{}'! Please check the /var/www/nsc-gamification/htdocs/config.inc.php file".format(GC_hostname,e))
        datadog_log('error','Unable to connect to Motivate Database, Please check the /var/www/nsc-gamification/htdocs/config.inc.php file', db_game)
        sys.exit(1)

    db_game.autocommit=True

    is_it_via = '0'
    current_hour = '-1'
    cur = db_game.cursor()
    cur.execute("set timezone='UTC'")
    cur.execute(SELECT_GAME_SETTING)
    if cur.rowcount>0:
        is_it_via = "%s" % cur.fetchall()[0]
    else:
        is_it_via = '0'

    log.info("   ALVARIA Motivate Connector Start:  ")
    log.info("Web Server Name: '{}'  ".format(wfm_server_name,))
    log.info("    Database   : '{}'  ".format(wfm_db_name,))
    log.info("    User       : '{}'  ".format(wfm_username,))
    log.info("    Super Group: '{}'  ".format(wfm_supergroup,))
    log.info("    Frequency  : '{}'  ".format(wfm_frequency_min,))
    log.info("    Tardy Wait : '{}'  ".format(wfm_tardy_wait,))
    log.info(" VIA Deployment: '{}'  ".format(is_it_via,))
    log.info("  Trans timeout: '{}'  ".format(wfm_transport_timeout,))

    if not wfm_server_name or not wfm_db_name or not wfm_username or not wfm_supergroup:
        log.error("MAIN function: Setup variables on /etc/profile.d/noble.sh are not configured on '{}' host. Aborting!  wfm_server_name ='{}', wfm_db_name ='{}', wfm_username ='{}', wfm_supergroup ='{}'".format(GC_hostname,wfm_server_name, wfm_db_name, wfm_username, wfm_supergroup))
        datadog_log('error','Setup variables on /etc/profile.d/noble.sh are not configured', db_game)
        sys.exit(1)

    get_ping(db_game)
    create_tmp_tables(db_game, log)

    l_siteid = "via"
    if is_it_via == '0':
       l_siteid = get_siteid(db_game, wfm_server_name, wfm_username, wfm_password, wfm_db_name, wfm_transport_timeout)
    else:
       l_siteid = "via"

    datadog_log('info','WFM Motivate Connector Started', db_game)
    run_every_X_min(db_game, current_hour)

if __name__ == "__main__":
    sys.exit(main())
