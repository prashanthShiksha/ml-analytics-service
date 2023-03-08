# ----------------------------------- #
# Script to delete the project datasources 
# prior to batch ingestion
# ----------------------------------- #


import json, sys, time
from configparser import ConfigParser,ExtendedInterpolation
import os
import requests
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import datetime
from slackclient import SlackClient
from datetime import date

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
bot = SlackClient(config.get("SLACK","token"))

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'), when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(config.get('LOGS', 'project_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'project_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

payload = json.loads(config.get("DRUID","project_injestion_spec"))
datasources = payload["spec"]["dataSchema"]["dataSource"]
ingestion_specs = [json.dumps(payload)]
headers = {'Content-Type': 'application/json'}

druid_end_point = config.get("DRUID", "batch_url") + 's'
get_timestamp = requests.get(druid_end_point, headers=headers, params={'state': 'complete', 
                                'datasource': payload["spec"]["dataSchema"]["dataSource"]})

last_ingestion = json.loads(get_timestamp.__dict__['_content'].decode('utf8').replace("'", '"'))
last_timestamp = None
for tasks in last_ingestion:
    if tasks['type'] == 'index':
        last_timestamp = dateutil.parser.parse((tasks['createdTime'])).date()

cur_timestamp = (datetime.datetime.now() - datetime.timedelta(1)).date()
if cur_timestamp == last_timestamp:
    bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"ALERT: Duplicate Run. (DISALLOWED)")
    sys.exit()


bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** STARTED DELETION: {datetime.datetime.now()} ***********\n")
druid_end_point = config.get("DRUID", "metadata_url") + datasources
get_timestamp = requests.get(druid_end_point, headers=headers)
if get_timestamp.status_code == 200:
    bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Fetched Timestamp of {datasources} | Waiting for 50s")
    successLogger.debug("Successfully fetched time stamp of the datasource " + datasources )
    timestamp = get_timestamp.json()
    #calculating interval from druid get api
    minTime = timestamp["segments"]["minTime"]
    maxTime = timestamp["segments"]["maxTime"]
    min1 = datetime.datetime.strptime(minTime, "%Y-%m-%dT%H:%M:%S.%fZ")
    max1 = datetime.datetime.strptime(maxTime, "%Y-%m-%dT%H:%M:%S.%fZ")
    new_format = "%Y-%m-%d"
    min1.strftime(new_format)
    max1.strftime(new_format)
    minmonth = "{:02d}".format(min1.month)
    maxmonth = "{:02d}".format(max1.month)
    min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
    max2 = str(max1.year) + "-" + maxmonth  + "-" + str(max1.day)
    interval = min2 + "_" + max2
    time.sleep(50)
    successLogger.debug(f"sleep 50s")

    disable_datasource = requests.delete(druid_end_point, headers=headers)

    if disable_datasource.status_code == 200:
        successLogger.debug("successfully disabled the datasource " + datasources)
        time.sleep(300)
        successLogger.debug(f"sleep 300s")

        delete_segments = requests.delete(
            druid_end_point + "/intervals/" + interval, headers=headers
        )
        if delete_segments.status_code == 200:
            successLogger.debug("successfully deleted the segments " + datasources)
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Deletion check successfull for {datasources}")
            time.sleep(600)
            successLogger.debug(f"sleep 300s")

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 200 or enable_datasource.status_code == 204:
                successLogger.debug("successfully enabled the datasource " + datasources)
                time.sleep(600)
                successLogger.debug(f"sleep 600s")
            else:
                errorLogger.error("failed to enable the datasource " + datasources)
                errorLogger.error("failed to enable the datasource " + str(enable_datasource.status_code))
                errorLogger.error(enable_datasource.text)
                bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to enable {datasources} | Error: {enable_datasource.status_code}")
        else:
            errorLogger.error("failed to delete the segments of the datasource " + datasources)
            errorLogger.error("failed to delete the segments of the datasource " + str(delete_segments.status_code))
            errorLogger.error(delete_segments.text)
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to delete the {datasources}")
    else:
        errorLogger.error("failed to disable the datasource " + datasources)
        errorLogger.error("failed to disable the datasource " + str(disable_datasource.status_code))
        bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to disable the {datasources}")
        errorLogger.error(disable_datasource.text)
else:
    errorLogger.error("failed to get the timestamp of the datasource " + datasources)
    errorLogger.error("failed to get the timestamp of the datasource " + str(get_timestamp.status_code))
    errorLogger.error(get_timestamp.text)

bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** COMPLETED DELETION: {datetime.datetime.now()} ***********\n")
successLogger.debug(f"Ingestion end for raw {datetime.datetime.now()}")
