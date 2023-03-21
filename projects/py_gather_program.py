# -----------------------------------------------------------------
# Gather program ids for storing & running in Shell script
# -----------------------------------------------------------------

import json, sys, time, os
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import requests
import datetime
from dateutil import parser
from slackclient import SlackClient

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
bot = SlackClient(config.get("SLACK","token"))

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]

with open(config.get('OUTPUT_DIR', 'program_text_file'), mode='w') as file:
    data = projectsCollec.distinct("programId", {"isAPrivateProgram": False, "isDeleted":False})
    for ids in data:
        ids = str(ids)
        if ids != 'None':
            file.write(f"{ids}\n")

bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Gathered ProgramIDs: {datetime.datetime.now()}. Checking previous ingestion...") 

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]

headers = {'Content-Type': 'application/json'}
payload = json.loads(config.get("DRUID","project_injestion_spec"))
druid_end_point = config.get("DRUID", "batch_url") + 's'
get_timestamp = requests.get(druid_end_point, headers=headers, params={'state': 'complete', 
                                'datasource': payload["spec"]["dataSchema"]["dataSource"]})

last_ingestion = json.loads(get_timestamp.__dict__['_content'].decode('utf8').replace("'", '"'))
last_timestamp = None
for tasks in last_ingestion:
    if tasks['type'] == 'index':
        last_timestamp = parser.parse((tasks['createdTime'])).date()

with open("./checker.txt", mode='w') as file:
    file.write(str(last_timestamp))



