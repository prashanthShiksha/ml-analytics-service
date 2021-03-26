# -----------------------------------------------------------------
# Name : py_observation_evidence_streaming.py
# Author : Shakthieshwari.A
# Description : Extracts the Evidence or Files Attached at each question level 
#               during the observation submission
# -----------------------------------------------------------------
from pymongo import MongoClient
from bson.objectid import ObjectId
import csv,os
import json
import boto3
import datetime
from datetime import date,time
import requests
import argparse
from kafka import KafkaConsumer
from kafka import KafkaProducer
import dateutil
from dateutil import parser as date_parser
from configparser import ConfigParser,ExtendedInterpolation
import faust
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.\
    RotatingFileHandler(config.get('LOGS','observation_streaming_evidence_success_log_filename'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_evidence_success_log_filename'),
                                                when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.\
    RotatingFileHandler(config.get('LOGS','observation_streaming_evidence_error_log_filename'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_evidence_error_log_filename'),
                                              when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
 kafka_url = (config.get("KAFKA","kafka_url"))
 app = faust.App('sl_observation_evidences_diksha_faust',broker='kafka://'+kafka_url,value_serializer='raw',
                 web_port=7002,broker_max_poll_records=500)
 rawTopicName = app.topic(config.get("KAFKA","kafka_raw_data_topic"))
 producer = KafkaProducer(bootstrap_servers=[kafka_url])

 #db production
 clientdev = MongoClient(config.get('MONGO','mongo_url'))
 dbdev = clientdev[config.get('MONGO','database_name')]

 observationSubmissionsDevCollec = dbdev[config.get('MONGO','observation_sub_collec')]
 solutionsDevCollec = dbdev[config.get('MONGO','solutions_collec')]
 observationDevCollec = dbdev[config.get('MONGO','observations_collec')]
 entityTypeDevCollec = dbdev[config.get('MONGO','entity_type_collec')]
 questionsDevCollec = dbdev[config.get('MONGO','questions_collec')]
 criteriaDevCollec = dbdev[config.get('MONGO','criteria_collec')]
 entitiesDevCollec = dbdev[config.get('MONGO','entities_collec')]
except Exception as e:
  errorLogger.error(e,exc_info=True)

try :
 def convert(lst): 
      return ','.join(lst)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def evidence_extraction(msg_id):
  for obSub in observationSubmissionsDevCollec.find({'_id':ObjectId(msg_id)}):
    successLogger.debug("Observation Evidence Submission Id : " + str(msg_id))
    try:
      completedDate = str(datetime.datetime.date(obSub['completedDate'])) + 'T' \
                      + str(datetime.datetime.time(obSub['completedDate'])) + 'Z'
    except KeyError:
      pass
    evidence_sub_count = 0
    try:
     answersArr = [ v for v in obSub['answers'].values()]
    except KeyError:
     pass
    for ans in answersArr:
        try:
            if len(ans['fileName']):
                evidence_sub_count   = evidence_sub_count + len(ans['fileName'])
        except KeyError:
           if len(ans['instanceFileName']):
              for instance in ans['instanceFileName']:
                 evidence_sub_count   = evidence_sub_count + len(instance)
    for answer in answersArr:
       observationSubQuestionsObj = {}
       observationSubQuestionsObj['completedDate'] = completedDate
       observationSubQuestionsObj['total_evidences'] = evidence_sub_count
       observationSubQuestionsObj['userName'] = obSub['evidencesStatus'][0]['submissions'][0]['submittedByName']
       observationSubQuestionsObj['userName'] = observationSubQuestionsObj['userName'].replace("null","")
       observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
       observationSubQuestionsObj['school'] = str(obSub['entityId'])
       observationSubQuestionsObj['schoolExternalId'] = obSub['entityExternalId']
       observationSubQuestionsObj['schoolName'] = obSub['entityInformation']['name']
       observationSubQuestionsObj['entityTypeId'] = str(obSub['entityTypeId'])
       observationSubQuestionsObj['createdBy'] = obSub['createdBy']
       observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
       observationSubQuestionsObj['solutionId'] = str(obSub['solutionId'])
       observationSubQuestionsObj['observationId'] = str(obSub['observationId'])
       try :
        observationSubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
       except KeyError :
        observationSubQuestionsObj['appName'] = config.get("COMMON","diksha_survey_app_name")
       fileName = []
       fileSourcePath = []
       try:
         observationSubQuestionsObj['remarks'] = answer['remarks']
         observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]
       except KeyError:
         pass
       observationSubQuestionsObj['questionId'] = str(answer['qid'])
       for ques in questionsDevCollec.find({'_id':ObjectId(observationSubQuestionsObj['questionId'])}):
          observationSubQuestionsObj['questionExternalId'] = ques['externalId']
       observationSubQuestionsObj['questionResponseType'] = answer['responseType']
       evidence = []
       evidenceCount = 0
       try:
         if answer['fileName']:
            evidence = answer['fileName']
            observationSubQuestionsObj['evidence_count'] = len(evidence)
            evidenceCount = len(evidence)
       except KeyError:
         if answer['instanceFileName']:
            for inst in answer['instanceFileName'] :
                evidence.extend(inst)
            observationSubQuestionsObj['evidence_count'] = len(evidence)
            evidenceCount = len(evidence)
       for evi in evidence:
           fileName.append(evi['name'])
           fileSourcePath.append(evi['sourcePath'])
       observationSubQuestionsObj['fileName'] = convert(fileName)
       observationSubQuestionsObj['fileSourcePath'] = convert(fileSourcePath)
       if evidenceCount > 0:
          producer.send((config.get("KAFKA","kafka_evidence_druid_topic")), json.dumps(observationSubQuestionsObj)
                        .encode('utf-8'))
          producer.flush()
          successLogger.debug("Send Obj to Kafka")
except Exception as e:
  errorLogger.error(e,exc_info=True)


try:
 @app.agent(rawTopicName)
 async def observationEvidenceFaust(consumer) :
  async for msg in consumer :
      msg_val = msg.decode('utf-8')
      msg_data = json.loads(msg_val)
      successLogger.debug("========== START OF OBSERVATION EVIDENCE SUBMISSION ========")
      obj_arr = evidence_extraction(msg_data['_id'])
      successLogger.debug("********* END OF OBSERVATION EVIDENCE SUBMISSION ***********")
except Exception as e:
  errorLogger.error(e,exc_info=True)

if __name__ == '__main__':
   app.main()
