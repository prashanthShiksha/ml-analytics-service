# Release Note 5.1.0 ML Analytics Service

The survey consumption report on Admin Dashboard is pointing to
ml-survey-status therefore it showing incorrect values.Hence we are
created new script pyspark_sur_distinct_count_status.py it will create
new aggregated data source called ml-survey-distinctCount-status
.

## Deploy ml-analytics-service
To retrieve the latest release tag for version 5.1.0, please visit the following URL: https://github.com/Sunbird-Ed/ml-analytics-service/tags e.g. release-5.1.0_RC19

To proceed with the deployment process, follow the steps below:

    1. Log in to JenkinsI .
    2. In ml-analytics-service we dont have build, we have deployment only.
    3. go to Dashboard -> Deploy -> staging -> managed-learn -> ml-analytics-service. OR for dev go to Dashboard -> Deploy -> dev -> managed-learn -> ml-analytics-service .
    6. Click on "Build with parameters" and provide the latest release tag in the field labeled "ml_analytics_version" and release branch in the "branch_or_tag".Initiate the deployment process
    7. Once the job is completed, the services will be deployed on the respective environment

### config changes
Add new templates in config.j2 please refer this : https://github.com/project-sunbird/sunbird-devops/blob/release-5.1.0/ansible/roles/ml-analytics-service/templates/config.j2

```html
[DRUID] 
ml_distinctCnt_survey_status_spec :{{ml_analytics_distinctCnt_survey_status_batch_ingestion_spec}} 

[OUTPUT_DIR]
survey_distinctCount_status = {{ml_analytics_survey_distinctCount_status_filepath}}

[COMMON]
survey_distinctCount_blob_path = {{ ml_analytics_survey_distinctCount_blob_path }}

[LOGS]
survey_streaming_success_error = {{ ml_analytics_survey_streaming_success_log_folder_path }}
```
Add configs in main.yml please refer this : https://github.com/project-sunbird/sunbird-devops/blob/release-5.1.0/ansible/roles/ml-analytics-service/defaults/main.yml

1.ml_analytics_distinctCnt_survey_status_batch_ingestion_spec :
```html
{"type":"index","spec":{"ioConfig":{"type":"index","inputSource":{"type":"local","baseDir":["local json file storage path"],"filter":"ml_survey_distinctCount_status.json"},"inputFormat":{"type":"json"}},"tuningConfig":{"type":"index","partitionsSpec":{"type":"dynamic"}},"dataSchema":{"dataSource":"ml-surveydistinctCount-status","granularitySpec":{"type":"uniform","queryGranularity":"none","rollup":false,"segmentGranularity":"DAY"},"timestampSpec":{"column":"time_stamp","format":"auto"},"dimensionsSpec":{"dimensions":[{"type":"string","name":"program_name"},{"type":"string","name":"program_id"},{"type":"string","name":"survey_name"},{"type":"string","name":"survey_id"},{"type":"string","name":"submission_status"},{"type":"string","name":"state_name"},{"type":"string","name":"state_externalId"},{"type":"string","name":"district_name"},{"type":"string","name":"district_externalId"},{"type":"string","name":"block_name"},{"type":"string","name":"block_externalId"},{"type":"string","name":"organisation_name"},{"type":"string","name":"organisation_id"},{"type":"string","name":"private_program"},{"type":"string","name":"parent_channel"},{"type":"long","name":"unique_users"},{"type":"long","name":"unique_submissions"},{"type":"string","name":"time_stamp"}]},"metricsSpec":[]}}}
```
Note : change the path (spec.inConfig.inputSource.baseDir : "local json file storage path")  

2.ml_analytics_distinctCnt_survey_status_batch_ingestion_spec :"local json file storage path"

3.ml_analytics_survey_distinctCount_blob_path : "cloud json file storage path"

4.ml_analytics_survey_streaming_success_log_folder_path : "logs storage path"

### Backend Json
Updating backend json using this api /api/data/v1/report/jobs/ 
1\.[ml no of surveys in started status currently sl.json](https://github.com/shikshalokam/ml-analytics-service/blob/release-6.0.0/migrations/releases/6.0.0/config/backend/create/ml_no_of_surveys_in_started_status_currently_sl.json)

2\.[ml no of surveys submitted till date sl.json](https://github.com/shikshalokam/ml-analytics-service/blob/release-6.0.0/migrations/releases/6.0.0/config/backend/create/ml_no_of_surveys_submitted_till_date_sl.json)
