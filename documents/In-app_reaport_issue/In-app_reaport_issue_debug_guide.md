# Issue - In-app Reports Not Coming Up

## Step 1: Check Druid Real-time Data Sources

- Verify if Druid real-time data sources have the latest data.
- You can check using Druid unified-console or in SuperSet.
- Example for Staging:
  - Druid Unified Console: [Staging - http://11.3.2.25:8888/unified-console.html](http://11.3.2.25:8888/unified-console.html)

## Step 2: Check .druid Kafka Topic

- Confirm if the .druid Kafka topic has the latest data.
- You can get the latest data from Jenkins.
- Job Link in Staging: [http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/GetEventsFromKafka/build?delay=0sec](http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/GetEventsFromKafka/build?delay=0sec)

## Step 3: Check Scripts and Logs

- Ensure that the scripts are running 24/7.
- Check if any scripts are throwing errors on the server.
- Real-time streaming scripts to check:
  - `sudo systemctl status faust_observation.service`
  - `sudo systemctl status faust_observation_evidence.service`
  - `sudo systemctl status faust_survey.service`
  - `sudo systemctl status faust_survey_evidence.service`
- To view logs of the scripts:
  - `tail -f /var/log/sys.log`
- To see the latest logs while checking logs, data should be submitted in parallel.

## Step 4: Check .raw Kafka Topic

- Confirm if the .raw Kafka topic has the latest data, which comes from the backend.
- Job Link in Staging: [http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/GetEventsFromKafka/build?delay=0sec](http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/GetEventsFromKafka/build?delay=0sec)

## Note

- Check that all IP addresses are correct in `config.ini` and in all Druid specs.

## How to Change or Edit the Spec in Druid

Here are two ways to change the spec in Druid:

### 1. Manually Updating Druid

1. Login to Druid unified-console with access to port 8888.
2. Select the "Ingestion" tab in the UI.
3. In "Supervisors," select the data sources that require updates and select "Settings" there.
4. Stop the supervisor by clicking on "Suspend."
5. After the status changes to suspended, reset the data source in settings by clicking the "Hard Reset" button.
6. After submitting the ingestion spec in the data loader, update and submit the spec.

### 2. Through Running Jenkins Job

1. Login to Druid unified-console with access to port 8888.
2. Select the "Ingestion" tab in the UI.
3. In "Supervisors," select the data sources that are required and select "Settings" there.
4. Stop the supervisor by clicking on "Suspend."
5. After the status changes to suspended, reset the data source in settings by clicking the "Hard Reset" button.
6. Go to Jenkins UI and navigate to the "DruidIngestion" job.
7. Select one or multiple ingestion_task_names and build.

Jenkins Job Link in Staging: [http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/DruidIngestion/](http://10.20.0.14:8080/jenkins/job/Deploy/job/staging/job/DataPipeline/job/DruidIngestion/)

If you want to add more specs, you can do so [here](https://github.com/Sunbird-Obsrv/sunbird-data-pipeline/tree/release-5.2.0/ansible/roles/druid-ingestion/templates).
