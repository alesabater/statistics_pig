# AWS Authentication
AWS_CONFIG_FILE="/home/ec2-user/.aws/config"

# Production data upload
00 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c events -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
03 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c venues -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
04 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c regions -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
05 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c hadoop -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/

# Staging data upload
06 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c events -w staging-mongodb-01.awsservers.mobiquitynetworks.com -k data/staging/
09 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c venues -w staging-mongodb-01.awsservers.mobiquitynetworks.com -k data/staging/
10 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c regions -w staging-mongodb-01.awsservers.mobiquitynetworks.com -k data/staging/
11 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c hadoop -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/staging/

# Development data upload
12 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c events -w dev-mongodb-01.awsservers.mobiquitynetworks.com -k data/development/
14 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c venues -w dev-mongodb-01.awsservers.mobiquitynetworks.com -k data/development/
15 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c regions -w dev-mongodb-01.awsservers.mobiquitynetworks.com -k data/development/
16 8 * * * source /home/ec2-user/bash_scripts/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c hadoop -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/development/

# EMR Cluster creation and steps provisioner
10 8 * * * aws emr create-cluster --name "statistics_pig_jobs" --enable-debugging --log-uri s3://mbeacon-hadoop-development/hadoop-logs --tags environment="production" --log-uri s3://mbeacon-hadoop-development/hadoop-logs/ --ami-version 3.3.1 --use-default-roles --applications Name=PIG --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m1.medium InstanceGroupType=CORE,InstanceCount=5,InstanceType=m3.xlarge --ec2-attributes KeyName=Development --steps Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=COMBINATIONS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/dimensions_production.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=VENUES_REGIONS_JOIN,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/venues_regions_relation_production.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=STATISTICS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/stats_sprint2_production.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=COMBINATIONS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/dimensions_staging.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=VENUES_REGIONS_JOIN,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/venues_regions_relation_staging.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=STATISTICS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/stats_sprint2_staging.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=COMBINATIONS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/dimensions_development.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=VENUES_REGIONS_JOIN,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/venues_regions_relation_development.pig] Type=Pig,ActionOnFailure=TERMINATE_CLUSTER,Name=STATISTICS,Args=[-f,s3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/pigscripts/stats_sprint2_development.pig] --auto-terminate

# Copy data from S3 to local
55 8 * * * aws s3 cp s3://mbeacon-hadoop-development/data/production/statistics-production /home/ec2-user/restore/production/ --recursive
30 9 * * * aws s3 cp s3://mbeacon-hadoop-development/data/staging/statistics-staging /home/ec2-user/restore/staging/ --recursive
50 9 * * * aws s3 cp s3://mbeacon-hadoop-development/data/development/statistics-development /home/ec2-user/restore/development/ --recursive

# Mongo scripts to delete existing collection and create new ones
58 8 * * * mongo mongo_scripts/mongoscript_production.js
33 9 * * * mongo mongo_scripts/mongoscript_staging.js
53 9 * * * mongo mongo_scripts/mongoscript_development.js

# Restore data to Mongo Collection
06 9 * * * source /home/ec2-user/bash_scripts/mongo_restore_production.sh
44 9 * * * source /home/ec2-user/bash_scripts/mongo_restore_staging.sh
55 9 * * * source /home/ec2-user/bash_scripts/mongo_restore_development.sh

# Delete data from local
23 10 * * * source /home/ec2-user/bash_scripts/delete_production_data.sh
01 15 * * * source /home/ec2-user/bash_scripts/delete_staging_data.sh
02 15 * * * source /home/ec2-user/bash_scripts/delete_development_data.sh
