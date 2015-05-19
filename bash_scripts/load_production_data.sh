source /home/ec2-user/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c events -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
source /home/ec2-user/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c venues -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
source /home/ec2-user/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c regions -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/
source /home/ec2-user/backup_collection_s3.sh -b mbeacon-hadoop-development -d backend -c hadoop -w prod-mongodb-01-a.awsservers.mobiquitynetworks.com -k data/production/

