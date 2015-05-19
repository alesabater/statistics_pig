aws s3 rm s3://mbeacon-hadoop-development/data/production/statistics-production --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/production/combinations --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/production/venues-region --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/production/events.bson
aws s3 rm s3://mbeacon-hadoop-development/data/production/venues.bson
aws s3 rm s3://mbeacon-hadoop-development/data/production/regions.bson
aws s3 rm s3://mbeacon-hadoop-development/data/production/hadoop.bson
aws s3 rm s3://mbeacon-hadoop-development/data/production/.events.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/production/.venues.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/production/.regions.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/production/.hadoop.bson.splits
rm -rf /home/ec2-user/restore/production/*

