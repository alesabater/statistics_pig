aws s3 rm s3://mbeacon-hadoop-development/data/development/statistics-development --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/development/combinations --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/development/venues-region --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/development/events.bson
aws s3 rm s3://mbeacon-hadoop-development/data/development/venues.bson
aws s3 rm s3://mbeacon-hadoop-development/data/development/regions.bson
aws s3 rm s3://mbeacon-hadoop-development/data/development/hadoop.bson
aws s3 rm s3://mbeacon-hadoop-development/data/development/.events.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/development/.venues.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/development/.regions.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/development/.hadoop.bson.splits
rm -rf /home/ec2-user/restore/production/*

