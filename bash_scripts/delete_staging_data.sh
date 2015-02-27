aws s3 rm s3://mbeacon-hadoop-development/data/staging/statistics-staging --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/staging/combinations --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/staging/venues-region --recursive
aws s3 rm s3://mbeacon-hadoop-development/data/staging/events.bson
aws s3 rm s3://mbeacon-hadoop-development/data/staging/venues.bson
aws s3 rm s3://mbeacon-hadoop-development/data/staging/regions.bson
aws s3 rm s3://mbeacon-hadoop-development/data/staging/hadoop.bson
aws s3 rm s3://mbeacon-hadoop-development/data/staging/.events.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/staging/.venues.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/staging/.regions.bson.splits
aws s3 rm s3://mbeacon-hadoop-development/data/staging/.hadoop.bson.splits
rm -rf /home/ec2-user/restore/staging/*

