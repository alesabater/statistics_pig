REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-pig-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-core-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-2.10.1.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/serviceStructure-1.0-SNAPSHOT.jar'

set pig.cachedbag.memusage 0.7
set pig.udf.profile true
set pig.exec.mapPartAgg true

DEFINE BSONLoader com.mongodb.hadoop.pig.BSONLoader();
DEFINE GetKeys com.mobiquitynetworks.servicestructure.GetKeys();
DEFINE createCombs com.mobiquitynetworks.servicestructure.createCombinations();

%DEFAULT DIMENSION_PATH 's3://mbeacon-hadoop-development/data/production/hadoop.bson'
--%DEFAULT OUTPUT_PATH '/tmp/combinations'
%DEFAULT OUTPUT_PATH 's3://mbeacon-hadoop-development/data/production/combinations'

keys_types = LOAD '$DIMENSION_PATH' USING BSONLoader();

keys_types = FILTER  keys_types BY $0#'dimension' is not null;

keys_types = FOREACH keys_types GENERATE $0#'dimension' as dimension;

keys_types = FOREACH (GROUP keys_types ALL) GENERATE keys_types;

keys = FOREACH keys_types GENERATE GetKeys($0);

keys_combs = FOREACH keys
            GENERATE createCombs($0);

STORE keys_combs INTO '$OUTPUT_PATH' USING PigStorage();

