/*
*
* Created on Jan 14, 2015
*
* @author: Alejandro Sabater
*/


/** 
 * Parameters - set default values here; you can override with -p on the command-line.
 * Or user -f to specify a file with the parameters's values.
 */

REGISTER 's3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/jars/mongo-hadoop-pig-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/jars/mongo-hadoop-core-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/jars/mongo-2.10.1.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/jars/StatsUtilsPig-1.0-SNAPSHOT.jar'

 
DEFINE BSONLoader com.mongodb.hadoop.pig.BSONLoader();
DEFINE BSONStorage com.mongodb.hadoop.pig.BSONStorage();
DEFINE MongoStorage com.mongodb.hadoop.pig.MongoStorage();
DEFINE MongoLoader com.mongodb.hadoop.pig.MongoLoader();
DEFINE keysCombinations com.mobiquitynetworks.statsutilspig.keysCombinations();
DEFINE JsonStructure com.mobiquitynetworks.statsutilspig.JsonStructure();

%DEFAULT EVENT_PATH 's3://mbeacon-hadoop-development/data/development/events.bson'
--%DEFAULT EVENT_PATH 'mongodb://prod-mongodb-01-a.awsservers.mobiquitynetworks.com:27017/backend.events'
%DEFAULT REGION_VENUE_PATH 's3://mbeacon-hadoop-development/data/development/venues-region/part*.bson'
--%DEFAULT COMBINATIONS_PATH '/tmp/combinations/part-r-*'
%DEFAULT COMBINATIONS_PATH 's3://mbeacon-hadoop-development/data/development/combinations/part-r-*'
%DEFAULT OUTPUT_PATH 's3://mbeacon-hadoop-development/statistics_interface/stat-hour.bson'
%DEFAULT DIMENSIONS_PATH 's3://mbeacon-hadoop-development/data/development/hadoop.bson'

IMPORT 's3://mbeacon-hadoop-development/pig/statistics_2nd_sprint/macros/stats_sprint2_macro.pig';

--set mongo.job.input.format 'com.mongodb.hadoop.BSONFileInputFormat'
--set pig.cachedbag.memusage 0.7
--set pig.udf.profile true
--set pig.exec.mapPartAgg true
set mongo.job.output.format 'com.mongodb.hadoop.BSONFileOutputFormat'
set mapreduce.output.fileoutputformat.outputdir 's3://mbeacon-hadoop-development/data/development/statistics-development'


events = LOAD '$EVENT_PATH' USING BSONLoader();
-- --events = LOAD '$EVENT_PATH' USING MongoLoader();

-- --STORE events INTO 's3://mbeacon-hadoop-development/nuevo' USING PigStorage();

--venues = LOAD '$REGION_VENUE_PATH' USING PigStorage() as venue:map[];
venues = LOAD '$REGION_VENUE_PATH' USING BSONLoader();
combinations = LOAD '$COMBINATIONS_PATH' USING TextLoader() as combinations:chararray;
dimensions = LOAD '$DIMENSIONS_PATH' USING BSONLoader();

dimensions = FOREACH dimensions GENERATE $0#'dimension';

dimensions = FOREACH (GROUP dimensions ALL) GENERATE dimensions;

events = FILTER events BY $0#'event'#'localtime' is not null and 
						  ($0#'event'#'extType'=='NOT' or $0#'event'#'extType'=='BCN') and 
						  ($0#'event'#'action'=='CLK' or $0#'event'#'action'=='DSP' or $0#'event'#'action'=='ENT');

events = FOREACH events GENERATE TOMAP(
									   'app',$0#'app',
									   'event.timestamp',(datetime)$0#'event'#'timestamp',
									   'event.localtime',(datetime)$0#'event'#'localtime',
									   'event.value.beacon',(chararray)$0#'event'#'value'#'beacon',
									   'event.value.notification',(chararray)$0#'event'#'value'#'notification',
									   'clientId',$0#'clientId',
									   'device.idDev',$0#'device'#'idDev',
									   'device.idFA',$0#'device'#'idFA',
									   'device.os',$0#'device'#'os',
									   'event.type',$0#'event'#'type',
									   'event.extType',$0#'event'#'extType',
									   'event.action',$0#'event'#'action');

events = JOIN events 
			BY (chararray)$0#'event.value.beacon',
				venues
			BY (chararray)$0#'beaconId';

events = FOREACH events 
		 GENERATE FLATTEN(keysCombinations($0,$1,combinations.$0)),
		 		  $0#'event.action' as action:chararray;

events = GENERATE_DATE_PARTS(events);

SPLIT events INTO events_bcn_ent IF action == 'ENT',
	  			  events_not IF (action == 'CLK' or action == 'DSP');

events_not = FOREACH events_not
			 GENERATE key as key:chararray,
			 		  year,
			 		  month,
			 		  day,
			 		  hour,
			 		  (action == 'CLK'?1:0) as clk:int,
			 		  (action == 'DSP'?1:0) as dsp:int,
			 		  0 as ent:int;

events_bcn_ent = FOREACH (GROUP events_bcn_ent BY (idFA,idDev,key,year,month,day)) 
				 GENERATE group.key as key:chararray,
				 		  group.year as year:int,
				 		  group.month as month:int,
				 		  group.day as day:int,
				 		  MIN(events_bcn_ent.hour) as hour:int,
				 		  0 as clk:int,
				 		  0 as dsp:int,
				 		  1 as ent:int;

events = UNION events_not,
			   events_bcn_ent;			     

events_hour = GROUP events BY (key,year,month,day,hour);
events_hour_count = FOREACH events_hour GENERATE group, 
												 SUM(events.ent) as hour_count_ent:int, 
												 SUM(events.clk) as hour_count_clk:int, 
												 SUM(events.dsp) as hour_count_dsp:int;

events_day = GROUP events_hour_count BY (group.key,group.year,group.month,group.day);
events_day_count = FOREACH events_day GENERATE group, 
											   SUM(events_hour_count.hour_count_ent) as day_count_ent:int, 
											   SUM(events_hour_count.hour_count_clk) as day_count_clk:int, 
											   SUM(events_hour_count.hour_count_dsp) as day_count_dsp:int;

events_month = GROUP events_day_count BY (group.key,group.year,group.month);
events_month_count = FOREACH events_month GENERATE group, 
												   SUM(events_day_count.day_count_ent) as month_count_ent:int, 
												   SUM(events_day_count.day_count_clk) as month_count_clk:int, 
												   SUM(events_day_count.day_count_dsp) as month_count_dsp:int;

events_year = GROUP events_month_count BY (group.key,group.year);
events_year_count = FOREACH events_year GENERATE group, 
												 SUM(events_month_count.month_count_ent) as year_count_ent:int, 
												 SUM(events_month_count.month_count_clk) as year_count_clk:int, 
												 SUM(events_month_count.month_count_dsp) as year_count_dsp:int;


final_events = UNION events_hour_count, events_day_count, events_month_count, events_year_count;

final_events = FOREACH final_events GENERATE JsonStructure(dimensions.$0, $0..);

STORE final_events INTO '$OUTPUT_PATH' USING BSONStorage();

/*

hour = FOREACH events_hour_count GENERATE JsonStructure(dimensions.$0, $0..);
day = FOREACH events_day_count GENERATE JsonStructure(dimensions.$0, $0..);
month = FOREACH events_month_count GENERATE JsonStructure(dimensions.$0, $0..);
year = FOREACH events_year_count GENERATE JsonStructure(dimensions.$0, $0..);

STORE hour INTO '$OUTPUT_PATH_HOUR' USING BSONStorage();

STORE day INTO '$OUTPUT_PATH_DAY' USING BSONStorage();

STORE month INTO '$OUTPUT_PATH_MONTH' USING BSONStorage();

STORE year INTO '$OUTPUT_PATH_YEAR' USING BSONStorage();

*/
-- final_events = UNION events_hour_structured,
-- 					 events_day_structured,
-- 					 events_month_structured,
-- 					 events_year_structured;



-- STORE events_year_structured INTO 'mongodb://staging-mongodb-01.awsservers.mobiquitynetworks.com:27017/backend.statistics_test' USING MongoStorage();


--DUMP events_hour_structured;
--STORE events_hour_structured INTO '$OUTPUT_PATH' USING BSONStorage();

-- STORE events_hour_structured INTO '/tmp/pigStorage' USING PigStorage();
-- STORE events_day_structured INTO '/tmp/pigStorage1' USING PigStorage();
-- STORE events_month_structured INTO '/tmp/pigStorage2' USING PigStorage();
-- STORE final_events INTO 's3://mbeacon-hadoop-development/some' USING PigStorage();


-- events = LOAD 's3://mbeacon-hadoop-development/some/part-m*' USING PigStorage() as (t:(chararray),c1:long,c2:long,c3:long);

-- events = FOREACH events GENERATE JsonStructure(dimensions.$0, $0..);

-- STORE events INTO '$OUTPUT_PATH' USING BSONStorage();

--events_hour_structured = FOREACH events_hour_count GENERATE JsonStructure(dimensions.$0, $0..);