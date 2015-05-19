/*
*
* Created on Jan 14, 2015
*
* @author: Alejandro Sabater
*/

/**
 * Registering JARs needed for the UDFs and for the mongo driver
 */
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-pig-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-core-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-2.10.1.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/serviceStructure-1.0-SNAPSHOT.jar'

/**
 * Setting pig optimization parameters:
 *		pig.cachedbag.memusage --> Sets the percentage of memory to be used for bags (remember that bags are always treated in memory, if the bag is to big you can get errors)
 *		
 */
set pig.cachedbag.memusage 0.7
set pig.udf.profile true
set pig.exec.mapPartAgg true

/**
 * Establishing Aliases for UDFs Classes	
 */
DEFINE BSONLoader com.mongodb.hadoop.pig.BSONLoader();
DEFINE GetKeys com.mobiquitynetworks.servicestructure.GetKeys();
DEFINE createCombs com.mobiquitynetworks.servicestructure.createCombinations();

/** 
 * Parameters - set default values here; you can override with -p on the command-line.
 * Or user -f to specify a file with the parameters's values.
 */
%DEFAULT DIMENSION_PATH 's3://mbeacon-hadoop-development/data/production/hadoop.bson'
%DEFAULT OUTPUT_PATH 's3://mbeacon-hadoop-development/data/production/combinations'

/**
 * Loading hadoop collection
 */
keys_types = LOAD '$DIMENSION_PATH' 
			 USING BSONLoader();

/**
 * Validate presence of dimensions
 * input --> ([_id#5475bf25313569af760e4d41,dimension#{type=id, real=venue.id, name=Venue Id}])
 */
keys_types = FILTER  keys_types 
			 BY $0#'dimension' is not null;

/**
 * Eliminating ObjectId from map
 * input --> ([_id#5475bf25313569af760e4d41,dimension#{type=id, real=venue.id, name=Venue Id}])
 */
keys_types = FOREACH keys_types 
			 GENERATE $0#'dimension' as dimension;

/**
 * Turning dataset into flat structure
 * input --> ([type#id,real#venue.id,name#Venue Id])
 */
keys_types = FOREACH (GROUP keys_types ALL) 
			 GENERATE keys_types;


/**
 * Getting grouped field
 * input --> ({([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.location.state,name#Venue State]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#app,name#AppKey]),([type#id,real#clientId,name#Customer]),([type#string,real#venue.name,name#Venue Name])})
 */

keys = FOREACH keys_types 
	   GENERATE GetKeys($0);

/**
 * Getting dimension's names
 * input --> (venue.id,venue.location.state,event.value.notification,app,clientId,venue.name)
 */
keys_combs = FOREACH keys
            GENERATE createCombs($0);





/**
 * Storing combinarions into S3
 * input --> (venue.id|venue.location.state|event.value.notification|app|clientId|venue.name,*|venue.location.state|event.value.notification|app|clientId|venue.name,
 			  venue.id|*|event.value.notification|app|clientId|venue.name,venue.id|venue.location.state|*|app|clientId|venue.name,
 			  venue.id|venue.location.state|event.value.notification|*|clientId|venue.name,venue.id|venue.location.state|event.value.notification|app|*|venue.name,
 			  venue.id|venue.location.state|event.value.notification|app|clientId|*,*|*|event.value.notification|app|clientId|venue.name,*|venue.location.state|*|app|clientId|venue.name,
 			  *|venue.location.state|event.value.notification|*|clientId|venue.name,*|venue.location.state|event.value.notification|app|*|venue.name,*|venue.location.state|event.value.notification|app|clientId|*,
 			  venue.id|*|*|app|clientId|venue.name,venue.id|*|event.value.notification|*|clientId|venue.name,venue.id|*|event.value.notification|app|*|venue.name,venue.id|*|event.value.notification|app|clientId|*,
 			  venue.id|venue.location.state|*|*|clientId|venue.name,venue.id|venue.location.state|*|app|*|venue.name,venue.id|venue.location.state|*|app|clientId|*,venue.id|venue.location.state|event.value.notification|*|*|venue.name,
 			  venue.id|venue.location.state|event.value.notification|*|clientId|*,venue.id|venue.location.state|event.value.notification|app|*|*,*|*|*|app|clientId|venue.name,*|*|event.value.notification|*|clientId|venue.name,
 			  *|*|event.value.notification|app|*|venue.name,*|*|event.value.notification|app|clientId|*,*|venue.location.state|*|*|clientId|venue.name,*|venue.location.state|*|app|*|venue.name,*|venue.location.state|*|app|clientId|*,
 			  *|venue.location.state|event.value.notification|*|*|venue.name,*|venue.location.state|event.value.notification|*|clientId|*,*|venue.location.state|event.value.notification|app|*|*,venue.id|*|*|*|clientId|venue.name,
 			  venue.id|*|*|app|*|venue.name,venue.id|*|*|app|clientId|*,venue.id|*|event.value.notification|*|*|venue.name,venue.id|*|event.value.notification|*|clientId|*,venue.id|*|event.value.notification|app|*|*,
 			  venue.id|venue.location.state|*|*|*|venue.name,venue.id|venue.location.state|*|*|clientId|*,venue.id|venue.location.state|*|app|*|*,venue.id|venue.location.state|event.value.notification|*|*|*,*|*|*|*|clientId|venue.name,
 			  *|*|*|app|*|venue.name,*|*|*|app|clientId|*,*|*|event.value.notification|*|*|venue.name,*|*|event.value.notification|*|clientId|*,*|*|event.value.notification|app|*|*,*|venue.location.state|*|*|*|venue.name,
 			  *|venue.location.state|*|*|clientId|*,*|venue.location.state|*|app|*|*,*|venue.location.state|event.value.notification|*|*|*,venue.id|*|*|*|*|venue.name,venue.id|*|*|*|clientId|*,venue.id|*|*|app|*|*,
 			  venue.id|*|event.value.notification|*|*|*,venue.id|venue.location.state|*|*|*|*,*|*|*|*|*|venue.name,*|*|*|*|clientId|*,*|*|*|app|*|*,*|*|event.value.notification|*|*|*,*|venue.location.state|*|*|*|*,
 			  venue.id|*|*|*|*|*,*|*|*|*|*|*)
 */

STORE keys_combs INTO '$OUTPUT_PATH' USING PigStorage();

