REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-pig-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-hadoop-core-1.4.0-SNAPSHOT.jar'
REGISTER 's3://mbeacon-hadoop-development/pig/statistics_1st_sprint/jars/mongo-2.10.1.jar'

DEFINE BSONLoader com.mongodb.hadoop.pig.BSONLoader();
DEFINE BSONStorage com.mongodb.hadoop.pig.BSONStorage();

%DEFAULT REGION_PATH 's3://mbeacon-hadoop-development/data/staging/regions.bson'
%DEFAULT VENUE_PATH 's3://mbeacon-hadoop-development/data/staging/venues.bson'
--%DEFAULT OUTPUT_PATH 's3n://mbeacon-hadoop-development/venues_regions_relation.bson'
--%DEFAULT OUTPUT_PATH 'venues_region_joined'
%DEFAULT OUTPUT_PATH 'venues-regions.bson'

set mongo.job.output.format 'com.mongodb.hadoop.BSONFileOutputFormat'
set mapreduce.output.fileoutputformat.outputdir 's3://mbeacon-hadoop-development/data/staging/venues-region'

/**
 * Load region instances.
 */
regions = LOAD '$REGION_PATH' 
		 USING BSONLoader();

/**
 * Projects '_id' and 'situation.venue' from regions
 * 'situation.venues' contains venue.id, venue.type, venue.coordinates, venue.name
 * The possible outputs of region_venues are (No venue associated, or venue associated):
 *
 * (540057657b2e4e577445b770,)
 * (5409ac277b2e4efd15f4ddc8,[_id#54041e9f7b2e4efb15f4d120,type#mall,coordinates#{({(33.8331),(-118.3481)})},name#Plaza Carolina])
 */

regions_venues = FOREACH regions 
					GENERATE $0#'_id' as beaconId,
							 $0#'situation'#'venue' as venue:map[];
/**
 * Load venues instances
 */
venues = LOAD '$VENUE_PATH' 
		USING BSONLoader();
/**
 * Projects '_id', 'country', 'state', 'city', 'zip' from venues
 * If one of the venues doesn't have one of these fields, they get returned as ''.
 * The output of venues_location:
 *
 * (5437fde67b2e4e5574be0484,us,NY,Central Valley,10917)
 * (540418887b2e4efc15f4d10c,us,,Atlanta,30345-2743)
 */
venues_location = FOREACH venues
				 GENERATE $0#'_id' as id:chararray,
				 		  $0#'country' as country:chararray,
				 		  $0#'state' as state:chararray,
				 		  --$0#'city' as city:chararray,
				 		  --$0#'zip' as zip:chararray,
				 		  $0#'metrics'#'dma'#'rank' as dma:int;
/**
 * Performs a left join operation over regions_venues(left) and venues_location.
 * The output of region venue can be a region with no venue or with venue as:
 * 
 * (539850be6375f9c787ff06e6,,,,,,)
 * (53ce3bb50af550cd2222a829,[type#mall,_id#54041d487b2e4efc15f4d11c,coordinates#{({(33.8331),(-118.3481)})},name#Roosevelt Field],54041d487b2e4efc15f4d11c,us,,Garden City,11530-3467)
 */
regions_join_venues = JOIN regions_venues
						BY venue#'_id',
						   venues_location
						BY id;


/**
 * Filtering regions_join_venues by desired values
 * delete redundant fields, and create a map structure
 * The output will be the following structure:
 *
 * ([region#{_id=53e07cd20af550b032aef6f5},type#,_id#,location#{state=null, zip=null, country=null, city=null},name#])
 */
venues_beacon_id = FOREACH regions_join_venues
				  GENERATE TOMAP('venue.name', $1#'name', -- venue name
				  		   'venue.type', $1#'type', -- venue type
				  		   'venue.id', id, -- venue id
				  		   'venue.metrics.dma', dma,
				  		   'beaconId', beaconId,
				  		   'venue.country', country,
				  		   'venue.location.state',state);
				  		   --'city', city,
				  		   --'zip', zip)) as venue;

STORE venues_beacon_id INTO '$OUTPUT_PATH' USING BSONStorage();
