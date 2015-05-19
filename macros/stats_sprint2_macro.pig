/*
* Created on Oct 31, 2014
*
* @author: Alejandro Sabater
*/


/**
* Macro funciton to filter and count the results by a is null or is not null condition
*/ 

DEFINE FILTER_AND_COUNT(relation,condition) RETURNS filtered,summary {

	$filtered = filter $relation by $condition;
	dropped = filter $relation by NOT($condition);
	$summary = COMPARE_TOTALS($relation, $filtered, dropped);
};

/**
 * Get totals on 3 relations, union and return them with labels 
 */

DEFINE COMPARE_TOTALS(r1,r2,r3) RETURNS totals {
  total1 = TOTAL_COUNT($r1);
  total2 = TOTAL_COUNT($r2);
  total3 = TOTAL_COUNT($r3);
  $totals = union total1, total2, total3;
};

/**
 * Returns the name of a relation and the number of records
 */
DEFINE TOTAL_COUNT(relation) RETURNS total {
  $total = FOREACH (group $relation all) generate '$relation' as label, COUNT_STAR($relation) as total;
};

/**
 * Creates a ISO 8601 formate date 'yyyy-MM-dd HH:mm:ss.SSSZ' and gets the year, month, day, hour and minute
 * to add them to each register.
 */
DEFINE GENERATE_DATE_PARTS(r1) RETURNS r2 {

	$r2 = FOREACH $r1 {
						ts = $1;
						year = GetYear(ts);
						month = GetMonth(ts);
						day = GetDay(ts);
						hour = GetHour(ts);
						GENERATE $0.., year as year:int, month as month:int, day as day:int, hour as hour:int;
		};
};

/**
 * Gets a relation of events as argument and return 5 relations grouped by 
 * (year), (year, month), (year, month, day), (year, month, day, hour), (year, month, day, hour, minute) 
 */
 /*

DEFINE RELATIONS_BY_INTERVALS(r1) RETURNS year,month,day,hour {

	$year = GROUP $r1 
			  BY ($0#'date'#'year');

	$month = GROUP $r1 
			  BY ($0#'date'#'year',$0#'date'#'month');
			  
	$day = GROUP $r1 
			  BY ($0#'date'#'year',$0#'date'#'month',$0#'date'#'day');

	$hour = GROUP $r1 
			  BY ($0#'date'#'year',$0#'date'#'month',$0#'date'#'day',$0#'date'#'hour');
*/
	--$minute = GROUP $r1 
	--		  BY ($0#'date'#'year',$0#'date'#'month',$0#'date'#'day',$0#'date'#'hour',$0#'date'#'minute');

--};
