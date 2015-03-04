/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import static org.bson.types.ObjectId.isValid;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.hadoop.io.WritableComparable;


/**
 *
 * @author asabater
 */

/*
RECEIVING INPUT
({([type#id,real#clientId,name#Customer]),([type#string,real#app,name#AppKey]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#venue.location.country,name#Venue Country]),([type#string,real#venue.location.state,name#Venue State]),([type#string,real#venue.location.city,name#Venue City]),([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.name,name#Venue Name]),([type#string,real#venue.type,name#Venue Type]),([type#string,real#device.os,name#Device OS]),([type#string,real#device.osVersion,name#Device osVersion])},
54491ba5689319a6170ea70b|a90a26bab9868d071c1e4325a9623df284efd384|*|us|*|Torrance|5404166a7b2e4e2017f4d106|Del Amo Fashion Center|mall|Android|4.4.2,
uniqueVisitors,
1,
2014-10-21T12:00:00.000Z,
2014-10-21T12:59:59.999Z,
1h)
({([type#id,real#clientId,name#Customer]),([type#string,real#app,name#AppKey]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#venue.location.country,name#Venue Country]),([type#string,real#venue.location.state,name#Venue State]),([type#string,real#venue.location.city,name#Venue City]),([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.name,name#Venue Name]),([type#string,real#venue.type,name#Venue Type]),([type#string,real#device.os,name#Device OS]),([type#string,real#device.osVersion,name#Device osVersion])},54491ba5689319a6170ea70b|a90a26bab9868d071c1e4325a9623df284efd384|*|us|*|Torrance|5404166a7b2e4e2017f4d106|Del Amo Fashion Center|mall|Android|4.4.2,uniqueVisitors,1,2014-10-21T12:18:00.000Z,2014-10-21T12:18:59.999Z,1m)
({([type#id,real#clientId,name#Customer]),([type#string,real#app,name#AppKey]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#venue.location.country,name#Venue Country]),([type#string,real#venue.location.state,name#Venue State]),([type#string,real#venue.location.city,name#Venue City]),([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.name,name#Venue Name]),([type#string,real#venue.type,name#Venue Type]),([type#string,real#device.os,name#Device OS]),([type#string,real#device.osVersion,name#Device osVersion])},54491ba5689319a6170ea70b|a90a26bab9868d071c1e4325a9623df284efd384|*|us|*|Torrance|5404166a7b2e4e2017f4d106|Del Amo Fashion Center|mall|Android|4.4.2,uniqueVisitors,1,2014-01-01T00:00:00.000Z,2014-12-31T23:59:59.999Z,1y)
*/


/*
({([type#id,real#clientId,name#Customer]),([type#string,real#app,name#AppKey]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#event.extType,name#event extType]),([type#string,real#event.action,name#event action]),([type#string,real#venue.location.country,name#Venue Country]),([type#string,real#venue.location.state,name#Venue State]),([type#string,real#venue.location.city,name#Venue City]),([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.name,name#Venue Name]),([type#string,real#venue.type,name#Venue Type]),([type#string,real#device.os,name#Device OS]),([type#string,real#device.osVersion,name#Device osVersion])},545211982220993f406f4de4|a90a26bab9868d071c1e4325a9623df284efd384|null|GEO,1y,2014-01-01T00:00:00.000Z,2014-12-30T23:59:59.999Z,{((visits,1))})


*/

public class Statistics extends EvalFunc<Map>{
    
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    BagFactory mBagFactory = BagFactory.getInstance();  
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Map exec(Tuple tuple) throws IOException {

        //Date formatter
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        // Extrating input values
        List<Object> params = tuple.getAll();
        DataBag keysDictionary = (DataBag)params.get(0);
        String[] keyValues = params.get(1).toString().split("\\|");
        String symbol = (String)params.get(2);
        
        //Map<String, Object> timestamps = new HashMap<>();
        //Object startDate = params.get(3);
        //Object endDate = params.get(4);
        //timestamps = setTimestamp(timestamps, startDate, endDate, symbol);
        Date startDate = null;
        Date endDate = null;
        Map<String, Object> timestamps = new HashMap<>();
        try{
            startDate = df.parse(params.get(3).toString().replace("Z", "+0000"));
            endDate =  df.parse(params.get(4).toString().replace("Z", "+0000"));
            timestamps = setTimestamp(timestamps, startDate, endDate, symbol);
        }
        catch (Exception E){
            LOG.warn("THERE IS AN ERROR WITH THE DATE : "+E);
        }
        

        DataBag metrics = (DataBag)params.get(5);
        
        //LOG.warn(" ------------>>>>>>>>>>>  THESE ARE THE KEYS!!   : "+ keys + " <<<<<<<<<<<<<<<<<<<-------------------");
        
        Map<String, Object> statistic = new HashMap<>();
        Tuple kdTuple = mTupleFactory.newTuple();
        Tuple vmTuple = mTupleFactory.newTuple();

        Tuple iterTup;
        Map<String,Object> iterMap;
        int count = 0;
        
        
        //{([type#string,real#device.osVersion,name#Device osVersion]),([type#string,real#device.os,name#Device OS]),([type#id,real#venue.id,name#Venue Id]),([type#string,real#venue.location.state,name#Venue State]),([type#id,real#event.value.notification,name#Notification Id]),([type#string,real#app,name#AppKey]),([type#id,real#clientId,name#Customer])}
        //*|*|*|CA|*|*|*
        
        for(Object column : keysDictionary){
            
            iterTup = (Tuple)column;
            if(!keyValues[count].equals("*")){
                Map<String, Object> kdMap = new HashMap<>();
                iterMap = (Map)iterTup.get(0);
                if(iterMap.get("type").equals("id")){
                    Object obj = null;
                    if(isValid(keyValues[count])){
                        obj = new ObjectId(keyValues[count]);
                        //kdList = addMapToList(kdList, (String)iterMap.get("name"), obj, kdMap);
                    }
                    else{
                        obj = "-";
                    }
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), obj, kdMap);
                }
                else{
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), keyValues[count], kdMap);
                    //kdList = addMapToList(kdList, (String)iterMap.get("name"), keyValues[count], kdMap);
                }
                //kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), keyValues[count], kdMap);
            }
            count++;
        }
        //{((visits,1)),((uniqueVisitors,1)),((notificationsClicked,5)),((notificationsSent,36))}
        for(Object obj: metrics){
            Tuple metric = (Tuple)obj;
            metric = (Tuple)metric.get(0);
            Map<String, Object> vmMap = new HashMap<>();
            vmTuple = addMapToTuple(vmTuple, (String)metric.get(0), metric.get(1), vmMap);
        }
        
        //timestamps = setTimestamp(timestamps, startDate, endDate, symbol);
        statistic.put("kd", kdTuple);
        statistic.put("ts", timestamps);
        statistic.put("nd", new Long(kdTuple.size()));
        statistic.put("vm", vmTuple);
        
        return statistic;
        
        
//statistic.put(3, );
        
                
        /*
        Funciona
        statistic.put("0", startDate);
        statistic.put("1", endDate);
        statistic.put("2", keyValues[0]);
        statistic.put("3", statisticValue);
        return statistic;*/
        
        /*
        Map<String, Object> statistic = new HashMap<>();
        Map<String, Object> kdMap = new HashMap<>();
        Map<String, Object> vmMap = new HashMap<>();
        List<Map<String,Object>> l = new ArrayList<>();
        
        kdMap.put("k", "key");
        kdMap.put("v", "value");
        
        l.add(0, kdMap);
        
        kdMap.put("k", "key1");
        kdMap.put("v", "value1");
        
        l.add(1, kdMap);
        
        kdMap.put("k", "key2");
        kdMap.put("v", "value2");
        
        l.add(2, kdMap);
        
        statistic.put("kd", l);
        statistic.put("nd", 8);
        
        
        return statistic;*/
    }
    
    public List<Map<String,Object>> addMapToList(List<Map<String,Object>> list, String k, Object v, Map<String,Object> map){
        map.put("k", k);
        map.put("v", v);
        list.add(map);
        return list;
    }
    
    public Tuple addMapToTuple(Tuple tup, String k, Object v, Map<String,Object> map){
        map.put("k", k);
        map.put("v", v);
        tup.append(map);
        return tup;
    }
    
    public Map<String,Object> setTimestamp(Map<String,Object> map, Object start, Object end, String symbol){
        map.put("b", start);
        map.put("e", end);
        map.put("u", symbol);
        return map;
    }
    
    
}
