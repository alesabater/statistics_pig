/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author asabater
 */
public class keysCombinations extends EvalFunc<DataBag> {

    
    // Bag factory instace to create as many bags as desired
    BagFactory mBagFactory = BagFactory.getInstance();  
    // Tuple factory instace to create as many bags as desired
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final Log LOG = LogFactory.getLog(keysCombinations.class);    
    
    @Override
    /* 
     * Method that is executed when a UDF is called
     * Recieves a tuple of the following form:
     *            ([device.idDev#9B5C0249-4F61-4A8F-9591-994625197C6E,app#dbf158bc74de36e5a7ebec145d1524887fc9313b,event.value.beacon#539850be6375f9c787ff07ce,event.value.notification#,event.type#MOB,device.idFA#,event.localtime#2015-01-24T14:36:04.466+01:00,device.os#iOS,event.extType#BCN,event.timestamp#2015-01-24T19:36:04.466+01:00,event.action#ENT,clientId#53b19a6b0af550e07817962f],
                    [venue.type#mall,venue.country#us,beaconId#539850be6375f9c787ff07ce,venue.id#5476cfd1a20d45dd20e8dd0a,venue.location.state#NY,venue.name#Smith Haven Mall,venue.metrics.dma#1],
                    venue.id|venue.location.state|event.value.notification|app|clientId|venue.name,*|venue.location.state|event.value.notification|app|clientId|venue.name,venue.id|*|event.value.notification|app|clientId|venue.name,venue.id|venue.location.state|*|app|clientId|venue.name,venue.id|venue.location.state|event.value.notification|*|clientId|venue.name,venue.id|venue.location.state|event.value.notification|app|*|venue.name,venue.id|venue.location.state|event.value.notification|app|clientId|*,*|*|event.value.notification|app|clientId|venue.name,*|venue.location.state|*|app|clientId|venue.name,*|venue.location.state|event.value.notification|*|clientId|venue.name,*|venue.location.state|event.value.notification|app|*|venue.name,*|venue.location.state|event.value.notification|app|clientId|*,venue.id|*|*|app|clientId|venue.name,venue.id|*|event.value.notification|*|clientId|venue.name,venue.id|*|event.value.notification|app|*|venue.name,venue.id|*|event.value.notification|app|clientId|*,venue.id|venue.location.state|*|*|clientId|venue.name,venue.id|venue.location.state|*|app|*|venue.name,venue.id|venue.location.state|*|app|clientId|*,venue.id|venue.location.state|event.value.notification|*|*|venue.name,venue.id|venue.location.state|event.value.notification|*|clientId|*,venue.id|venue.location.state|event.value.notification|app|*|*,*|*|*|app|clientId|venue.name,*|*|event.value.notification|*|clientId|venue.name,*|*|event.value.notification|app|*|venue.name,*|*|event.value.notification|app|clientId|*,*|venue.location.state|*|*|clientId|venue.name,*|venue.location.state|*|app|*|venue.name,*|venue.location.state|*|app|clientId|*,*|venue.location.state|event.value.notification|*|*|venue.name,*|venue.location.state|event.value.notification|*|clientId|*,*|venue.location.state|event.value.notification|app|*|*,venue.id|*|*|*|clientId|venue.name,venue.id|*|*|app|*|venue.name,venue.id|*|*|app|clientId|*,venue.id|*|event.value.notification|*|*|venue.name,venue.id|*|event.value.notification|*|clientId|*,venue.id|*|event.value.notification|app|*|*,venue.id|venue.location.state|*|*|*|venue.name,venue.id|venue.location.state|*|*|clientId|*,venue.id|venue.location.state|*|app|*|*,venue.id|venue.location.state|event.value.notification|*|*|*,*|*|*|*|clientId|venue.name,*|*|*|app|*|venue.name,*|*|*|app|clientId|*,*|*|event.value.notification|*|*|venue.name,*|*|event.value.notification|*|clientId|*,*|*|event.value.notification|app|*|*,*|venue.location.state|*|*|*|venue.name,*|venue.location.state|*|*|clientId|*,*|venue.location.state|*|app|*|*,*|venue.location.state|event.value.notification|*|*|*,venue.id|*|*|*|*|venue.name,venue.id|*|*|*|clientId|*,venue.id|*|*|app|*|*,venue.id|*|event.value.notification|*|*|*,venue.id|venue.location.state|*|*|*|*,*|*|*|*|*|venue.name,*|*|*|*|clientId|*,*|*|*|app|*|*,*|*|event.value.notification|*|*|*,*|venue.location.state|*|*|*|*,venue.id|*|*|*|*|*,*|*|*|*|*|*))
     * The input is formed by 3 fields, is a tuple that contains 2 tuples and a string, the first 2 tuples correspond to the event info and 
     * the venue info respectively and the string is the possible combinations we can get with the features to get metrics from
     *
     * @param   input   Is the Tuple that pig passes to the UDF
     * @return  statistics  Returns a map structure that pig can read and later save as a JSON in mongoDB.
     *
     */
    public DataBag exec(Tuple tuple) throws IOException {
        
        // Bag to return
        DataBag bagToReturn = mBagFactory.newDefaultBag();
        
        // getting input data
        // event info
        Map<String,Object> event = (Map<String,Object>)tuple.get(0);
        // venue info
        Map<String,Object> venue = (Map<String,Object>)tuple.get(1); 
        // combinations
        String dimensions = (String)tuple.get(2);
        // combinations as array
        String[] dimensionsArray = dimensions.split(",");
        
        event.putAll(venue);
        for (String combination : dimensionsArray) {
            String key = "";
            
            String[] combinationDimension = combination.split("\\|");
            Tuple finalEvent = mTupleFactory.newTuple();
            //finalEvent.append(tuple.get(0));
            //finalEvent.append(tuple.get(1));
            for(String field : combinationDimension){
                
                    if(field.toString().equals("*")){
                        key = key + "*|";
                    }
                    else{
                        Object value = event.get(field);
                        if(value!=null){
                            key = key + value.toString() + "|";
                        }
                        else{
                            key = key + "|";
                        }
                    }
                }
            key = key.substring(0, key.length()-1);
            finalEvent.append(key);
            finalEvent.append(event.get("event.localtime"));
            finalEvent.append(event.get("device.idFA"));
            finalEvent.append(event.get("device.idDev"));
            bagToReturn.add(finalEvent);        
            }
        
        return bagToReturn; 
    }
    
    @Override
        public Schema outputSchema(Schema input) {
        
        //@outputSchema('map_reduce:bag{t:(key:chararray,value:int,start_date:datetime,end_date:datetime,interval:chararray,idFA:chararray,idDev:chararray,eventType:chararray,eventextType:chararray,eventAction:chararray)}')
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("key", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("localtime", DataType.DATETIME));
            tupleSchema.add(new Schema.FieldSchema("idFA", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("idDev", DataType.CHARARRAY));

            Schema bagSchema = new Schema(new Schema.FieldSchema("t", tupleSchema, DataType.TUPLE));
            return new Schema(new Schema.FieldSchema("event_info",bagSchema,DataType.BAG));
        } catch (FrontendException e) {
            e.printStackTrace();
            return null;
        }
    }
        
        
        
        
 }
       
