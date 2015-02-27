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

    BagFactory mBagFactory = BagFactory.getInstance(); 
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final Log LOG = LogFactory.getLog(keysCombinations.class);    
    
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        
        
        DataBag bagToReturn = mBagFactory.newDefaultBag();
        Map<String,Object> event = (Map<String,Object>)tuple.get(0);
        Map<String,Object> venue = (Map<String,Object>)tuple.get(1); 
        String dimensions = (String)tuple.get(2);
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
       
