/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.bson.types.ObjectId;
import static org.bson.types.ObjectId.isValid;
import org.joda.time.DateTime;


/**
 *
 * @author asabater
 */
public class JsonStructure extends EvalFunc<Map> {
    
    
    private static final Log LOG = LogFactory.getLog(JsonStructure.class);
    
    
    
    BagFactory mBagFactory = BagFactory.getInstance();  
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public Map exec(Tuple input) throws IOException {
        
        Map<String, Object> statistic = new HashMap<>();
        Tuple kdTuple = mTupleFactory.newTuple();
        Tuple vmTuple = mTupleFactory.newTuple();
        DataBag keysDictionary = (DataBag)input.get(0);
        Tuple eventValues = (Tuple)input.get(1);
        Map<String, Object> ts = createDateAndUnit(eventValues, eventValues.size());
        vmTuple = getMetricValues((long)input.get(2),(long)input.get(3),(long)input.get(4),vmTuple);
        String[] keyValues = eventValues.get(0).toString().split("\\|");
        Tuple iterTup;
        Map<String,Object> iterMap;
        int count = 0;
        
        for(Object column : keysDictionary){
            
            iterTup = (Tuple)column;
            if(!keyValues[count].equals("*")){

                Map<String, Object> kdMap = new HashMap<>();
                iterMap = (Map)iterTup.get(0);
                if(iterMap.get("type").equals("id")){
                    Object obj = null;
                    if(isValid(keyValues[count])){
                        obj = new ObjectId(keyValues[count]);
                    }
                    else{
                        obj = "-";
                    }
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), obj, kdMap);
                }
                else{
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), keyValues[count], kdMap);
                }
            }
            count++;
        }
        
        
        statistic.put("kd", kdTuple);
        statistic.put("vm", vmTuple);
        statistic.put("nd", new Long(kdTuple.size()));
        statistic.put("ts", ts);
        return statistic;
    }
    
    public Tuple getMetricValues(long visits, long notClk, long notDsp, Tuple vmTuple){
        
        long[] metrics = {visits,notClk,notDsp};
        String[] metricsName = {"visits","notificationsClicked","notificationsSent"};
        for (int i = 0; i<metrics.length; i++){
            Map<String, Object> vmMap = new HashMap<>();
            if(metrics[i]!=0){
               vmTuple = addMapToTuple(vmTuple, metricsName[i], metrics[i], vmMap);
            }
        }
        //Map<String,Object> metrics = new HashMap<>();
       
        return vmTuple;
    }
    
    public Map<String,Object> createDateAndUnit(Tuple eventValues, int eventValuesSize) throws ExecException{
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        Map<String, Object> mapToReturn = new HashMap<>();
        Date b = new Date();
        Date e = new Date();
        String unit = "";
        //DateTime startDate = new DateTime();
        //DateTime endDate = new DateTime();
        
        if(eventValuesSize==2){
            
            LOG.warn("THIS IS THE CASE: 2");
            /*
            startDate = new Date((int)eventValues.get(1),1,1,0,0,0);
            endDate = new Date((int)eventValues.get(1),12,31,23,59,59);
            unit = "1y";
            */
                        
            String startDateStr = (Integer)eventValues.get(1) + "-01-01T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-12-31T23:59:59.999Z";
            try {
                //startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
                //endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            /*
            startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
            endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
            */
            unit = "1y";
        }
        
        else if(eventValuesSize==3){
            LOG.warn("THIS IS THE CASE: 3");
            Calendar mycal = new GregorianCalendar((Integer)eventValues.get(1),(Integer)eventValues.get(1)-1, 1);
            int days = mycal.getActualMaximum(Calendar.DAY_OF_MONTH);
                        
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-01T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + days + "T23:59:59.999Z";
            try {
                //startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
                //endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            /*
            startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
            endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
            */
            unit = "1M";
        }
        else if(eventValuesSize==4){
            LOG.warn("THIS IS THE CASE: 4");
                        
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T23:59:59.999Z";
            try {
                //startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
                //endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            /*
            startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
            endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
            */
            unit = "1d";
        }
        else if(eventValuesSize==5){
            LOG.warn("THIS IS THE CASE: 5");
            
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T" + (Integer)eventValues.get(4) + ":00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T" + (Integer)eventValues.get(4) + ":59:59.999Z";
            try {
                //startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
                //endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            /*
            startDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),0,0);
            endDate = new Date((Integer)eventValues.get(1),(Integer)eventValues.get(2),(Integer)eventValues.get(3),(Integer)eventValues.get(4),59,59);
            */
            unit = "1h";
            
        }
        
        //Date b = DateUtils.addHours(startDate, 1);
        //Date e = DateUtils.addHours(endDate, 1);
        
        mapToReturn.put("b", b);
        mapToReturn.put("e", e);
        mapToReturn.put("u", unit);
        return mapToReturn;
    }
    
    public Tuple addMapToTuple(Tuple tup, String k, Object v, Map<String,Object> map){
        map.put("k", k);
        map.put("v", v);
        tup.append(map);
        return tup;
    }
    
//    @Override
//        public Schema outputSchema(Schema input) {
//        
//        //@outputSchema('map_reduce:bag{t:(key:chararray,value:int,start_date:datetime,end_date:datetime,interval:chararray,idFA:chararray,idDev:chararray,eventType:chararray,eventextType:chararray,eventAction:chararray)}')
//        try {
//            
//            Schema tsInner = new Schema();
//            tsInner.add(new Schema.FieldSchema("b", DataType.DATETIME));
//            tsInner.add(new Schema.FieldSchema("e", DataType.DATETIME));
//            tsInner.add(new Schema.FieldSchema("u", DataType.CHARARRAY));
//            
//            Schema mapInner = new Schema();
//            mapInner.add(new Schema.FieldSchema("k", DataType.CHARARRAY));
//            mapInner.add(new Schema.FieldSchema("v", DataType.LONG));
//            
//            
//            Schema finalSchema = new Schema();
//            finalSchema.add(new Schema.FieldSchema("kd", DataType.TUPLE));
//            finalSchema.add(new Schema.FieldSchema("vm", DataType.TUPLE));
//            finalSchema.add(new Schema.FieldSchema("nd", DataType.LONG));
//            finalSchema.add(new Schema.FieldSchema("ts", tsInner, DataType.MAP));
//            
//            return new Schema(new Schema.FieldSchema("statistic",finalSchema,DataType.MAP));
//        } catch (FrontendException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
    
}
