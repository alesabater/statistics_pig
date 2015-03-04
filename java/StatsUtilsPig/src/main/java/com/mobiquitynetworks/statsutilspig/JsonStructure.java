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
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.bson.types.ObjectId;
import static org.bson.types.ObjectId.isValid;


/**
 *
 * @author asabater
 */
public class JsonStructure extends EvalFunc<Map> {
    
    
    private static final Log LOG = LogFactory.getLog(JsonStructure.class);
    
    
    // Bag factory instace to create as many bags as desired
    BagFactory mBagFactory = BagFactory.getInstance();  
    // Tuple factory instace to create as many bags as desired
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    /* 
     * Method that is executed when a UDF is called
     * Recieves a tuple of the following form:
     *            ({
     *               ([type#id,real#venue.id,name#Venue Id]),
     *               ([type#string,real#venue.location.state,name#Venue State]),
     *               ([type#id,real#event.value.notification,name#Notification Id]),
     *               ([type#string,real#app,name#AppKey]),
     *               ([type#id,real#clientId,name#Customer]),
     *               ([type#string,real#venue.name,name#Venue Name])
     *            },
     *            (5476d523a20d45dd20e8dd1f|*||be119bdc27c47750acce35c50ed9d09fbedfddd4|*|Kitsap Mall,2015,1,24),1,0,0)
     * As you can see the input is formed by a tuple that contains a bag and another tuple inside, the bag which is the first element of the tuple
     * contains information of the features we need to calculate metrics and the inner tuple contains the event info and counters
     *
     * @param   input   Is the Tuple that pig passes to the UDF
     * @return  statistics  Returns a map structure that pig can read and later save as a JSON in mongoDB.
     *
     */
    public Map exec(Tuple input) throws IOException {
        
        // Getting data from input and placing it in the adequate java structures
        // Bag instance to grab features information of the input
        DataBag keysDictionary = (DataBag)input.get(0);
        // Tuple instance to grab events information of the input
        Tuple eventValues = (Tuple)input.get(1);
        
        // Creating Java structures for the map that will return the UDF
        // Map to return 
        Map<String, Object> statistic = new HashMap<>();
        // Tuple to store keys and values of the dimensions
        Tuple kdTuple = mTupleFactory.newTuple();
        // Tuple to store keys and values of the metrics
        Tuple vmTuple = mTupleFactory.newTuple();
        // Map with date information such as begin, end and unit
        Map<String, Object> ts = createDateAndUnit(eventValues, eventValues.size());
        // Tuple with maps of metrics
        vmTuple = getMetricValues((long)input.get(2),(long)input.get(3),(long)input.get(4),vmTuple);
        // Exploding first field of the inner tuple of input, which contains the keys of the event in the form:
        //          5476d523a20d45dd20e8dd1f|*||be119bdc27c47750acce35c50ed9d09fbedfddd4|*|Kitsap Mall
        String[] keyValues = eventValues.get(0).toString().split("\\|");
        
        // Loop that will go through the bag of features we want, creating key-value pairs with the name of the feature or dimension and its value
        // Declaring temporal tuple for loop
        Tuple iterTup;
        // Declaring temporal map
        Map<String,Object> iterMap;
        // counter
        int count = 0;
        //Start of loop - the loop will perform at most the size of the bag, so if the bag size is 7 the loop will run 7 times
        for(Object column : keysDictionary){
            
            // The bag contains tuples inside and maps inside the tuples, so we have to instantiate them as tuples
            iterTup = (Tuple)column;
            // if the value in the key at the position of the counter is not * then we have to insert the value instead
            if(!keyValues[count].equals("*")){
                
                // Map that will be added to the kdTuple which contains the array of maps for the value of features
                Map<String, Object> kdMap = new HashMap<>();
                // Getting the map embedded into the tuple of features wanted
                iterMap = (Map)iterTup.get(0);
                // Checking for the type of field that the value should be, if the type is id then it has to be and ObjectId if not it is a String
                if(iterMap.get("type").equals("id")){
                    
                    Object obj = null;
                    // Checking if the value for the feature is really a ObjectId or if it is a fake or bad created ObjectId
                    if(isValid(keyValues[count])){
                        // The value is valid so we create an ObjectId out of it
                        obj = new ObjectId(keyValues[count]);
                    }
                    else{
                        // Invalid ObjectId, we use a "-"
                        obj = "-";
                    }
                    // Add Map with ObjectId to Tuple
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), obj, kdMap);
                }
                else{
                    // Add Map without ObjectId to Tuple
                    kdTuple = addMapToTuple(kdTuple, (String)iterMap.get("name"), keyValues[count], kdMap);
                }
            }
            // move counter
            count++;
        }
        
        // Add metrics tuple to statistics map
        statistic.put("kd", kdTuple);
        // Add dimensions tuple to statistics map
        statistic.put("vm", vmTuple);
        // Add number of dimensions
        statistic.put("nd", new Long(kdTuple.size()));
        // Add time info to statistics map
        statistic.put("ts", ts);
        return statistic;
        
    }
    
    /* 
     * Method that creates the Tuple of metrics
     *
     * @param   visits  number of visits of event
     * @param   notClk  number of notifications clicked of event
     * @param   notDsp  number of notifications displayed of event
     * @return  vmTuple Tuple with metrics
     */
    public Tuple getMetricValues(long visits, long notClk, long notDsp, Tuple vmTuple){
        
        // Array of type long to store metrics of event
        long[] metrics = {visits,notClk,notDsp};
        // possible metrics
        String[] metricsName = {"visits","notificationsClicked","notificationsSent"};
        
        for (int i = 0; i<metrics.length; i++){
            // Map to store into the Tuple to return for each valid metric
            Map<String, Object> vmMap = new HashMap<>();
            // if the metric is equal to 0 we dont store it
            if(metrics[i]!=0){
               // Method that fills the map that will be stored into the metrics tuple
               vmTuple = addMapToTuple(vmTuple, metricsName[i], metrics[i], vmMap);
            }
        }
       
        return vmTuple;
    }
    
    
    /* 
     * Creates the date format needed to be stored in MongoDB, it also sets de time frame or unit
     *
     * @param   eventValues     event information without the metrics
     * @param   eventValuesSize size of the tuple with event information
     * @return  mapToReturn     Map with begin, end and unit of the localtime of the event
     */
    public Map<String,Object> createDateAndUnit(Tuple eventValues, int eventValuesSize) throws ExecException{
        
        // format for dates
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        // timezone for dates
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        // Map to be returned
        Map<String, Object> mapToReturn = new HashMap<>();
        Date b = new Date();
        Date e = new Date();
        String unit = "";
        
        // case we only have the year so time unit would be '1y'
        if(eventValuesSize==2){
     
            String startDateStr = (Integer)eventValues.get(1) + "-01-01T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-12-31T23:59:59.999Z";
            try {
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            unit = "1y";
        }
        
        // case we have the year and month so time unit would be '1M'
        else if(eventValuesSize==3){

            Calendar mycal = new GregorianCalendar((Integer)eventValues.get(1),(Integer)eventValues.get(1)-1, 1);
            int days = mycal.getActualMaximum(Calendar.DAY_OF_MONTH);
                        
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-01T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + days + "T23:59:59.999Z";
            try {
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            unit = "1M";
        }
        
        // case we have the year, month and day so time unit would be '1d'
        else if(eventValuesSize==4){
                        
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T00:00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T23:59:59.999Z";
            try {
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            unit = "1d";
        }
        
        // case we have the year, month, day and hour so time unit would be '1h'
        else if(eventValuesSize==5){
            
            String startDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T" + (Integer)eventValues.get(4) + ":00:00.000Z";
            String endDateStr = (Integer)eventValues.get(1) + "-" + (Integer)eventValues.get(2) + "-" + (Integer)eventValues.get(3) + "T" + (Integer)eventValues.get(4) + ":59:59.999Z";
            try {
                b = df.parse(startDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                e = df.parse(endDateStr);
            } catch (ParseException ex) {
                Logger.getLogger(JsonStructure.class.getName()).log(Level.SEVERE, null, ex);
            }   
            unit = "1h";       
        }
        mapToReturn.put("b", b);
        mapToReturn.put("e", e);
        mapToReturn.put("u", unit);
        return mapToReturn;
    }
    
    /* 
     * Method that adds map to a tuple
     *
     * @param   tup  tuple to add map to
     * @param   k  key of the map
     * @param   v  value of the map
     * @return  tup tuple with map
     */
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
