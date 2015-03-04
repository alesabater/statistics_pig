/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
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
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.joda.time.DateTime;

/**
 *
 * @author asabater
 */
public class computeCombinations extends EvalFunc<DataBag> {
    
    BagFactory mBagFactory = BagFactory.getInstance(); 
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        
        DataBag bagOfEvents = (DataBag)tuple.get(0);
        DataBag bagToReturn = mBagFactory.newDefaultBag();
        List<String[]> combinations = splitFieldKeys((String)tuple.get(1));
        Tuple dates = mTupleFactory.newTuple();
        
        try{
            Tuple groupInfo = (Tuple)tuple.get(2);
            dates = getDates(groupInfo);
        }
        catch(Exception E){
            int year = (int)tuple.get(2);
            //LOG.warn("------------>>>>>>> THERE HAS BEEN AND EXCEPTION, WHAT IS HAPPENING: THE CLASS OF INPUT IS: "+tuple.get(2).getClass()+" <-------------");
            Tuple interTpl = mTupleFactory.newTuple();
            interTpl.append(year);
            CreateFinalYear gettingDates = new CreateFinalYear();
            dates = gettingDates.exec(interTpl);
            dates.append("1y");
            
        }
        for(Tuple T: bagOfEvents){
            String idFA = getSpecificFields( (Map<String,Object>)T.get(0), "uniqueId.idFA");
            String idDev = getSpecificFields( (Map<String,Object>)T.get(0), "uniqueId.idDev");
            String eventType = getSpecificFields( (Map<String,Object>)T.get(0), "event.type");
            String eventextType = getSpecificFields( (Map<String,Object>)T.get(0), "event.extType");
            String eventAction = getSpecificFields( (Map<String,Object>)T.get(0), "event.action");
            List<String[]> auxCombinations = new ArrayList<>(combinations);
            String[] eventMapped = getDesiredFields((Map<String,Object>)T.get(0), auxCombinations.get(0));
            for(int i=0;i<auxCombinations.size(); i++){
                Tuple tupForInfo = mTupleFactory.newTuple();
                String[] combinationIterator = auxCombinations.get(i).clone();
                for(int j=0; j<combinationIterator.length; j++){
                    if(!combinationIterator[j].equals("*")){
                            combinationIterator[j]=eventMapped[j];
                        
                    }
                }
                String key = StringUtils.join(combinationIterator, "|");
                int value = 1;
                DateTime start_date = (DateTime) dates.get(0);
                DateTime end_date = (DateTime) dates.get(1);
                String interval = (String) dates.get(2);
                tupForInfo.append(key);
                tupForInfo.append(value);
                tupForInfo.append(start_date);
                tupForInfo.append(end_date);
                tupForInfo.append(interval);
                tupForInfo.append(idFA);
                tupForInfo.append(idDev);
                tupForInfo.append(eventType);
                tupForInfo.append(eventextType);
                tupForInfo.append(eventAction);
                bagToReturn.add(tupForInfo);
            }
        }
        //LOG.warn("THIS IS WHAT IS GOING BACK : "+bagToReturn.toString());
        return bagToReturn;
        //return tuple.get(0).toString();
    }
    
    public Tuple getDates(Tuple group) throws ExecException{
        
        Tuple tupleToReturn = mTupleFactory.newTuple(3);
        DateTime startDate = null;
        DateTime endDate = null;
        String symbol = null;
        
        switch (group.size()){
            case 1: startDate = new DateTime((int)group.get(0),01,01,00,00,00,000);
                    endDate = new DateTime((int)group.get(0),12,31,23,59,59,999);
                    symbol = "1y";
                    break;
            case 2: Calendar mycal = new GregorianCalendar((int)group.get(0),(int)group.get(1)-1, 1);
                    int days = mycal.getActualMaximum(Calendar.DAY_OF_MONTH);
                    startDate = new DateTime((int)group.get(0),(int)group.get(1),01,00,00,00,000);
                    endDate = new DateTime((int)group.get(0),(int)group.get(1),days,23,59,59,999);
                    symbol = "1M";
                    break;
            case 3: startDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),00,00,00,000);
                    endDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),23,59,59,999);
                    symbol = "1d";
                    break;
            case 4: startDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),(int)group.get(3),00,00,000);
                    endDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),(int)group.get(3),59,59,999);
                    symbol = "1h";
                    break;
            case 5: startDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),(int)group.get(3),(int)group.get(4),00,000);
                    endDate = new DateTime((int)group.get(0),(int)group.get(1),(int)group.get(2),(int)group.get(3),(int)group.get(4),59,999);
                    symbol = "1m";
                    break;      
        }
        tupleToReturn.set(0, startDate);
        tupleToReturn.set(1, endDate);
        tupleToReturn.set(2, symbol);
        return tupleToReturn;
        
    }
    
    public String getSpecificFields(Map<String,Object> map, String field){
        
        String strToReturn = "";
        String[] fields = field.split("\\.");
        for(int i = 0; i<fields.length; i++){
            if(i==fields.length-1){
                try{
                    strToReturn+=map.get(fields[i]);
                    break;
                }
                catch (Exception E){
                    break;
                }
            }
            else{
                try{
                    map = (Map<String, Object>) map.get(fields[i]);
                    continue;
                }
                catch (Exception E){
                    break;
                }
            }
        }
        return strToReturn;
    }
    
    public List<String[]> splitFieldKeys(String keys){
        String sep1 = ",";
        String sep2 = "\\|";
        List<String[]> lToReturn = new ArrayList<>();
        String[] stLevelSplit = keys.split(sep1);
        for (int i=0; i<stLevelSplit.length; i++){
            String[] ndLevelSplit = stLevelSplit[i].split(sep2);
            lToReturn.add(ndLevelSplit);
        }
        return lToReturn;
    }
    
    public String[] getDesiredFields(Map<String,Object> event, String[] keys){
        
        String eventMapped = "";
        List<String[]> fields = new ArrayList<>();
        for(int i=0; i<keys.length; i++){
            fields.add(keys[i].split("\\."));
        }
        for(int i=0; i<fields.size(); i++){
            String[] request = fields.get(i);
            Map<String, Object> auxEvent = new HashMap<>(event);
            for(int j=0; j<request.length; j++){
                if(j==request.length-1){
                    try{
                        eventMapped+=auxEvent.get(request[j])+"|";
                        break;
                    }
                    catch (Exception E){
                        eventMapped += " " + "|";
                        break;
                    }
                }
                else{
                    try{
                        auxEvent = (Map<String, Object>) auxEvent.get(request[j]);
                        continue;
                    }
                    catch (Exception E){
                        eventMapped += " " + "|";
                        break;
                    }
                }
            }
        }
        String[] returnEventMapped = eventMapped.split("\\|");
        for(int i = 0; i<returnEventMapped.length;i++){
            if(returnEventMapped[i]==null || returnEventMapped[i].equals("null")){
                returnEventMapped[i]="-";
            }
        }
        return returnEventMapped;
    }
    

    public Schema outputSchema(Schema input) {
        
        //@outputSchema('map_reduce:bag{t:(key:chararray,value:int,start_date:datetime,end_date:datetime,interval:chararray,idFA:chararray,idDev:chararray,eventType:chararray,eventextType:chararray,eventAction:chararray)}')
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("key", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("value", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("start_date", DataType.DATETIME));
            tupleSchema.add(new FieldSchema("end_date", DataType.DATETIME));
            tupleSchema.add(new FieldSchema("interval", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("idFA", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("idDev", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("eventType", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("eventextType", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("eventAction", DataType.CHARARRAY));

            Schema bagSchema = new Schema(new FieldSchema("t", tupleSchema, DataType.TUPLE));
            return new Schema(new FieldSchema("map_reduce",bagSchema,DataType.BAG));
        } catch (FrontendException e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
