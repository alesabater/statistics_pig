/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.bson.types.ObjectId;
import static org.bson.types.ObjectId.isValid;

/**
 *
 * @author asabater
 */
public class MongoDBStore extends EvalFunc<String> {
    
    
    
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    @Override
    public String exec(Tuple tuple) throws IOException {
        
        DataBag batch = (DataBag)tuple.get(0);
        DB database = null;
        try {
            //database = connectToDataBase("mongodb://dev-mongodb-01.awsservers.mobiquitynetworks.com", 27017, "backend");
            database = connectToDataBase("localhost", 27017, "production_replica");
            LOG.info("----------------<<<<<>>>>>>>>>>>>-----------Succesfully connected to host:" + database.getMongo().getAddress() + " and port:" + database.getMongo().getConnectPoint());
        } catch (Exception e) {
            LOG.error("Failed to connect to database");
        }
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        DBCollection DBcollection = database.getCollection("statistics_foo");
        
        for(Object element: batch){
            Tuple tpl = (Tuple)element;
            BasicDBObject doc = new BasicDBObject();
            
            List<Object> params = tpl.getAll();
            DataBag keysDictionary = (DataBag)params.get(0);
            String[] keyValues = params.get(1).toString().split("\\|");
            String statisticName = (String)params.get(2);
            Long statisticValue = (Long)params.get(3);
            Date startDate = null;
            Date endDate = null;
            try {
                startDate = df.parse(params.get(4).toString().replace("Z", "+0000"));
                endDate =  df.parse(params.get(5).toString().replace("Z", "+0000"));
            } catch (ParseException ex) {
                Logger.getLogger(MongoDBStore.class.getName()).log(Level.SEVERE, null, ex);
            }


            String symbol = (String)params.get(6);



                    // Output Variables
            Map<String, Object> timestamps = new HashMap<>();
            List<Map<String,Object>> kdList = new ArrayList<>();
            List<Map<String,Object>> vmList = new ArrayList<>();

            Map<String, Object> vmMap = new HashMap<>();

            Tuple iterTup;
            Map<String,Object> iterMap;
            int count = 0;

            for(Object column : keysDictionary){
                iterTup = (Tuple)column;
                if(!keyValues[count].equals("*")){
                    Map<String, Object> kdMap = new HashMap<>();
                    iterMap = (Map)iterTup.get(0);
                    if(iterMap.get("type").equals("id")){
                        if(isValid(keyValues[count])){
                            ObjectId obj = new ObjectId(keyValues[count]);
                            kdList = addMapToList(kdList, (String)iterMap.get("name"), obj, kdMap);
                        }
                        else{
                            LOG.warn("THE VALUE "+ keyValues[count] + " IS NOT A VALID MONGODB OBJECTID, IGNORING THIS VALUE");
                        }     
                    }
                    else{
                        kdList = addMapToList(kdList, (String)iterMap.get("name"), keyValues[count], kdMap);
                    }
                }
                count++;
            }
            timestamps = setTimestamp(timestamps, startDate, endDate, symbol);
            vmList = addMapToList(vmList, statisticName, statisticValue, vmMap);
            
            BasicDBObject addToSet = new BasicDBObject("$addToSet", new BasicDBObject("vm", new BasicDBObject("$each",vmList)));

            
            doc.append("kd", kdList);
            doc.append("ts", timestamps);
            doc.append("nd", new Long(kdList.size()));
            
            
            //doc.append("vm", vmList);
            DBcollection.update(doc,addToSet,true,false);
            //DBcollection.insert(doc);
            
    
            
            LOG.warn("THE ELEMENT IS ------------->>>>>>>> "+ element.toString());
        }
        
        
        return "SUCCESS";
   
    }
    
    public DB connectToDataBase(String host, int port, String database) throws UnknownHostException{
    
    MongoClient mongoClient = new MongoClient( host , port );
    DB db = mongoClient.getDB(database);
    return db;
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
