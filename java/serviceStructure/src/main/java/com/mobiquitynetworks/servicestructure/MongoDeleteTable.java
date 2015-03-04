/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import java.io.IOException;
import java.net.UnknownHostException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
/**
 *
 * @author asabater
 */
public class MongoDeleteTable extends EvalFunc<String>{
    
    
    
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    String _host = new String();
    int _port = 27017;
    String _db = new String();
    String _collection = new String();
    MongoClient mongoClient;
    DB dataBase;
    DBCollection col;
    
    public MongoDeleteTable(String host, String db, String collection) throws UnknownHostException{
        
        _host = host;
        _db = db;
        _collection = collection;
        //_port = port;
        mongoClient = new MongoClient(new ServerAddress(_host, _port));
        dataBase = mongoClient.getDB(_db);
        
    }
    
    @Override
    public String exec(Tuple tuple) throws IOException {
        
        
        
        try{
            
            //col.drop();
            dataBase.getCollection(_collection).drop();
            LOG.warn("THE COLLECTION: "+ _collection +" OF THE DATABASE:" + _db + " HAS JUST BEEN DROPPED");
            
            try{
                BasicDBObject idx = new BasicDBObject();
                DBObject opt = new BasicDBObject("background", true);
                idx.append("ts.b", -1);
                idx.append("ts.e", -1);
                idx.append("ts.u", 1);
                idx.append("nd", 1);
                idx.append("kd.k", 1);
                opt.put("background", true);
                dataBase.createCollection(_collection,null);
                col = dataBase.getCollection(_collection);
                col.createIndex(idx);
                return (String)tuple.get(0); 
            
            }
            catch(Exception E){
                LOG.error(E);
                LOG.warn("THE COLLECTION: "+ _collection +" OF THE DATABASE:" + _db + " HAS NOT BEEN CREATED DUE TO AN EXCEPTION");
                return (String)tuple.get(0);  
            }
        }
        catch(Exception E){
            LOG.warn("The Database has not been deleted due to the exception: " + E);
            return (String)tuple.get(0);
        }

        
    }
}