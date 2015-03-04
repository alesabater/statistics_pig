package com.mobiquitynetworks.servicestructure;


import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author asabater
 */
public class CreateFinalYear extends EvalFunc<Tuple> {
    
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        
       String strToReturn = null;
       Tuple dates = mTupleFactory.newTuple();
       DateTime startDate = null;
       DateTime endDate = null;
       
        if(tuple.size()==1){
            startDate = new DateTime((int)tuple.get(0),01,01,00,00,00,000);
            endDate = new DateTime((int)tuple.get(0),12,31,23,59,59,999);
            
        }
        else if(tuple.size()==2){
            Calendar mycal = new GregorianCalendar((int)tuple.get(0),(int)tuple.get(1)-1, 1);
            int days = mycal.getActualMaximum(Calendar.DAY_OF_MONTH);
            startDate = new DateTime((int)tuple.get(0),(int)tuple.get(1),01,00,00,00,000);
            endDate = new DateTime((int)tuple.get(0),(int)tuple.get(1),days,23,59,59,999);
        
        }
        else{
            LOG.warn("THE TUPLE createFinalStartDate received is not of size 1 or 2. Returning NULL");
        }
        dates.append(startDate);
        dates.append(endDate);
        return dates;
        
        
    }
    
    public Schema outputSchema(Schema input) {
        
        //@outputSchema('map_reduce:bag{t:(key:chararray,value:int,start_date:datetime,end_date:datetime,interval:chararray,idFA:chararray,idDev:chararray,eventType:chararray,eventextType:chararray,eventAction:chararray)}')
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("start_date", DataType.DATETIME));
            tupleSchema.add(new Schema.FieldSchema("end_date", DataType.DATETIME));

            return new Schema(new Schema.FieldSchema("t", tupleSchema, DataType.TUPLE));
        } catch (FrontendException e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
