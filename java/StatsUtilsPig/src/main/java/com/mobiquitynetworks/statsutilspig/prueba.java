/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import static org.apache.pig.data.DataType.DATETIME;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;

/**
 *
 * @author asabater
 */
public class prueba extends EvalFunc<Tuple> {

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    
    @Override
    public Tuple exec(Tuple input) throws IOException {
        
        Tuple nuevo = mTupleFactory.newTuple(2);
        int year = (int)input.get(0);
        int month = (int)input.get(1);
        int day = (int)input.get(2);
        DateTime start = new DateTime(year,month,day,0,0,0,0);
        String some = "probando fechas";
        nuevo.set(0, some);
        nuevo.set(1, start);
        return nuevo;
    }
    
    @Override
    public Schema outputSchema(Schema input) {

    //@outputSchema('map_reduce:bag{t:(key:chararray,value:int,start_date:datetime,end_date:datetime,interval:chararray,idFA:chararray,idDev:chararray,eventType:chararray,eventextType:chararray,eventAction:chararray)}')
    try {
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema("key", DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema("localtime", DataType.DATETIME));

        Schema bagSchema = new Schema(new Schema.FieldSchema("t", tupleSchema, DataType.TUPLE));
        return bagSchema;
    } catch (FrontendException e) {
        e.printStackTrace();
        return null;
    }
    }
    
}
