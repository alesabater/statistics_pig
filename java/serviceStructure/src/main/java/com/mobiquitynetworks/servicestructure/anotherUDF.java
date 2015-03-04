/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 *
 * @author asabater
 */
public class anotherUDF extends EvalFunc<Map> {
    
    private static final Log LOG = LogFactory.getLog(Statistics.class);
    BagFactory mBagFactory = BagFactory.getInstance();  
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    /*
    public Schema outputSchema(Schema input) {

       Schema TupleSchema;
       TupleSchema = new Schema();
       TupleSchema.add(new Schema.FieldSchema("id", DataType.CHARARRAY));
       TupleSchema.add(new Schema.FieldSchema("name", DataType.CHARARRAY));
       TupleSchema.add(new Schema.FieldSchema("lastName", DataType.CHARARRAY));
       
       Schema innerTupleSchema;
       innerTupleSchema = new Schema();
       innerTupleSchema.add(new Schema.FieldSchema("age", DataType.INTEGER));
       innerTupleSchema.add(new Schema.FieldSchema("country", DataType.CHARARRAY));
       
       Schema MapSchema;
       MapSchema = new Schema();
       MapSchema.add(new Schema.FieldSchema("info", innerTupleSchema));
       
       TupleSchema.add(new Schema.FieldSchema("inf", MapSchema));
       
       return TupleSchema;
       
       
       //MapSchema.add(new Schema.FieldSchema());
       TupleSchema.add(new Schema.FieldSchema("sym", DataType.CHARARRAY));
       TupleSchema.add(new Schema.FieldSchema("vm", DataType.CHARARRAY));
       TupleSchema.add(new Schema.FieldSchema("vmValue", DataType.LONG));
       
       String schemaName =  "points";
       
        try {
            return new Schema(new Schema.FieldSchema(schemaName,TupleSchema,DataType.MAP));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }*/

    @Override
    public Map exec(Tuple tuple) throws IOException {
        
        // Extrating input values
        List<Object> params = tuple.getAll();
        Map<String,Object> big = new HashMap<>();
        Map<String,Object> info = new HashMap<>();
        String name = (String)params.get(0);
        String lastName = (String)params.get(1);
        String Id = (String)params.get(2);
        int age = (int)params.get(3);
        String country = (String)params.get(4);
        
        Tuple tpl = mTupleFactory.newTuple();
        for (int i=0; i<=4 ; i++){
            Map<String, Object> ma = new HashMap<>();
            ma.put("foo", 1);
            ma.put("foo1", 2);
            tpl.append(ma);
        }
        
        
        info.put("age", age);
        info.put("country", country);
        
        
        big.put("name", name);
        big.put("lastName", lastName);
        big.put("Id", Id);
        big.put("info", info);
        big.put("tup",tpl);
        
        return big;
        
        
        /*
        tpl.set(0, name.length);
        tpl.set(1, "Alejandro");
        tpl.set(2, keyValues[5]);
        tpl.set(3, symbol);
        tpl.set(4, statisticName);
        tpl.set(5, statisticValue);*/
        
        //Date startDate = null;
        //Date endDate = null;
        
        
    }
    
}
