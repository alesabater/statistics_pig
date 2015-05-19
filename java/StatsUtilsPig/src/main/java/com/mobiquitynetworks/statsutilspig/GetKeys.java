/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

import com.google.common.primitives.Ints;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.bson.types.ObjectId;
import static org.bson.types.ObjectId.isValid;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author asabater
 */
public class GetKeys extends EvalFunc<String>{
    
    private static final Log LOG = LogFactory.getLog(GetKeys.class);
    BagFactory mBagFactory = BagFactory.getInstance();  
    TupleFactory mTupleFactory = TupleFactory.getInstance();

    @Override
    public String exec(Tuple params) throws IOException {
        
        DataBag keysDictionary = (DataBag)params.get(0);
        String [] keys = new String[Ints.checkedCast(keysDictionary.size())];
        
        int counter = 0;
        for (Object obj: keysDictionary){
            Tuple iterTup = (Tuple)obj;
            Map<String,String>iterMap = (Map)iterTup.get(0);
            keys[counter] = iterMap.get("real");
            counter++;
        }
        
        return StringUtils.join(keys,",");
        
        
    }
    
    
}