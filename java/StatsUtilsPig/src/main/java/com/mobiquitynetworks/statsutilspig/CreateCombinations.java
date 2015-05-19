/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.IntRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.commons.math3.util.ArithmeticUtils.binomialCoefficientDouble;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 *
 * @author asabater
 */
public class CreateCombinations extends EvalFunc<String>{
    
    private static final Log LOG = LogFactory.getLog(CreateCombinations.class);
    
    @Override
    public String exec(Tuple tuple) throws IOException {
        
        String[] fields = tuple.get(0).toString().split(",");
        Integer fieldsCardinality = fields.length;
        //LOG.warn("--------------------------->>>>>>>>>>>>>>>>>>>>> THE AMOUNT OF FIELDS IS: "+ fieldsCardinality + " <<<<<<<<<<<<<<<<<<<<<<<<<<---------------------------");
        int[] combinationValues = getCombinationValues(fieldsCardinality);
        Map<Integer,Object> vectors = getVectors(fieldsCardinality);
        String symbol = "*";
        List<String[]> combStrings = computeCombinations(fields, combinationValues, vectors, symbol);
        //int combStrings = computeCombinations(fields, combinationValues, vectors, symbol);
        String returnCombination = "";
        for(int i =0;i<combStrings.size();i++){
            String joined = StringUtils.join(combStrings.get(i), "|");
            if(i==combStrings.size()-1){
                returnCombination += joined;
            }
            else{
                returnCombination += joined+",";
            }
        }
        
        return returnCombination;
   
    }
    
    public int[] getCombinationValues(Integer fieldsCardinality){
        int[] combinations = new int[fieldsCardinality];
        for(int i = 1; i<=combinations.length; i++){
            Double number = binomialCoefficientDouble(fieldsCardinality,i);
            combinations[i-1]= number.intValue();
        }
        return combinations;
    }
    
    public Map<Integer,Object> getVectors(Integer fieldsCardinality){
        Map<Integer,Object> vectors = new HashMap<>();
        for(int i = 0; i<fieldsCardinality; i++){
            int[] intArray = new IntRange(0, i).toArray();
            vectors.put(i+1, intArray);
        }
        return vectors;
    }
    
    public List<String[]> computeCombinations(String[] fields, int[] combinationValues, Map<Integer,Object> vectors, String symbol){

        List<String[]> combinations = new ArrayList<>();
        String[] combo = new String[fields.length];
        System.arraycopy( fields, 0, combo, 0, fields.length );
        combinations.add(combo);
        
        for(int i = 0; i < combinationValues.length; i++){
            int[] auxVector = (int[])vectors.get(i+1);
            int leaderIdx = auxVector.length-1;
            int subLeaderIdx = auxVector.length-2;
            int head = auxVector.length-1;
            int counter = 1;
            
            while (counter<combinationValues[i]){
                
                counter++;
                combinations.add(createCombination(fields, auxVector, symbol));
                
                if(auxVector[head]==fields.length-1){
                    
                    if((auxVector[leaderIdx]-auxVector[subLeaderIdx])==2 && subLeaderIdx!=0){
                        leaderIdx--;
                        subLeaderIdx--;
                        auxVector[leaderIdx]++; 
                    }
                    else{
                        int movingIdx = subLeaderIdx;
                        int value = auxVector[subLeaderIdx]+1;
                        while (movingIdx<=(auxVector.length-1)){
                            auxVector[movingIdx] = value;
                            value++;
                            movingIdx++;
                        }
                        leaderIdx = auxVector.length-1;
                        subLeaderIdx = auxVector.length-2;
                    }
                }
                else{
                    auxVector[leaderIdx]++;
                }    
            }
            combinations.add(createCombination(fields, auxVector, symbol));
        }
        return combinations;
    }
    
    public String[] createCombination(String[] fields, int[] auxVector, String symbol){
        
        String[] arrayToReturn = new String[fields.length];
        System.arraycopy( fields, 0, arrayToReturn, 0, fields.length );
        for(int i=0; i<auxVector.length; i++){
            if(!(i<0 || i>=fields.length)){
                arrayToReturn[auxVector[i]] = symbol;
            }
        }
        return arrayToReturn; 
    }
}