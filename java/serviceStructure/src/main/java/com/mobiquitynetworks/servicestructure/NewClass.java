/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.servicestructure;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.math.IntRange;
import static org.apache.commons.math3.util.ArithmeticUtils.binomialCoefficientDouble;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
/**
 *
 * @author asabater
 */
public class NewClass {
    
    public static void main(String[] args) throws ExecException, IOException, ParseException{
        
        /*String a = "aleja||ndro||finall";
        String[] b = a.split("\\|");
        String c = StringUtils.join(b,"|");
        System.out.println(c);*/
        
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        DB db = mongoClient.getDB("production_replica");
        DBCollection col = db.getCollection("callForVictory");
        col.drop();
        
        //SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        
        //DateTime z = new DateTime("2012-08-16T07:22:05Z");
        //System.out.println(z.toString());
        //System.out.println(z.getMillis());
        //Date endDate =  df.parse(date);
        
        
        //String[] fields = a.split("\\|");
        //System.out.println(Arrays.toString(fields));
        
        /*
        System.out.println(fields[0]);
        System.out.println(Arrays.toString(fields));
        String keys = "*|app|*|*|*|venue.location.country|*|*|*|*|*|*|*,*|app|*|*|event.action|*|*|*|*|*|*|*|*,*|app|*|event.extType|*|*|*|*|*|*|*|*|*,*|app|event.value.notification|*|*|*|*|*|*|*|*|*|*,clientId|*|*|*|*|*|*|*|*|*|*|*|device.osVersion,clientId|*|*|*|*|*|*|*|*|*|*|device.os|*,clientId|*|*|*|*|*|*|*|*|*|venue.type|*|*,clientId|*|*|*|*|*|*|*|*|venue.name|*|*|*,clientId|*|*|*|*|*|*|*|venue.id|*|*|*|*,clientId|*|*|*|*|*|*|venue.location.city|*|*|*|*|*,clientId|*|*|*|*|*|venue.location.state|*|*|*|*|*|*,clientId|*|*|*|*|venue.location.country|*|*|*|*|*|*|*,clientId|*|*|*|event.action|*|*|*|*|*|*|*|*,clientId|*|*|event.extType|*|*|*|*|*|*|*|*|*,clientId|*|event.value.notification|*|*|*|*|*|*|*|*|*|*,clientId|app|*|*|*|*|*|*|*|*|*|*|*,*|*|*|*|*|*|*|*|*|*|*|*|device.osVersion,*|*|*|*|*|*|*|*|*|*|*|device.os|*,*|*|*|*|*|*|*|*|*|*|venue.type|*|*,*|*|*|*|*|*|*|*|*|venue.name|*|*|*,*|*|*|*|*|*|*|*|venue.id|*|*|*|*,*|*|*|*|*|*|*|venue.location.city|*|*|*|*|*,*|*|*|*|*|*|venue.location.state|*|*|*|*|*|*,*|*|*|*|*|venue.location.country|*|*|*|*|*|*|*,*|*|*|*|event.action|*|*|*|*|*|*|*|*,*|*|*|event.extType|*|*|*|*|*|*|*|*|*,*|*|event.value.notification|*|*|*|*|*|*|*|*|*|*,*|app|*|*|*|*|*|*|*|*|*|*|*,clientId|*|*|*|*|*|*|*|*|*|*|*|*,*|*|*|*|*|*|*|*|*|*|*|*|*";
        NewClass b = new NewClass();
        List<String[]> list = b.splitFieldKeys(keys,",", "|");
        String sa = "sadsadsaa | asdsdsa"+"|";
        String[] sb = sa.split("\\|");
        System.out.println(Arrays.toString(sb));
        System.out.println(sa);
        System.out.println(Arrays.toString(list.get(0)));
        String[] prueba = list.get(0);
        String joined = StringUtils.join(prueba, "|");
        System.out.println(joined);*/
       /* String str;
        str = "{([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#54491ba5689319a6170ea70c,event#{timestamp=1413893903385, value={beacon=539525540af550fd5cfb96b6}, action=RQT, extType=GEO, type=SGN},device#{os=Android, gpsVersion=6.1.09 (1459805-038), osVersion=4.4.2, type=samsung GT-I9505, idFA=2de252cf-566e-4292-a9d3-532255dfa520},sdk#{version=0.5.0},date#{minute=18, month=10, year=2014, hour=12, day=21, complete=2014-10-21T12:18:23.385Z},venue#{region={_id=539525540af550fd5cfb96b6}, id=5404166a7b2e4e2017f4d106, type=mall, location={state=CA, zip=, city=Torrance, country=us}, name=Del Amo Fashion Center},user#{role=public, username=german@mobiquitynetworks.com, provider=Facebook},clientId#54491ba5689319a6170ea70b,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa520}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5451f78eea1f4c4c3d5e3a4d,event#{timestamp=1414657930576, action=RQT, value={beacon=539850be6375f9c787ff0551}, extType=GEO, type=SGN},device#{type=iPhone7,2, os=iOS, idFA=2de252cf-566e-4292-a9d3-532255dfa580, osVersion=7.1.2},sdk#{version=2.0.0},date#{minute=32, month=10, year=2014, hour=8, day=30, complete=2014-10-30T08:32:10.576Z},venue#{region={_id=539850be6375f9c787ff0551}, id=540435987b2e4e071df4d11f, type=mall, location={state=dssa, zip=232321, city=Hanover, country=gb}, name=Arundel Mills Marketplace},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5451f78eea1f4c4c3d5e3905,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa580}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450d043f8ccca8930c602b9,event#{timestamp=1414582338572, value={beacon=539850be6375f9c787ff0560}, action=RQT, extType=GEO, type=SGN},device#{type=iPhone7,1, os=iOS, idFA=2de252cf-566e-4292-a9d3-532255dfa580, osVersion=8.0.2},sdk#{version=2.0.0},date#{minute=32, month=10, year=2014, hour=11, day=29, complete=2014-10-29T11:32:18.572Z},venue#{region={_id=539850be6375f9c787ff0560}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450d043f8ccca8930c60285,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa580}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450c860639c1f1a2e61a376,event#{timestamp=1414580319624, value={beacon=539850be6375f9c787ff0578}, action=RQT, extType=GEO, type=SGN},device#{type=iPad4,4, os=iOS, idFA=2de252cf-566e-4292-a9d3-532255dfa520, osVersion=7.1},sdk#{version=2.0.0},date#{minute=58, month=10, year=2014, hour=10, day=29, complete=2014-10-29T10:58:39.624Z},venue#{region={_id=539850be6375f9c787ff0578}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450c860639c1f1a2e61a369,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa520}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5451f905f781f43b3e4d86d1,event#{timestamp=1414658304475, action=RQT, value={beacon=539850be6375f9c787ff05e6}, extType=GEO, type=SGN},device#{type=samsung GT-I9505, os=Android, idFA=2de252cf-566e-4292-a9d3-532255dfa530, osVersion=5.0},sdk#{version=0.5.3},date#{minute=38, month=10, year=2014, hour=8, day=30, complete=2014-10-30T08:38:24.475Z},venue#{region={_id=539850be6375f9c787ff05e6}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5451f904f781f43b3e4d85d4,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa530}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450d043f8ccca8930c602c6,event#{timestamp=1414582338572, value={beacon=539850be6375f9c787ff0625}, action=RQT, extType=GEO, type=SGN},device#{type=samsung GT-I9505, os=Android, idFA=2de252cf-566e-4292-a9d3-532255dfa540, osVersion=4.3},sdk#{version=0.5.3},date#{minute=32, month=10, year=2014, hour=11, day=29, complete=2014-10-29T11:32:18.572Z},venue#{region={_id=539850be6375f9c787ff0625}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450d043f8ccca8930c6028f,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa540}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450f72da18453ac32f65841,event#{timestamp=1414592298260, value={beacon=539850be6375f9c787ff062f}, action=RQT, extType=GEO, type=SGN},device#{type=LGE Nexus 5, os=Android, idFA=2de252cf-566e-4292-a9d3-532255dfa530, osVersion=4.4},sdk#{version=0.5.3},date#{minute=18, month=10, year=2014, hour=14, day=29, complete=2014-10-29T14:18:18.260Z},venue#{region={_id=539850be6375f9c787ff062f}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450f72ca18453ac32f6570d,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa530}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450f749a9c96cee320634a4,event#{timestamp=1414592325842, value={beacon=539850be6375f9c787ff063f}, action=RQT, extType=GEO, type=SGN},device#{type=iPad4,4, os=iOS, idFA=2de252cf-566e-4292-a9d3-532255dfa570, osVersion=7.1.2},sdk#{version=2.0.0},date#{minute=18, month=10, year=2014, hour=14, day=29, complete=2014-10-29T14:18:45.842Z},venue#{region={_id=539850be6375f9c787ff063f}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450f748a9c96cee320633c6,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa570}]),([app#a90a26bab9868d071c1e4325a9623df284efd384,_id#5450f7186b27137e324316d4,event#{timestamp=1414592278240, value={beacon=539850be6375f9c787ff0668}, action=RQT, extType=GEO, type=SGN},device#{type=iPhone7,2, os=iOS, idFA=2de252cf-566e-4292-a9d3-532255dfa580, osVersion=8.0.2},sdk#{version=2.0.0},date#{minute=17, month=10, year=2014, hour=14, day=29, complete=2014-10-29T14:17:58.240Z},venue#{region={_id=539850be6375f9c787ff0668}, id=null, type=null, location={state=null, zip=null, city=null, country=null}, name=null},user#{role=public, username=dummy@mobiquitynetworks.com, provider=Facebook},clientId#5450f7186b27137e32431625,uniqueId#{idDev=null, idFA=2de252cf-566e-4292-a9d3-532255dfa580}])}";
        BagFactory mBagFactory = BagFactory.getInstance(); 
        TupleFactory mTupleFactory = TupleFactory.getInstance();
        DataBag baggie = mBagFactory.newDefaultBag();
        baggie = (DataBag)str;
        Tuple a = mTupleFactory.newTuple(1);
        a.set(0, dims);
        NewClass b = new NewClass();
        System.out.println(b.exec(a));
                */
        //System.out.println(b.exec(a));
        
    }
    
    BagFactory mBagFactory = BagFactory.getInstance(); 
    TupleFactory mTupleFactory = TupleFactory.getInstance();
    
        public List<String[]> splitFieldKeys(String keys, String sep1, String sep2){
        sep1 = ",";
        sep2 = "\\|";
        List<String[]> lToReturn = new ArrayList<>();
        String[] stLevelSplit = keys.split(sep1);
        for (int i=0; i<stLevelSplit.length; i++){
            String[] ndLevelSplit = stLevelSplit[i].split(sep2);
            lToReturn.add(ndLevelSplit);
        }
        return lToReturn;
    }
    
    public String exec(Tuple tuple) throws IOException {
        DataBag bagOfEvents = (DataBag)tuple.get(0);
        String Combinations = (String)tuple.get(1);
        Tuple groupInfo = (Tuple)tuple.get(2);
        return getDates(groupInfo).toString();
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
            case 2: Calendar cal = Calendar.getInstance();
                    cal.set(Calendar.YEAR, (int)group.get(0));
                    cal.set(Calendar.MONTH, (int)group.get(1));
                    startDate = new DateTime((int)group.get(0),(int)group.get(1),01,00,00,00,000);
                    endDate = new DateTime((int)group.get(0),(int)group.get(1),cal.getActualMaximum(Calendar.DAY_OF_MONTH),23,59,59,999);
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
    
}
