/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mobiquitynetworks.statsutilspig;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.joda.time.DateTime;

/**
 *
 * @author asabater
 */
public class foo {
    
    public static void main(String [] args) throws ParseException{
       Date b = new Date();
       Integer a = 2015;
       DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(TimeZone.getTimeZone("CET"));
        String endDateStr = a + "-12-31T23:59:59.999Z";
        b = df.parse(endDateStr);
        //isoFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(b.toString());
    }
    
}
