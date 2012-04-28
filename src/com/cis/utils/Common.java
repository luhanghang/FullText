package com.cis.utils;

import java.util.*;
import java.text.*;

public class Common {
    public static final TimeZone DEFAULT_TIMEZONE = TimeZone.getTimeZone("GMT+08:00");
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Return Current Time by giving format and default timezone ("GMT+08:00")
     * @param format java.text.SimpleDateFormat format
     * @return Current Time String
     */
    public static String now(String format) {
        DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(DEFAULT_TIMEZONE);
        return df.format(new Date());
    }


    /**
     * Return Current Time by default format and default timezone ("GMT+08:00")
     * @return Current Time String like "yyyy-MM-dd HH:mm:ss"
     */
    public static String now(){
        return now(DEFAULT_FORMAT);
    }

    /**
     * Return Current Time by giving format and giving timezone
     * @param format java.text.SimpleDateFormat format
     * @param tZone Time Zone
     * @return Current Time String
     */
    public static String now(String format,TimeZone tZone) {
        DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(tZone);
        return df.format(new Date());
    }

    /**
     * Return Current Time by giving format and giving timezone
     * @param tZone Time Zone
     * @return Current Time String
     */
    public static String now(TimeZone tZone){
        return now(DEFAULT_FORMAT,tZone);
    }

    public static void main(String[] args){
        for(int i = 0; i < 50 ; i++)
        System.out.println(Common.now("yyyyMMddHHmmssS"));
    }

    /**
     * Get date string by giving calendar and giving format and default timezone ("GMT+08:00") 
     * @param calendar Calendar
     * @param format java.text.SimpleDateFormat format
     * @return Date String
     */
    public static String showDate(Calendar calendar, String format){
    	DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(DEFAULT_TIMEZONE);
        return df.format(calendar.getTime());
    }

    /**
     * Get date string by giving calendar and default format (yyyy-MM-dd HH:mm:ss) and default timezone ("GMT+08:00")
     * @param calendar Calendar
     * @return Date String
     */
    public static String showDate(Calendar calendar){
    	return showDate(calendar, DEFAULT_FORMAT);
    }

    /**
     * Get date string by giving calendar and giving format and giving timezone
     * @param calendar Calendar
     * @param format java.text.SimpleDateFormat format
     * @param tZone Time Zone
     * @return Date String
     */
    public static String showDate(Calendar calendar, String format, TimeZone tZone){
    	DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(tZone);
        return df.format(calendar.getTime());
    }

    /**
     * Get date string by giving calendar and default format (yyyy-MM-dd HH:mm:ss) and giving timezone
     * @param calendar Calendar
     * @param tZone Time Zone
     * @return Date String
     */
    public static String showDate(Calendar calendar, TimeZone tZone){
    	return showDate(calendar, DEFAULT_FORMAT, tZone);
    }
}