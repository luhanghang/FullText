package com.cis.utils;

import com.fb.common.util.CommonUtil;

import java.sql.Connection;
import java.sql.DriverManager;

public class DB {
    static String[] host = {"192.168.0.80", "localhost"};
    static String[] pass = {"zzwl0518", "cistyz0328"};

    public static Connection getConnection(int i) throws Exception {
        String url = CommonUtil.getDBConnString("MySql", host[i], "infoseek");
        String driver = CommonUtil.getDbDriverStr("MySql");
        Class.forName(driver).newInstance();
        return DriverManager.getConnection(url, "root", pass[i]);
    }
}
