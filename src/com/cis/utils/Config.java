package com.cis.utils;

import java.util.Properties;

public class Config {

    //private static Config config = new Config();

    private Properties p = new Properties();

    private Config() {
        try {
            p.load(this.getClass().getResourceAsStream("/config.properties"));
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static Config getInstance() {
        return new Config();
    }

    public String getPath() {
        String fp = this.getClass().getClassLoader().getResource("config.properties").getFile();
        return fp.split("WEB-INF")[0];
    }

    public boolean isDebug() {
        String b = p.getProperty("debug");
        return b != null && b.equalsIgnoreCase("true");
    }

    public static void main(String[] args) {
        System.out.println(Config.getInstance().isDebug());
        System.out.println(Config.getInstance().getPath());
    }
}
