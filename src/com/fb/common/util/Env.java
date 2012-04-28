package com.fb.common.util;

public class Env {

	public static String fileSep = null;

	public static String pathSep = null;

	public static String homePath = "d:/";//;com.cis.utils.Config.getInstance().getPath();

	static {
		fileSep = System.getProperty("file.separator");
		pathSep = System.getProperty("path.separator");
	}
	
	public static String getConfPath(){
		String confPath = homePath;
		if (!homePath.endsWith(fileSep))
            confPath = homePath + fileSep;
		
		confPath = confPath + "conf";
		return confPath;
	}
}
