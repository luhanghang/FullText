package com.fb.db;

import java.util.Properties;

import com.cis.utils.Config;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.fb.common.util.XmlEntry;
import com.fb.common.util.XmlPackager;
import com.fb.common.util.Env;

public class DBInfoManager {

	private static DBInfoManager instance = null;

	private DBInfoManager() throws Exception {
		init();
	}

	public static DBInfoManager getInstance() throws Exception {
		if (instance == null)
			instance = new DBInfoManager();

		return instance;
	}

	public void init() throws Exception {
	        //String confFile = Env.getConfPath() + Env.fileSep +  "dbinfo.xml";
	        String confFile = (new StringBuffer(Config.getInstance().getPath()).append("WEB-INF/dbinfo.xml")).toString();
            System.out.println("configFile->" + confFile);
            XmlPackager parser = new XmlPackager();
            parser.parseFile(confFile);
            Document _document = parser.document();
            Element elem = _document.getDocumentElement();
            XmlEntry rootEntry = new XmlEntry(elem);

            Properties prop = getProperties(rootEntry, "dbInfo");
            
            dbIP = (String) prop.getProperty("ip");
            dbUser = (String) prop.getProperty("user");
            dbPasswd = (String) prop.getProperty("passwd");
            dbName = (String) prop.getProperty("dbName");
            dbType = (String)prop.getProperty("dbType");
            System.out.println(dbIP + ":" + dbUser + ":" + dbName);
	       
	    }

	private Properties getProperties(XmlEntry root, String element) {
		XmlEntry entry = root.getSubEntry(element);
		if (entry.entryName() == null) {
			return null;
		}
		return entry.getAttributeList();
	}

	private String dbIP;

	private String dbUser;

	private String dbPasswd;

	private String dbName;
	
	private String dbType;

	public String getDbIP() {
		return dbIP;
	}

	public void setDbIP(String dbIP) {
		this.dbIP = dbIP;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbPasswd() {
		return dbPasswd;
	}

	public void setDbPasswd(String dbPasswd) {
		this.dbPasswd = dbPasswd;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

    public static void main(String[] args) throws Exception {
        String confFile = (new StringBuffer(Config.getInstance().getPath()).append("WEB-INF/dbinfo.xml")).toString();
        System.out.println(confFile);
    }
}
