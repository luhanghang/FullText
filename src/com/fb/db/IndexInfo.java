package com.fb.db;

import java.util.Vector;

public class IndexInfo {
	
	public int indexID;
	
	public String indexName;
	
	public String dbType;
	
	public String dbIP;
	
	public String dbName;
	
	public String passwd;
	
	public String userName;
	
	public String tableName;
	
	public String indexPath;
	
	public int indexStatus;
	
	public Vector vFieldInfo = new Vector();
	
	public void setFieldInfo(String fieldInfo){
		if(vFieldInfo.size() > 0)
			vFieldInfo.clear();
		
		StringBuffer strBuf = new StringBuffer(fieldInfo);
		strBuf.append("&");
		
		int pos = strBuf.indexOf("&");
		while(pos != -1){
			String str = strBuf.substring(0,pos);
			if(str.length() == 0)
				break;
			
			int pos2 = str.indexOf(",");
			if(pos2 != -1){
				String fieldName = str.substring(0,pos2);

				int indexType = Integer.parseInt(str.substring(pos2+1,str.length()));
				
				FieldInfo fInfo = new FieldInfo();
				fInfo.indexType = indexType;
				fInfo.fieldName = fieldName;
				
				vFieldInfo.add(fInfo);
			}
			
			strBuf.delete(0,pos+1);
			pos = strBuf.indexOf("&");
		}
	}	
	
	public FieldInfo getFieldInfo(String fieldName){
		
		for(int i=0; i<vFieldInfo.size(); i++){
			FieldInfo fieldInfo = (FieldInfo)vFieldInfo.get(i);
			if(fieldInfo.fieldName.equals(fieldName))
				return fieldInfo;
		}		
		return null;
	}

}
