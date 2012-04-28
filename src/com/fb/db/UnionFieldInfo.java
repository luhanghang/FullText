package com.fb.db;

import java.util.Vector;

public class UnionFieldInfo {
	
	public String fieldName;
	
	public Vector vSubUnionFieldInfo = new Vector();
	
	
	//按照 字段名:索引名，实际索引字段名;索引名，实际索引字段名
	public void setUnionFieldInfo(String fieldInfo){
		
		if(vSubUnionFieldInfo.size() > 0)
			vSubUnionFieldInfo.clear();
		
		StringBuffer strBuf = new StringBuffer(fieldInfo);		
		int pos = strBuf.indexOf(":");
		
		if(pos != -1)
		{
			String str = strBuf.substring(0,pos);
			strBuf.delete(0,pos+1);
			fieldName = str;
		}
		
		AllIndexsInfo allIndexsInfo = AllIndexsInfo.getInstance();
		
		strBuf.append(";");
		pos = strBuf.indexOf(";");
		while(pos != -1){
			String str = strBuf.substring(0,pos);
			if(str.length() == 0)
				break;
			
			int pos2 = str.indexOf(",");
			if(pos2 !=-1){
				String indexName = str.substring(0,pos2);
				String fieldName = str.substring(pos2+1,str.length());
				
				SubUnionFieldInfo info = new SubUnionFieldInfo();
				info.indexName = indexName; 
				
				//获取字段信息
				IndexInfo indexInfo = allIndexsInfo.getIndexInfoFromDB(indexName);
				
				if(indexInfo != null)
				{
					FieldInfo fInfo = indexInfo.getFieldInfo(fieldName);					
					info.fieldInfo = fInfo;
					vSubUnionFieldInfo.add(info);	
				}										
			}
			
			strBuf.delete(0,pos+1);
			pos = strBuf.indexOf(";");
		}
	}
	
	public FieldInfo getIndexFieldInfo(String indexName){
		for(int i=0; i<vSubUnionFieldInfo.size(); i++){
			SubUnionFieldInfo info = (SubUnionFieldInfo)vSubUnionFieldInfo.get(i);
			
			if(info.indexName.equals(indexName))
				return info.fieldInfo;
		}
		
		return null;
	}
}
