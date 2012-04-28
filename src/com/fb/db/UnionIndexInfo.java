package com.fb.db;

import java.util.Vector;

public class UnionIndexInfo {

	public int indexID;

	public String indexName;

	public String indexPath;

	public int indexStatus;
	
	public Vector vSubIndexInfo = new Vector();
	
	public Vector vFieldInfo = new Vector();
	
	//���� �ֶ���:��������ʵ�������ֶ���;��������ʵ�������ֶ���&�ֶ���:��������ʵ�������ֶ���;��������ʵ�������ֶ���
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
			
			UnionFieldInfo uFieldInfo = new UnionFieldInfo();
			uFieldInfo.setUnionFieldInfo(str);
			vFieldInfo.add(uFieldInfo);
			
			strBuf.delete(0,pos+1);
			pos = strBuf.indexOf("&");
		}
	}

	
	//�����ǰ������������ԷֺŽ��зָ�
	public void setSubIndexInfo(String subIndexInfo){
		if(vSubIndexInfo.size() > 0)
			vSubIndexInfo.clear();
		
		AllIndexsInfo indexsInfo = AllIndexsInfo.getInstance(); 
		
		StringBuffer strBuf = new StringBuffer(subIndexInfo);
		strBuf.append(";");
		
		int pos = strBuf.indexOf(";");
		while(pos != -1){
			String str = strBuf.substring(0,pos);
			if(str.length() == 0)
				break;
			
			IndexInfo info = indexsInfo.getIndexInfoFromDB(str);
			if(info != null)
				vSubIndexInfo.add(info);
			
			strBuf.delete(0,pos+1);
			pos = strBuf.indexOf(";");
		}
	}
	
	//ͨ�������ֶ��Լ���������Ϣ����ȡ����Ӧ�ķ�������ʵ���ֶ���Ϣ
	public FieldInfo getRealFieldInfo(String fieldName,String indexName){
		for(int i=0; i<vFieldInfo.size(); i++){
			UnionFieldInfo fieldInfo = (UnionFieldInfo)vFieldInfo.get(i);
			
			if(!fieldInfo.fieldName.equals(fieldName))
				continue;
			
			return fieldInfo.getIndexFieldInfo(indexName);
		}
		
		return null;
	}
	
	public String fieldInfo2String(){		
		StringBuffer buf = new StringBuffer();
		for(int i=0; i<vFieldInfo.size(); i++){
			UnionFieldInfo fieldInfo = (UnionFieldInfo)vFieldInfo.get(i);
			buf.append(fieldInfo.fieldName);
			buf.append(":");
		
			int size = fieldInfo.vSubUnionFieldInfo.size();
			for(int j=0; j<size; j++){
				SubUnionFieldInfo subFieldInfo = (SubUnionFieldInfo)fieldInfo.vSubUnionFieldInfo.get(j);
				buf.append(subFieldInfo.indexName);
				buf.append(",");
				
				if(subFieldInfo.fieldInfo == null)
					buf.append("");
				else
					buf.append(subFieldInfo.fieldInfo.fieldName);
					
				
				if(j != size-1)
					buf.append(";");
			}
			
			if(i != vFieldInfo.size()-1)
				buf.append("&");
		}
		
		return buf.toString();
	}
	
	public String indexInfo2String(){		
		StringBuffer buf = new StringBuffer(); 
		for(int i=0; i<vSubIndexInfo.size(); i++){
			IndexInfo info = (IndexInfo)vSubIndexInfo.get(i);
			buf.append(info.indexName);
			if(i != vSubIndexInfo.size()-1)
				buf.append(";");
		}
		
		return buf.toString();
	}
}
