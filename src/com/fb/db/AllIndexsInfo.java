package com.fb.db;

import java.util.Vector;

import com.fb.common.util.CommonUtil;

public class AllIndexsInfo {
	
	private static AllIndexsInfo ins = null;
	
	private Vector vIndexInfo = new Vector();
	
	private Vector vUnionIndexInfo = new Vector();
	
	private AllIndexsInfo(){loadIndexInfoFromDB();}
	
	public static AllIndexsInfo getInstance(){
		if(ins == null)
			ins = new AllIndexsInfo();
		
		return ins;
	}
	
	//将数据库中的所有索引信息加载到内存中
	public boolean loadIndexInfoFromDB(){
		
		vIndexInfo.clear();
		
		try {
			CommonUtil.getIndexInfoFromDB(vIndexInfo);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}
	
	
//	将数据库中的所有索引信息加载到内存中
	public boolean loadUnionIndexInfoFromDB(){
		
		vUnionIndexInfo.clear();
		
		try {
			CommonUtil.getUnionIndexInfoFromDB(vUnionIndexInfo);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}
	
	
	public void deleteIndexInfo(IndexInfo info){
		for(int i=0; i<vIndexInfo.size(); i++){
			IndexInfo info2 = (IndexInfo)vIndexInfo.get(i);
			
			if(info2.indexID == info.indexID){
				vIndexInfo.elementAt(i);
				break;
			}
		}
	}
	
	public void addIndexInfo(IndexInfo info){
		if(info == null)
			return;
		
		vIndexInfo.add(info);
	}
	
	public IndexInfo getIndexInfoFromDB(int nIndexID){
			
		for(int i=0; i<vIndexInfo.size(); i++){
			IndexInfo info = (IndexInfo)vIndexInfo.get(i);
			if(info.indexID == nIndexID)
				return info;
		}
		
		return null;
	}
	
	public UnionIndexInfo getUnionIndexInfoFromDB(int nIndexID){
		
		for(int i=0; i<vUnionIndexInfo.size(); i++){
			UnionIndexInfo info = (UnionIndexInfo)vUnionIndexInfo.get(i);
			if(info.indexID == nIndexID)
				return info;
		}
		
		return null;
	}
	
	public UnionIndexInfo getUnionIndexInfoFromDB(String indexName){
		for(int i=0; i<vUnionIndexInfo.size(); i++){
			UnionIndexInfo info = (UnionIndexInfo)vUnionIndexInfo.get(i);
			if(info.indexName.equals(indexName))
				return info;
		}
		
		return null;
	}
	
	public IndexInfo getIndexInfoFromDB(String indexName){
        System.out.println("required indexName is:" + indexName);
        for(int i=0; i<vIndexInfo.size(); i++){
			IndexInfo info = (IndexInfo)vIndexInfo.get(i);
            System.out.println("info->" + info.indexName + " loaded");
			if(info.indexName.equals(indexName))
				return info;
		}
		
		return null;
	}
	
	public int getIndexCount(){
		return vIndexInfo.size();
	}
	
	public IndexInfo getIndexInfo(int nIndex){
		
		if(nIndex <0 || nIndex >= vIndexInfo.size())
			return null;
		
		IndexInfo info = (IndexInfo)vIndexInfo.get(nIndex);
		
		return info;
	}
	
	public int getIndexType(String indexName){
		
		for(int i=0; i<vIndexInfo.size(); i++){
			IndexInfo info = (IndexInfo)vIndexInfo.get(i);
			if(info.indexName.equals(indexName))
				return 0;
		}
		
		for(int i=0; i<vUnionIndexInfo.size(); i++){
			UnionIndexInfo info = (UnionIndexInfo)vUnionIndexInfo.get(i);
			if(info.indexName.equals(indexName))
				return 1;
		}
		
		return -1;
	}
	
}
