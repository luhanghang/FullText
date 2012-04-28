package com.fb.db;

public class FieldInfo {
	
	public String fieldName;
	
	public int indexType;  //索引类型 0表示唯一标识 1标识进行索引 2表示不进行索引但保存 3: 进行索引,但不保存

}
