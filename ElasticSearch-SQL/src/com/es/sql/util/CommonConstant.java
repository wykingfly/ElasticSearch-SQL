package com.es.sql.util;

public class CommonConstant {

	public enum Operator{
		EQ,//=
		NOTIN,//not in
		IN,//in
		LIKE,//like
		NE,//!=
		LT,//<
		LTE,//<=
		GT,//>
		GTE,//>=
		RANGE
	}
	
	public enum DataType{
		INT,
		STRING,
		DOUBLE,
		BOOLEAN
	}
	
}
