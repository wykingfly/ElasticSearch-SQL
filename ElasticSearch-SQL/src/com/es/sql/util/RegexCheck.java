package com.es.sql.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.es.sql.util.CommonConstant.DataType;

/**
 * 正则验证
 * @author wangyong
 *
 */
public class RegexCheck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String str = "true";
		System.out.println(getDataType(str));
	}

	
	// 整数
	public static boolean isNumber(String str) {
		Pattern pattern = Pattern.compile("0|[1-9][0-9]*");
		Matcher match = pattern.matcher(str);
		if (match.matches() == false) {
			return false;
		} else {
			return true;
		}
	}

	//小数
	public static boolean isDouble(String str) {
		Pattern pattern = Pattern.compile("[0-9]+.[0-9]{1,6}");
		Matcher match = pattern.matcher(str);
		if (match.matches() == false) {
			return false;
		} else {
			return true;
		}
	}
	public static boolean isBoolean(String str) {
		Pattern pattern = Pattern.compile("(true|false)");
		Matcher match = pattern.matcher(str);
		if (match.matches() == false) {
			return false;
		} else {
			return true;
		}
	}
	//字符
	public static boolean isString(String str) {
		if(str.startsWith("'") ||str.startsWith("\"")){
			return true;
		}else{
			return false;
		}
	}
	
	public static DataType getDataType(String str){
		if(isString(str)){
			return DataType.STRING;
		}else if(isNumber(str)){
			return DataType.INT;
		}else if(isDouble(str)){
			return DataType.DOUBLE;
		}else if(isBoolean(str)){
			return DataType.BOOLEAN;
		}else{
			return DataType.STRING;
		}
	}
}
