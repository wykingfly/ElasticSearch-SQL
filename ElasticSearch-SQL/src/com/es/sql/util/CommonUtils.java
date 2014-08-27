package com.es.sql.util;

import java.util.HashMap;
import java.util.Map;

import com.es.sql.util.CommonConstant.DataType;

/**
 * 工具类
 * @author wangyong
 *
 */
public class CommonUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//(ds between 1 and 2)
		//(a>10 and a<20)
		//b not in(3,54,10)
		//Map<DataType, String[]> map = getValues("(atx>=10 and atx<20)", 3);
		//Map<DataType, String[]> map = getValues("(atx between '10' and '20')", 2);
		/*Map<DataType, String[]> map = getValues("atx in('1','2','3')", 1);
		for(Map.Entry<DataType, String[]> entry : map.entrySet()){
			System.out.println(entry.getKey());
			System.out.println(entry.getValue()+"------------------------");
			for(String x:entry.getValue()){
				System.out.println(x);
			}
		}*/
		System.out.println(getFieldIncludeLowerUpper("(atx>=10 and atx<20)"));
	}
	
	public static Map<DataType, String[]> getValues(String str,int type){
		Map<DataType, String[]> map = new HashMap<DataType, String[]>();
		String[] values = null;
		if(type == 1){//in
			int start = str.indexOf("in(");
			int start1 = str.indexOf("in (");
			String tmp = null;
			if(start>start1){
				tmp = str.substring(start+3,str.length());
			}else{
				tmp = str.substring(start1+4,str.length());
			}
			String[] arr = tmp.split(",");
			arr[arr.length-1]  = arr[arr.length-1].substring(0,arr[arr.length-1].length()-1);
			DataType dataType = RegexCheck.getDataType(arr[0]);
			values = new String[arr.length];
			if(dataType==DataType.STRING){
				int i = 0;
				for(String a:arr){
					String b = a.trim();
					b = b.substring(1,b.length()-1);
					values[i++] = b;
				}
			}else{
				values = arr;
			}
			map.put(dataType, values);
		}else if(type == 2){//between and
			String[] arr = str.split(" and ");
				if(arr.length==2){
					arr[1] = arr[1].substring(0,arr[1].length()-1).trim();
					DataType dataType = RegexCheck.getDataType(arr[1].trim());
					values = new String[arr.length];
					String tmp = arr[0].split("between")[1].trim();
					if(dataType==DataType.STRING){
						values[0] = tmp.substring(1,tmp.length()-1);
						values[1] = arr[1].trim().substring(1,arr[1].trim().length()-1);
					}else{
						values[0] = tmp;
						values[1] = arr[1].trim();
					}
				map.put(dataType, values);
			}
		}else if(type == 3){// a>10 and a <20
			String[] arr = str.split(" and ");
			values = new String[arr.length];
			DataType dataType = DataType.STRING;
			if(arr.length == 2){
				int i = 0;
				for(String t:arr){
					if(t.indexOf(">=")>0){
						t = t.substring(t.indexOf(">=")+2).trim();
					}else if(t.indexOf("<=")>0){
						t = t.substring(t.indexOf("<=")+2).trim();
					}else if(t.indexOf(">")>0){
						t = t.substring(t.indexOf(">")+1).trim();
					}else if(t.indexOf("<")>0){
						t = t.substring(t.indexOf("<")+1).trim();
					}
					if(t.endsWith(")")){
						t = t.substring(0,t.length()-1);
					}
					dataType = RegexCheck.getDataType(t);
					if(dataType == DataType.STRING){
						t = t.substring(1,t.length());
					}
					values[i++] = t;
				}
			}
			map.put(dataType, values);
		}
		
		return map;
		
	}

	
	public static String getFieldIncludeLowerUpper(String str){
		StringBuffer sBuffer = new StringBuffer();
		String[] arr = str.split(" and ");
		int lower = 0;
		int upper = 0;
		String field = "";
		if(arr.length == 2){
			for(String t:arr){
				if(t.indexOf(">=")>0){
					lower = 1;
					t = t.substring(0,t.indexOf(">=")).trim();
				}else if(t.indexOf("<=")>0){
					upper = 1;
					t = t.substring(0,t.indexOf("<=")).trim();
				}else if(t.indexOf(">")>0){
					t = t.substring(0,t.indexOf(">")).trim();
				}else if(t.indexOf("<")>0){
					t = t.substring(0,t.indexOf("<")).trim();
				}
				
				field = t;
				
			}
		}
		
		return sBuffer.append(field).append("_").append(lower).append("_").append(upper).toString();
	}
	
	public static String[] trimStrings(String[] args){
		if(args==null || args.length<=0){
			return args;
		}
		String[] tmp = new String[args.length];
		for(int i = 0 ;i<args.length;i++){
			tmp[i] = args[i].trim();
		}
		return tmp;
	}
	
}
