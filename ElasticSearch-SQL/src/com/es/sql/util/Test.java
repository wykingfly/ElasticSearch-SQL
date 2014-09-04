package com.es.sql.util;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;

/**
 * Test Class
 * @author wangyong
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//System.out.println(isIdQuery("id=9BnH0MToTvWMHwikTb-uhA"));
		//test();
		/*List<String> list = splitGroupBy();
		for(String str:list){
			System.out.println(str);
		}*/
		//rangeGroupBySplit("age[*-20|20-25|25-30|30-35|35-40|40-*]");
		splitIndex("[index.2014]");
	}

	private static void splitIndex(String index){
		if(index.startsWith("[") && index.indexOf("]")>0){
			String x = index.substring(1,index.indexOf("]"));
			System.out.println(x);
			if(x.length()+2==index.length()){
				
			}else{
				String y = index.substring(index.lastIndexOf(".")+1);
				System.out.println(y);
			}
			
			
		}
	}
	
	private static void rangeGroupBySplit(String g){
		String field = g.substring(0,g.indexOf("[")).trim();
		String value = g.substring(g.indexOf("[")+1,g.indexOf("]"));
		String[] arr = value.split("\\|");
		
		System.out.println("--------field="+field);
		if(arr.length==1){
			System.out.println("---------arr[0]"+arr[0]);
			String[] tmp = arr[0].split("-");
			if(tmp.length==2){
				if("*".equals(tmp[0].trim())){
					
					if(isIntegerOrDouble(tmp[1])){//必须是数字或者小数
						AggregationBuilders.range(field).field(field).addUnboundedTo(arr[0].trim(),Double.parseDouble(tmp[1]));
					}
				}else if("*".equals(tmp[1].trim())){
					if(isIntegerOrDouble(tmp[0])){//必须是数字或者小数
						AggregationBuilders.range(field).field(field).addUnboundedFrom(arr[0].trim(),Double.parseDouble(tmp[0]));
					}
				}
			}else{
				//格式错误
			}
		}else if(arr.length>1){
			RangeBuilder rg = AggregationBuilders.range(field).field(field);
			for(int i=0;i<arr.length;i++){
				
				System.out.println("---------arr["+i+"]"+arr[i]);
				
				String[] tmp = arr[i].split("-");
				if(tmp.length==2){
					if(i==0 || i==arr.length-1){//只有第一个和最后一个存在开口
						if("*".equals(tmp[0].trim())){
							
							if(isIntegerOrDouble(tmp[1])){//必须是数字或者小数
								
								rg.addUnboundedTo(arr[i].trim(),Double.parseDouble(tmp[1]));
							}
						}else if("*".equals(tmp[1].trim())){
							if(isIntegerOrDouble(tmp[0])){//必须是数字或者小数
								
								rg.addUnboundedFrom(arr[i].trim(),Double.parseDouble(tmp[0]));
							}
						}
					}else{
						if(isIntegerOrDouble(tmp[0]) && isIntegerOrDouble(tmp[1])){//-分割两端的数据都为数字类型
							rg.addRange(arr[i], Double.parseDouble(tmp[0]), Double.parseDouble(tmp[1]));
						}
					}
				}else{
					//格式错误
				}
			}
			
		}
	}
	/**
	 * 整数或者小数
	 * @param str
	 * @return
	 */
	private static boolean isIntegerOrDouble(String str)
    {
        java.util.regex.Pattern pattern=java.util.regex.Pattern.compile("[0-9]+\\.{0,1}[0-9]{0,2}");
        java.util.regex.Matcher match=pattern.matcher(str);
        if(match.matches()==false)
        {
             return false;
        }
        else
        {
             return true;
        }
    }
	
	private static List<String> splitGroupBy(){
		String str = "(city,age[*-20|20-25|25-30|30-35|35-40|40-*],state),(city,state),(age,account_number)";
		int num =0;
		boolean start = false;
		List<String> list = new ArrayList<String>();
		StringBuffer stringBuffer = new StringBuffer();
		for(int i=0;i<str.length();i++){
			char tmp = str.charAt(i);
			if(tmp=='('){
				num++;
				start = true;
			}else if(tmp==')'){
				num--;
				list.add(stringBuffer.toString());
				stringBuffer = new StringBuffer();
				
				start = false;
			}else{
				if(start){
					stringBuffer.append(tmp);
				}
			}
		}
		return list;
	}
	
	private static void test(){
		String[] indexArr = new String[]{"index.account","index1.account1"};
		
		StringBuffer indexBuffer = new StringBuffer();
		StringBuffer typeBuffer = new StringBuffer();
		for(String str:indexArr){
			String[] x = str.split("\\.");
			if(x.length==1){
				indexBuffer.append(x[0]).append("\001");
			}else{
				indexBuffer.append(x[0]).append("\001");
				typeBuffer.append(x[1]).append("\001");
			}
		}
		String[] index = indexBuffer.toString().substring(0,(indexBuffer.toString().length()-"\001".length())).split("\001");
		String[] type = typeBuffer.toString().substring(0,(typeBuffer.toString().length()-"\001".length())).split("\001");
		
		System.out.println(index.toString());
		System.out.println(type.toString());
	}
	
	/**
	 * 判断是否是id查询
	 * @param whereCondition
	 * @return
	 */
	private static int isIdQuery(String whereCondition){
		int flag =0;//0:多条件查询 1：id查询 2：单一条件查询
		try{
			if(whereCondition.indexOf(" and ")>0 || whereCondition.indexOf(" or ")>0){
				flag = 0;
				if(whereCondition.indexOf(" between ")>0 && whereCondition.indexOf(" or ")<0){
					if(whereCondition.indexOf(" and ") == whereCondition.lastIndexOf(" and ")){//只有一个and 没有or 有between
						flag = 2;
					}
				}
			}else if(whereCondition.startsWith("id") && 
					(whereCondition.indexOf("=")>0 && 
							!((whereCondition.indexOf("in(")>0||whereCondition.indexOf("in (")>0) || 
									(whereCondition.indexOf("not in(")>0||whereCondition.indexOf("not in (")>0)))){
				flag = 1;
			}else{
				flag = 2;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return flag;
	}
	
}
