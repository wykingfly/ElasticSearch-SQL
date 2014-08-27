package com.es.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import com.es.sql.parse.QuerySqlParser;
import com.es.sql.util.CommonConstant.DataType;
import com.es.sql.util.CommonConstant.Operator;
import com.es.sql.util.CommonUtils;
import com.es.sql.util.RegexCheck;

public class QueryFilterHandler {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sql = "select ds,serverid,count(distinc userid) as count from index.segment where (appid='10xxxx' and what='item' and (ds between '2014-08-12' and '2014-08-13') and tz='+8') or (axt in(1,2,3) and a='t') or is_admin=1 group by ds,serverid order by count desc limit 0,100";
		//String where = "appid = 'x' and what='item' and (ds between 1 and 2) and (a>10 and a<20) and b not in(3,54,10) and c like '%x'";
		//String where = "(a=x and b=y) or (c=z)";
		//String where = "a=x and b = y or c=z";//非严格语句，不处理
		//String where = "a=x or b=y or (c=z and d=t)";
		QuerySqlParser qsp = new QuerySqlParser(sql);
		
		getFilterBuilder(qsp);
	}
	
	public static FilterBuilder getFilterBuilder(QuerySqlParser qsp){
		FilterBuilder filterBuilder = null;
		try{
			String where = qsp.getWhereCondition();//where条件
			if(StringUtils.isEmpty(where)){
				return null;
			}
			Map<String, List<String>> map = WhereConditionHandler.handlerWhereCondition(where);
			int split1 = 1;
			List<FilterBuilder> list1 = null;
			Map<String,List<FilterBuilder>> map2 = new HashMap<String,List<FilterBuilder>>();
			for(Map.Entry<String, List<String>> entry : map.entrySet()){
				String key = entry.getKey();
				if(key.startsWith("1_")){//第一层
					split1 = getSplit(key);
					list1 = getLevelOneFilterBuilder(entry.getValue());
				}else if(key.startsWith("2_")){//第二层
					map2.put(key,getLevelTwoFilterBuilder(entry.getValue()));
				}
			}
			List<FilterBuilder> list2 = new ArrayList<FilterBuilder>();
			if(map2!=null && map2.size()>0){
				for(Map.Entry<String, List<FilterBuilder>> entry : map2.entrySet()){
					String key = entry.getKey();
					List<FilterBuilder> tmpList = entry.getValue();
					FilterBuilder[] filters = new FilterBuilder[tmpList.size()];
					for(int j=0;j<tmpList.size();j++){
						filters[j] = tmpList.get(j);
					}
					int split2 = getSplit(key);
					if(split2==1){
						list2.add(FilterBuilders.andFilter(filters));
					}else if(split2==2){
						list2.add(FilterBuilders.orFilter(filters));
					}
				}
			}
			
			if(list1!=null && list1.size()>0){
				int length = list1.size();
				if(list2!=null && list2.size()>0){
					length+=list2.size();
				}
				FilterBuilder[] filters = new FilterBuilder[length];
				for(int i=0;i<list1.size();i++){
					filters[i] = list1.get(i);
				}
				if(list2!=null && list2.size()>0){
					for(int j = 0;j<list2.size();j++){
						filters[list1.size()+j] =list2.get(j); 
					}
				}
				if(split1 == 1){
					filterBuilder = FilterBuilders.andFilter(filters);
				}else if(split1==2){
					filterBuilder = FilterBuilders.orFilter(filters);
				}else if(split1==0){
					filterBuilder = filters[0];
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return filterBuilder;
	}

	/**
	 * 二级查询处理
	 * @param list
	 * @return
	 */
	private static List<FilterBuilder> getLevelTwoFilterBuilder(List<String> list){
		List<FilterBuilder> fbList = new ArrayList<FilterBuilder>();
		try{
			if(list!=null && list.size()>0){
				for(String str:list){
					fbList.add(getFilterBuilder(str));
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return fbList;
	}
	
	/**
	 * 一级查询处理
	 * @param list
	 * @param split
	 * @return
	 */
	private static List<FilterBuilder> getLevelOneFilterBuilder(List<String> list){
		List<FilterBuilder> fbList = new ArrayList<FilterBuilder>();
		try{
			if(list!=null && list.size()>0){
				for(String str:list){
					fbList.add(getFilterBuilder(str));
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return fbList;
	}
	
	/**
	 * 获取一级处理的每一个filterBuilder
	 * @param condition
	 * @return
	 */
	private static FilterBuilder getFilterBuilder(String condition){
		FilterBuilder filterBuilder = null;
		String str = condition.trim();
		try{
			if((str.startsWith("(") && str.endsWith(")"))||(str.indexOf("in(")>0 || str.indexOf("in (")>0)){//括弧配对完整,存在二级处理条件的可能性
				filterBuilder = analysisComplexWhereConditionFilterBuilder(str);
			}else{
				filterBuilder = analysisSingleWhereConditionFilterBuilder(str.trim());
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return filterBuilder;
	}
	
	/**
	 * 分析带有括弧的复杂表达式
	 * @param str
	 * @return
	 */
	private static FilterBuilder analysisComplexWhereConditionFilterBuilder(String str){
		FilterBuilder filterBuilder = null;
		try{
			String field = null;
			String[] values = null;
			Operator operator = Operator.EQ;
			DataType dataType = DataType.STRING;
			
			if((str.indexOf("in(")>0 || str.indexOf("in (")>0 )&& str.indexOf(" and ")<0 && str.indexOf(" or ")<0){//in 或者 not in ，不满足二级处理条件
				
				Map<DataType, String[]> map = CommonUtils.getValues(str, 1);
				for(Map.Entry<DataType, String[]> entry : map.entrySet()){
					dataType = entry.getKey();
					values = entry.getValue();
					System.out.println(values+"------------------------");
				}
				
				int end = str.indexOf("in(")>0?str.indexOf("in("):str.indexOf("in (");
				field = str.substring(0,end).trim();
				
				operator = Operator.IN;
				
				if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
					filterBuilder = createFilterBuilder(field, operator, dataType,false,false, values);
				}
				
			}else if((str.indexOf("not in(")>0 || str.indexOf("not in (")>0 )&& str.indexOf(" and ")<0 && str.indexOf(" or ")<0){//in 或者 not in ，不满足二级处理条件
				
				Map<DataType, String[]> map = CommonUtils.getValues(str, 1);
				for(Map.Entry<DataType, String[]> entry : map.entrySet()){
					dataType = entry.getKey();
					values = entry.getValue();
					System.out.println(values+"------------------------");
				}
				
				int end = str.indexOf("not in");
				field = str.substring(0,end).trim();
				
				operator = Operator.NOTIN;
				
				if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
					filterBuilder = createFilterBuilder(field, operator, dataType,false,false, values);
				}
				
			}else if(str.indexOf(" between ")>0 && str.indexOf(" and ")>0 && (str.lastIndexOf(" and ")==str.indexOf(" and "))){//between and
				
				Map<DataType, String[]> map = CommonUtils.getValues(str, 2);
				for(Map.Entry<DataType, String[]> entry : map.entrySet()){
					dataType = entry.getKey();
					values = entry.getValue();
					System.out.println(values+"------------------------");
				}
				
				int end = str.indexOf(" between ");
				field = str.substring(str.indexOf("(")+1,end).trim();
				
				operator = Operator.RANGE;
				
				if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
					filterBuilder = createFilterBuilder(field, operator, dataType,true,true, values);
				}
				
			}else if(str.indexOf(" like ")>0 && str.indexOf(" and ")<0 && str.indexOf(" or ")<0){//只有like一条语句
				str = str.trim().substring(1,str.trim().length()-1).trim();
				String[] arr = str.split(" like ");
				values = new String[1];
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = DataType.STRING;
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length());
						if(values[0].startsWith("%")){
							values[0] = "*"+values[0].substring(1,values[0].length());
						}
						if(values[0].endsWith("%")){
							values[0] = values[0].substring(0,values[0].length()-1)+"*";
						}
					}
				}
				
				int end = str.indexOf(" like ");
				field = str.substring(0,end).trim();
				
				operator = Operator.LIKE;
				
				if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
					filterBuilder = createFilterBuilder(field, operator, dataType,false,false, values);
				}
				
			}else {//注定是and组成的range算法
				if(str.indexOf(" and ")>0){//包含and语句,且只有1个and,对应的关于一个field的range处理
					
					Map<DataType, String[]> map = CommonUtils.getValues(str, 3);
					for(Map.Entry<DataType, String[]> entry : map.entrySet()){
						dataType = entry.getKey();
						values = entry.getValue();
						System.out.println(values+"------------------------");
					}
					
					operator = Operator.RANGE;
					
					String xString = CommonUtils.getFieldIncludeLowerUpper(str);
					String[] arr  = xString.split("_");
					boolean isL = Integer.parseInt(arr[1])==1?true:false;
					boolean isU = Integer.parseInt(arr[2])==1?true:false;
					field = arr[0].trim();
					if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
						filterBuilder = createFilterBuilder(field, operator, dataType,isL,isU, values);
					}
				}
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return filterBuilder;
	}
	
	
	/**
	 * 普通where condition 表达式分析处理
	 * @param str
	 * @return
	 */
	private static FilterBuilder analysisSingleWhereConditionFilterBuilder(String str){
		FilterBuilder filterBuilder = null;
		try{
			String field = null;
			String[] values = new String[1];
			Operator operator = Operator.EQ;
			DataType dataType = DataType.STRING;
			
			if(str.indexOf(">=")>0){
				
				String[] arr = str.split(">=");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.GTE;
				
			}else if(str.indexOf("<=")>0){
				
				String[] arr = str.split("<=");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.LTE;
				
			}else if(str.indexOf("!=")>0){
				
				String[] arr = str.split("!=");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.NE;
				
			}else if(str.indexOf(" like ")>0){
				
				String[] arr = str.split(" like ");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = DataType.STRING;
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
						if(values[0].startsWith("%")){
							values[0] = "*"+values[0].substring(1,values[0].length());
							
						}
						if(values[0].endsWith("%")){
							values[0] = values[0].substring(0,values[0].length()-1)+"*";
						}
					}
				}
				operator = Operator.LIKE;
				
			}else if(str.indexOf("=")>0){
				
				String[] arr = str.split("=");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.EQ;
				
			}else if(str.indexOf(">")>0){
				
				String[] arr = str.split(">");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.GT;
				
			}else if(str.indexOf("<")>0){
				
				String[] arr = str.split("<");
				if(arr.length ==2){
					field = arr[0].trim();
					values[0] = arr[1].trim();
					dataType = RegexCheck.getDataType(values[0]);
					if(dataType == DataType.STRING){
						values[0] = values[0].substring(1,values[0].length()-1);
					}
				}
				operator = Operator.LT;
				
			}
			if(StringUtils.isNotEmpty(field) && values!=null && values.length>0){
				filterBuilder = createFilterBuilder(field, operator, dataType,false,false, values);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return filterBuilder;
	}
	
	/**
	 * 构造查询过滤条件
	 * @param field
	 * @param operator
	 * @param dataType
	 * @param values
	 * @return
	 */
	private static FilterBuilder createFilterBuilder(String field,Operator operator,DataType dataType,boolean isIncludeLower,boolean isIncludeUpper,String... values){
		FilterBuilder filterBuilder = null;
		try{
			
			if(operator == Operator.EQ){//等于
				filterBuilder = FilterBuilders.queryFilter(QueryBuilders.queryString(values[0]).field(field));
			}else if(operator == Operator.IN){//in
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.inFilter(field, arr).filterName(field);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.inFilter(field, arr).filterName(field);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.inFilter(field, values).filterName(field);
				}else{
					filterBuilder = FilterBuilders.inFilter(field, values).filterName(field);
				}
				
			}else if(operator == Operator.NE){//!=
				if("null".equals(values[0].toLowerCase())){
					filterBuilder = FilterBuilders.missingFilter(field);
				}else{
					/*if(dataType == DataType.INT){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Integer.parseInt(values[0])));
					}else if(dataType == DataType.DOUBLE){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Double.parseDouble(values[0])));
					}else if(dataType == DataType.BOOLEAN){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Boolean.parseBoolean(values[0])));
					}else{//字符串
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, values[0]));
					}*/
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.queryFilter(QueryBuilders.queryString(values[0]).field(field)));
				}
				
			}else if(operator == Operator.LIKE){
				filterBuilder = FilterBuilders.queryFilter(QueryBuilders.wildcardQuery(field, values[0]));
			}else if(operator == Operator.NOTIN){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values).filterName(field));
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values).filterName(field));
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values).filterName(field));
				}else{
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values).filterName(field));
				}
			}else if(operator == Operator.RANGE){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.rangeFilter(field).filterName(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.rangeFilter(field).filterName(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.rangeFilter(field).filterName(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else{
					filterBuilder = FilterBuilders.rangeFilter(field).filterName(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}
			}else if(operator == Operator.GT){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Integer.parseInt(values[0])).filterName(field);
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Double.parseDouble(values[0])).filterName(field);
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Boolean.parseBoolean(values[0])).filterName(field);
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).gt(values[0]).filterName(field);
				}
			}else if(operator == Operator.GTE){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Integer.parseInt(values[0])).filterName(field);
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Double.parseDouble(values[0])).filterName(field);
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Boolean.parseBoolean(values[0])).filterName(field);
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).gte(values[0]).filterName(field);
				}
			}else if(operator == Operator.LT){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Integer.parseInt(values[0])).filterName(field);
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Double.parseDouble(values[0])).filterName(field);
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Boolean.parseBoolean(values[0])).filterName(field);
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).lt(values[0]).filterName(field);
				}
			}else if(operator == Operator.LTE){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Integer.parseInt(values[0])).filterName(field);
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Double.parseDouble(values[0])).filterName(field);
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Boolean.parseBoolean(values[0])).filterName(field);
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).lte(values[0]).filterName(field);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return filterBuilder;
	}
	/**
	 * 构造查询过滤条件
	 * @param field
	 * @param operator
	 * @param dataType
	 * @param values
	 * @return
	 */
	private static FilterBuilder createFilterBuilder2(String field,Operator operator,DataType dataType,boolean isIncludeLower,boolean isIncludeUpper,String... values){
		FilterBuilder filterBuilder = null;
		try{
			
			if(operator == Operator.EQ){//等于
				
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.termFilter(field, Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.termFilter(field, Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.termFilter(field, Boolean.parseBoolean(values[0]));
				}else{//字符串
					filterBuilder = FilterBuilders.termFilter(field, values[0]);
				}
			}else if(operator == Operator.IN){//in
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.inFilter(field, arr);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.inFilter(field, arr);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.inFilter(field, values);
				}else{
					filterBuilder = FilterBuilders.inFilter(field, values);
				}
				
			}else if(operator == Operator.NE){//!=
				if("null".equals(values[0].toLowerCase())){
					filterBuilder = FilterBuilders.missingFilter(field);
				}else{
					if(dataType == DataType.INT){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Integer.parseInt(values[0])));
					}else if(dataType == DataType.DOUBLE){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Double.parseDouble(values[0])));
					}else if(dataType == DataType.BOOLEAN){
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, Boolean.parseBoolean(values[0])));
					}else{//字符串
						filterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(field, values[0]));
					}
				}
				
			}else if(operator == Operator.LIKE){
				
			}else if(operator == Operator.NOTIN){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values));
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values));
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values));
				}else{
					filterBuilder = FilterBuilders.notFilter(FilterBuilders.inFilter(field, values));
				}
			}else if(operator == Operator.RANGE){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					filterBuilder = FilterBuilders.rangeFilter(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					filterBuilder = FilterBuilders.rangeFilter(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					filterBuilder = FilterBuilders.rangeFilter(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else{
					filterBuilder = FilterBuilders.rangeFilter(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}
			}else if(operator == Operator.GT){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).gt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).gt(values[0]);
				}
			}else if(operator == Operator.GTE){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).gte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).gte(values[0]);
				}
			}else if(operator == Operator.LT){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).lt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).lt(values[0]);
				}
			}else if(operator == Operator.LTE){
				if(dataType == DataType.INT){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					filterBuilder = FilterBuilders.rangeFilter(field).lte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					filterBuilder = FilterBuilders.rangeFilter(field).lte(values[0]);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return filterBuilder;
	}
	private static Object caseType(DataType dataType,String value){
		if(dataType == DataType.INT){
			return Integer.parseInt(value);
		}else if(dataType == DataType.DOUBLE){
			return Double.parseDouble(value);
		}else if(dataType == DataType.BOOLEAN){
			return Boolean.parseBoolean(value);
		}else{
			return value;
		}
	}
	/**
	 * 获取连接字符 and or
	 * @param key
	 * @return
	 */
	private static int getSplit(String key){
		int split = 0;
		String[] arr = key.split("_");
		if("1".equals(arr[1])){
			split = 1;
		}else if("2".equals(arr[1])){
			split = 2;
		}
		return split;
	}
}
