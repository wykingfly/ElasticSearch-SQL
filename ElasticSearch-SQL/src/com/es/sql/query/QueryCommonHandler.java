package com.es.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Query;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.es.sql.parse.QuerySqlParser;
import com.es.sql.util.CommonConstant.DataType;
import com.es.sql.util.CommonConstant.Operator;
import com.es.sql.util.CommonUtils;
import com.es.sql.util.RegexCheck;

/**
 * 普通query查询处理
 * @author wangyong
 *
 */
public class QueryCommonHandler {

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sql = "select ds,serverid from index.segment where (appid='10xxxx' and what='item' and (ds between '2014-08-12' and '2014-08-13') and tz='+8' and tt!='xxx') or (axt in(1,2,3) and a='t') or is_admin=1 or (person like '%admin') order by count desc limit 0,100";
		//String where = "appid = 'x' and what='item' and (ds between 1 and 2) and (a>10 and a<20) and b not in(3,54,10) and c like '%x'";
		//String where = "(a=x and b=y) or (c=z)";
		//String where = "a=x and b = y or c=z";//非严格语句，不处理
		//String where = "a=x or b=y or (c=z and d=t)";
		QuerySqlParser qsp = new QuerySqlParser(sql);
		
		QueryBuilder queryBuilder = getQueryBuilder(qsp);
		
		System.out.println("ok");
		
	}
	
	public static QueryBuilder getQueryBuilder(QuerySqlParser qsp){
		QueryBuilder queryBuilder = null;
		try{
			String where = qsp.getWhereCondition();//where条件
			if(StringUtils.isEmpty(where)){
				return null;
			}
			Map<String, List<String>> map = WhereConditionHandler.handlerWhereCondition(where);
			int split1 = 1;
			Map<QueryBuilder, Operator> map1 = null;
			
			Map<String,Map<QueryBuilder, Operator>> map2 = new HashMap<String,Map<QueryBuilder, Operator>>();
			
			for(Map.Entry<String, List<String>> entry : map.entrySet()){
				String key = entry.getKey();
				if(key.startsWith("1_")){//第一层
					split1 = getSplit(key);
					map1 = getLevelOneQueryBuilder(entry.getValue());
				}else if(key.startsWith("2_")){//第二层
					map2.put(key,getLevelTwoQueryBuilder(entry.getValue()));
				}
			}
			
			List<QueryBuilder> list2 = new ArrayList<QueryBuilder>();
			if(map2!=null && map2.size()>0){
				for(Map.Entry<String, Map<QueryBuilder, Operator>> entry : map2.entrySet()){
					String key = entry.getKey();
					Map<QueryBuilder, Operator> tmpMap = entry.getValue();
					int split2 = getSplit(key);
					BoolQueryBuilder bqb = null;
					int  num = 0;
					for(Map.Entry<QueryBuilder, Operator> entry2 :tmpMap.entrySet()){
						QueryBuilder qBuilder = entry2.getKey();
						Operator operator = entry2.getValue();
						
						if(split2==1){//must and
							if(num ==0){
								if(operator == Operator.NOTIN || operator == Operator.NE){
									bqb = QueryBuilders.boolQuery().mustNot(qBuilder);
								}else{
									bqb = QueryBuilders.boolQuery().must(qBuilder);
								}
							}else{
								if(operator == Operator.NOTIN || operator == Operator.NE){
									bqb.mustNot(qBuilder);
								}else{
									bqb.must(qBuilder);
								}
							}
						}else if(split2==2){//should or
							if(num ==0){
								if(operator == Operator.NOTIN || operator == Operator.NE){
									bqb = QueryBuilders.boolQuery().mustNot(qBuilder);
								}else{
									bqb = QueryBuilders.boolQuery().should(qBuilder);
								}
							}else{
								if(operator == Operator.NOTIN || operator == Operator.NE){
									bqb.mustNot(qBuilder);
								}else{
									bqb.should(qBuilder);
								}
							}
						}
						num++;
					}
					list2.add(bqb);
				}
			}
			QueryBuilder quBuilder = null;
			if(map1!=null && map1.size()>0){
				
				BoolQueryBuilder bqb = null;
				int  num = 0;
				for(Map.Entry<QueryBuilder, Operator> entry : map1.entrySet()){
					QueryBuilder qBuilder = entry.getKey();
					Operator operator = entry.getValue();
					
					if(split1==1){//must and
						if(num ==0){
							if(operator == Operator.NOTIN || operator == Operator.NE){
								bqb = QueryBuilders.boolQuery().mustNot(qBuilder);
							}else{
								bqb = QueryBuilders.boolQuery().must(qBuilder);
							}
						}else{
							if(operator == Operator.NOTIN || operator == Operator.NE){
								bqb.mustNot(qBuilder);
							}else{
								bqb.must(qBuilder);
							}
						}
					}else if(split1==2){//should or
						if(num ==0){
							if(operator == Operator.NOTIN || operator == Operator.NE){
								bqb = QueryBuilders.boolQuery().mustNot(qBuilder);
							}else{
								bqb = QueryBuilders.boolQuery().should(qBuilder);
							}
						}else{
							if(operator == Operator.NOTIN || operator == Operator.NE){
								bqb.mustNot(qBuilder);
							}else{
								bqb.should(qBuilder);
							}
						}
					}else if(split1==0){
						quBuilder = qBuilder;
						break;
					}
					num++;
				}
				if(list2!=null && list2.size()>0){
					for(int j = 0;j<list2.size();j++){
						if(split1 == 1){
							bqb.must(list2.get(j));
						}else if(split1==2){
							bqb.should(list2.get(j));
						}
					}
				}
				if(split1==0){
					queryBuilder = quBuilder;
				}else{
					queryBuilder = bqb;
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return queryBuilder;
	}

	/**
	 * 二级查询处理
	 * @param list
	 * @return
	 */
	private static Map<QueryBuilder, Operator> getLevelTwoQueryBuilder(List<String> list){
		Map<QueryBuilder, Operator> map = new HashMap<QueryBuilder,Operator>();
		try{
			if(list!=null && list.size()>0){
				for(String str:list){
					Map<QueryBuilder, Operator> maptmp = getFilterBuilder(str);
					for(Map.Entry<QueryBuilder, Operator> entry : maptmp.entrySet()){
						map.put(entry.getKey(), entry.getValue());
					}
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	/**
	 * 一级查询处理
	 * @param list
	 * @param split
	 * @return
	 */
	private static Map<QueryBuilder, Operator> getLevelOneQueryBuilder(List<String> list){
		Map<QueryBuilder, Operator> map = new HashMap<QueryBuilder,Operator>();
		try{
			if(list!=null && list.size()>0){
				for(String str:list){
					Map<QueryBuilder, Operator> maptmp = getFilterBuilder(str);
					for(Map.Entry<QueryBuilder, Operator> entry : maptmp.entrySet()){
						map.put(entry.getKey(), entry.getValue());
					}
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	/**
	 * 获取一级处理的每一个filterBuilder
	 * @param condition
	 * @return
	 */
	public static Map<QueryBuilder, Operator> getFilterBuilder(String condition){
		Map<QueryBuilder, Operator> map = null;
		String str = condition.trim();
		try{
			if((str.startsWith("(") && str.endsWith(")"))||(str.indexOf("in(")>0 || str.indexOf("in (")>0)){//括弧配对完整,存在二级处理条件的可能性
				map = analysisComplexWhereConditionQueryBuilder(str);
			}else{
				map = analysisSingleWhereConditionQueryBuilder(str.trim());
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	/**
	 * 分析带有括弧的复杂表达式
	 * @param str
	 * @return
	 */
	private static Map<QueryBuilder, Operator> analysisComplexWhereConditionQueryBuilder(String str){
		Map<QueryBuilder, Operator> mapqo = new HashMap<QueryBuilder,Operator>();
		QueryBuilder queryBuilder = null;
		Operator operator = Operator.EQ;
		try{
			String field = null;
			String[] values = null;
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
					queryBuilder = createQueryBuilder(field, operator, dataType,false,false, values);
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
					queryBuilder = createQueryBuilder(field, operator, dataType,false,false, values);
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
					queryBuilder = createQueryBuilder(field, operator, dataType,true,true, values);
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
						values[0] = values[0].substring(1,values[0].length()-1);
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
					queryBuilder = createQueryBuilder(field, operator, dataType,false,false, values);
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
						queryBuilder = createQueryBuilder(field, operator, dataType,isL,isU, values);
					}
				}
			}
			mapqo.put(queryBuilder, operator);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return mapqo;
	}
	
	
	/**
	 * 普通where condition 表达式分析处理
	 * @param str
	 * @return
	 */
	private static Map<QueryBuilder, Operator> analysisSingleWhereConditionQueryBuilder(String str){
		Map<QueryBuilder, Operator> map = new HashMap<QueryBuilder,Operator>();
		QueryBuilder queryBuilder = null;
		Operator operator = Operator.EQ;
		try{
			String field = null;
			String[] values = new String[1];
			
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
				queryBuilder = createQueryBuilder(field, operator, dataType,false,false, values);
			}
			map.put(queryBuilder, operator);
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return map;
	}
	
	/**
	 * 构造查询过滤条件
	 * @param field
	 * @param operator
	 * @param dataType
	 * @param values
	 * @return
	 */
	private static QueryBuilder createQueryBuilder(String field,Operator operator,DataType dataType,boolean isIncludeLower,boolean isIncludeUpper,String... values){
		
		QueryBuilder queryBuilder = null;
		try{
			
			if(operator == Operator.EQ){//等于
				
				queryBuilder = QueryBuilders.queryString(values[0]).field(field);
				
			}else if(operator == Operator.IN){//in
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.inQuery(field, values);
				}else{
					queryBuilder = QueryBuilders.inQuery(field, values);
				}
				
			}else if(operator == Operator.NE){//!=
				if("null".equals(values[0].toLowerCase())){
					queryBuilder = QueryBuilders.termQuery(field, values[0]);
				}else{
					
					queryBuilder = QueryBuilders.queryString(values[0]).field(field);
				}
				
			}else if(operator == Operator.LIKE){
				//queryBuilder = QueryBuilders.fuzzyLikeThisFieldQuery(field).likeText(values[0]);
				queryBuilder = QueryBuilders.wildcardQuery(field, values[0]);
				
			}else if(operator == Operator.NOTIN){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.inQuery(field, values);
				}else{
					queryBuilder = QueryBuilders.inQuery(field, values);
				}
				
			}else if(operator == Operator.RANGE){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.rangeQuery(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.rangeQuery(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.rangeQuery(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else{
					queryBuilder = QueryBuilders.rangeQuery(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}
			}else if(operator == Operator.GT){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).gt(values[0]);
				}
			}else if(operator == Operator.GTE){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).gte(values[0]);
				}
			}else if(operator == Operator.LT){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).lt(values[0]);
				}
			}else if(operator == Operator.LTE){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).lte(values[0]);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return queryBuilder;
	}
	
	/**
	 * 构造查询过滤条件
	 * @param field
	 * @param operator
	 * @param dataType
	 * @param values
	 * @return
	 */
	private static QueryBuilder createQueryBuilder2(String field,Operator operator,DataType dataType,boolean isIncludeLower,boolean isIncludeUpper,String... values){
		
		QueryBuilder queryBuilder = null;
		try{
			
			if(operator == Operator.EQ){//等于
				
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.termQuery(field, Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.termQuery(field, Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.termQuery(field, Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.termQuery(field, values[0]);
				}
			}else if(operator == Operator.IN){//in
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.inQuery(field, values);
				}else{
					queryBuilder = QueryBuilders.inQuery(field, values);
				}
				
			}else if(operator == Operator.NE){//!=
				if("null".equals(values[0].toLowerCase())){
					queryBuilder = QueryBuilders.termQuery(field, values[0]);
				}else{
					if(dataType == DataType.INT){
						queryBuilder = QueryBuilders.termQuery(field,  Integer.parseInt(values[0]));
					}else if(dataType == DataType.DOUBLE){
						queryBuilder = QueryBuilders.termQuery(field, Double.parseDouble(values[0]));
					}else if(dataType == DataType.BOOLEAN){
						queryBuilder = QueryBuilders.termQuery(field, Boolean.parseBoolean(values[0]));
					}else{//字符串
						queryBuilder = QueryBuilders.termQuery(field, values[0]);
					}
				}
				
			}else if(operator == Operator.LIKE){
				queryBuilder = QueryBuilders.fuzzyLikeThisFieldQuery(field).likeText(values[0]).maxQueryTerms(1000);
				
			}else if(operator == Operator.NOTIN){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.inQuery(field, arr);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.inQuery(field, values);
				}else{
					queryBuilder = QueryBuilders.inQuery(field, values);
				}
				
			}else if(operator == Operator.RANGE){
				if(dataType == DataType.INT){
					int[] arr = new int[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Integer.parseInt(value);
					}
					queryBuilder = QueryBuilders.rangeQuery(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.DOUBLE){
					double[] arr = new double[values.length];
					int i = 0;
					for(String value:values){
						arr[i++]=Double.parseDouble(value);
					}
					queryBuilder = QueryBuilders.rangeQuery(field).from(arr[0]).to(arr[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else if(dataType == DataType.BOOLEAN){//作为字符串处理
					queryBuilder = QueryBuilders.rangeQuery(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}else{
					queryBuilder = QueryBuilders.rangeQuery(field).from(values[0]).to(values[1]).includeLower(isIncludeLower).includeUpper(isIncludeUpper);
				}
			}else if(operator == Operator.GT){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).gt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).gt(values[0]);
				}
			}else if(operator == Operator.GTE){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).gte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).gte(values[0]);
				}
			}else if(operator == Operator.LT){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).lt(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).lt(values[0]);
				}
			}else if(operator == Operator.LTE){
				if(dataType == DataType.INT){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Integer.parseInt(values[0]));
				}else if(dataType == DataType.DOUBLE){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Double.parseDouble(values[0]));
				}else if(dataType == DataType.BOOLEAN){
					queryBuilder = QueryBuilders.rangeQuery(field).lte(Boolean.parseBoolean(values[0]));
				}else{//字符串
					queryBuilder = QueryBuilders.rangeQuery(field).lte(values[0]);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		return queryBuilder;
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
