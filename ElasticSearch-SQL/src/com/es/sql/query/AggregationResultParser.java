package com.es.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.InternalCardinality;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.stats.InternalStats;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;

import com.es.sql.parse.QuerySqlParser;

/**
 * 
 * @author wangyong
 *
 */
public class AggregationResultParser {

	private enum Operation {
		COUNT, SUM, AVG, MAX, MIN, STATS
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static Map<Integer, Map<String, String>> resultParser3(SearchResponse response,QuerySqlParser qsp){
		Map<Integer,Map<String, String>> resultMap = new HashMap<Integer,Map<String,String>>();
		
		try{
			
			InternalGlobal global =  response.getAggregations().get("all_agg");
			
			String where = qsp.getWhereCondition();//where条件
			InternalFilter filter = null;
			if(StringUtils.isNotEmpty(where)){
				filter = global.getAggregations().get("filter");
			}
			
			String group = qsp.getGroupCondition().trim();//获取group 条件
			
			Map<Integer,Map<Integer, String>> map = new HashMap<Integer,Map<Integer, String>>();
			if (group.startsWith("(")) {// 可能多层嵌套group存在
				//(city,age[*-20|20-25|25-30|30-35|35-40|40-*],state),(city,state),(age,account_number)
				
				List<String> list = splitGroupBy(group);
				for(int i=0;i<list.size();i++){
					Map<Integer, String> map1 = getNameMap(list.get(i).trim());
					map.put(i, map1);
				}
				
			} else{// 普通
				//city,age[*-20|20-25|25-30|30-35|35-40|40-*],state
				//city,state
				Map<Integer, String> map1 = getNameMap(group.trim());
				map.put(0, map1);
			}
			
			String functionName = getFunctionName(qsp.getSelectCol());
			
			if(map.size()==1){//只有一层
				Map<String, String> rMap = getResult(map.get(0),functionName,global,filter);
				resultMap.put(0, rMap);
			}else if(map.size()>1){//多层
				for(Map.Entry<Integer, Map<Integer, String>> entry:map.entrySet()){
					Map<String, String> rMap = getResult(entry.getValue(),functionName,global,filter);
					resultMap.put(entry.getKey(), rMap);
				}
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return resultMap;
	}
	
	private static Map<String, String> getResult(Map<Integer, String> map,String functionName,InternalGlobal global,InternalFilter filter){
		Map<String, String> result = new HashMap<String,String>();
		try{
			InternalSingleBucketAggregation tmp = global;
			if(filter != null){
				tmp = filter;
			}
			String type = "terms";//last type
			List<String> tmpList = new ArrayList<String>();
			
			for(Map.Entry<Integer, String> entry : map.entrySet()){
				
				String value = entry.getValue();
				
				tmpList.add(value);
				
				String[] arr = value.split("\001");
				if(arr.length ==1 ){
					if("datehistogram".equals(arr[0].trim())){
						type = "datehistogram";
						
					}else if("histogram".equals(arr[0].trim())){
						type = "histogram";
					}
				}else if(arr.length>1){
					if("range".equals(arr[0].trim())){
						type = "range";
					}else if("terms".equals(arr[0].trim())){
						type = "terms";
					}
				}
				
			}
			List<String> keyList = new ArrayList<String>();
			keyList = recursiveAllLevel(tmp.getAggregations(),"",keyList,0,tmpList,functionName);
			
			
			for(String key : keyList){
				String[] arr = key.split("\002");
				if(arr.length==2){
					String mapkey = arr[0].substring("\001".length());
					if(StringUtils.isEmpty(functionName)){//count
						result.put(mapkey, arr[1]);
					}else{
						String[] valueArr = arr[1].split("\003");
						if(valueArr.length==2){
							result.put(mapkey, valueArr[1]);//如果查询stats，其结果使用\004分隔,顺序分别是：count、sum、avg、max、min
						}
					}
				}
			}
			
			
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 递归分析数据
	 * @param list
	 * @param value
	 * @return
	 */
	private static List<String> recursiveAllLevel(Aggregations tmp,String value,List<String> list,int level,List<String> nameList,String functionName){
		try{
			String name = nameList.get(level);
			int type = getType(name);
			switch (type) {
				case 1:
					InternalTerms terms = tmp.get(getName(name));
					if(level==nameList.size()-1){
						for(org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket: terms.getBuckets()){
							String x = value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount();
							if(StringUtils.isNotEmpty(functionName)){
								if("distinct".equals(functionName)){
									InternalCardinality distinct = bucket.getAggregations().get("distinct");
									x = x+"\003"+distinct.getValue();
								}else if("sum".equals(functionName)){
									InternalSum sum = bucket.getAggregations().get("sum");
									x = x+"\003"+sum.getValue();
								}else if("avg".equals(functionName)){
									InternalAvg avg = bucket.getAggregations().get("avg");
									x = x+"\003"+avg.getValue();
								}else if("max".equals(functionName)){
									InternalMax max = bucket.getAggregations().get("max");
									x = x+"\003"+max.getValue();
								}else if("min".equals(functionName)){
									InternalMin min = bucket.getAggregations().get("min");
									x = x+"\003"+min.getValue();
								}else if("stats".equals(functionName)){
									InternalStats stats = bucket.getAggregations().get("stats");
									x = x+"\003"+stats.getCount()+"\004"+stats.getSum()+"\004"+stats.getAvg()+"\004"+stats.getMax()+"\004"+stats.getMin();
								}
							}
							list.add(x);
						}
					}else{
						level++;
						for(org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket bucket: terms.getBuckets()){
							list.addAll(recursiveAllLevel(bucket.getAggregations(),value+"\001"+bucket.getKeyAsText(),list,level,nameList,functionName));
						}
					}
					break;
				case 2:
					InternalRange range = tmp.get(getName(name));
					if(level==nameList.size()-1){
						for(Object bucketr: range.getBuckets()){
							org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket = (org.elasticsearch.search.aggregations.bucket.range.Range.Bucket)bucketr;
							//list.add(value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount());
							String x = value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount();
							if(StringUtils.isNotEmpty(functionName)){
								if("distinct".equals(functionName)){
									InternalCardinality distinct = bucket.getAggregations().get("distinct");
									x = x+"\003"+distinct.getValue();
								}else if("sum".equals(functionName)){
									InternalSum sum = bucket.getAggregations().get("sum");
									x = x+"\003"+sum.getValue();
								}else if("avg".equals(functionName)){
									InternalAvg avg = bucket.getAggregations().get("avg");
									x = x+"\003"+avg.getValue();
								}else if("max".equals(functionName)){
									InternalMax max = bucket.getAggregations().get("max");
									x = x+"\003"+max.getValue();
								}else if("min".equals(functionName)){
									InternalMin min = bucket.getAggregations().get("min");
									x = x+"\003"+min.getValue();
								}else if("stats".equals(functionName)){
									InternalStats stats = bucket.getAggregations().get("stats");
									x = x+"\003"+stats.getCount()+"\004"+stats.getSum()+"\004"+stats.getAvg()+"\004"+stats.getMax()+"\004"+stats.getMin();
								}
							}
							list.add(x);
						}
					}else{
						level++;
						for(Object bucketr: range.getBuckets()){
							org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket = (org.elasticsearch.search.aggregations.bucket.range.Range.Bucket)bucketr;
							recursiveAllLevel(bucket.getAggregations(),value+"\001"+bucket.getKeyAsText(),list,level,nameList,functionName);
						}
					}
					break;
				case 3:
					Histogram histogram = tmp.get(getName(name));
					if(level==nameList.size()-1){
						for(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket bucket: histogram.getBuckets()){
							//list.add(value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount());
							String x = value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount();
							if(StringUtils.isNotEmpty(functionName)){
								if("distinct".equals(functionName)){
									InternalCardinality distinct = bucket.getAggregations().get("distinct");
									x = x+"\003"+distinct.getValue();
								}else if("sum".equals(functionName)){
									InternalSum sum = bucket.getAggregations().get("sum");
									x = x+"\003"+sum.getValue();
								}else if("avg".equals(functionName)){
									InternalAvg avg = bucket.getAggregations().get("avg");
									x = x+"\003"+avg.getValue();
								}else if("max".equals(functionName)){
									InternalMax max = bucket.getAggregations().get("max");
									x = x+"\003"+max.getValue();
								}else if("min".equals(functionName)){
									InternalMin min = bucket.getAggregations().get("min");
									x = x+"\003"+min.getValue();
								}else if("stats".equals(functionName)){
									InternalStats stats = bucket.getAggregations().get("stats");
									x = x+"\003"+stats.getCount()+"\004"+stats.getSum()+"\004"+stats.getAvg()+"\004"+stats.getMax()+"\004"+stats.getMin();
								}
							}
							list.add(x);
						}
					}else{
						level++;
						for(org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket bucket: histogram.getBuckets()){
							recursiveAllLevel(bucket.getAggregations(),value+"\001"+bucket.getKeyAsText(),list,level,nameList,functionName);
						}
					}
					break;
				case 4:
					DateHistogram dateHistogram = tmp.get(getName(name));
					if(level==nameList.size()-1){
						for(Bucket bucket: dateHistogram.getBuckets()){
							//list.add(value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount());
							String x = value+"\001"+bucket.getKeyAsText()+"\002"+bucket.getDocCount();
							if(StringUtils.isNotEmpty(functionName)){
								if("distinct".equals(functionName)){
									InternalCardinality distinct = bucket.getAggregations().get("distinct");
									x = x+"\003"+distinct.getValue();
								}else if("sum".equals(functionName)){
									InternalSum sum = bucket.getAggregations().get("sum");
									x = x+"\003"+sum.getValue();
								}else if("avg".equals(functionName)){
									InternalAvg avg = bucket.getAggregations().get("avg");
									x = x+"\003"+avg.getValue();
								}else if("max".equals(functionName)){
									InternalMax max = bucket.getAggregations().get("max");
									x = x+"\003"+max.getValue();
								}else if("min".equals(functionName)){
									InternalMin min = bucket.getAggregations().get("min");
									x = x+"\003"+min.getValue();
								}else if("stats".equals(functionName)){
									InternalStats stats = bucket.getAggregations().get("stats");
									x = x+"\003"+stats.getCount()+"\004"+stats.getSum()+"\004"+stats.getAvg()+"\004"+stats.getMax()+"\004"+stats.getMin();
								}
							}
							list.add(x);
						}
					}else{
						level++;
						for(Bucket bucket: dateHistogram.getBuckets()){
							recursiveAllLevel(bucket.getAggregations(),value+"\001"+bucket.getKeyAsText(),list,level,nameList,functionName);
						}
					}
					break;
				default:
					break;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	
	
	private static String getName(String value){
		String[] arr = value.split("\001");
		if(arr.length ==1 ){
			return arr[0].trim();
		}else{
			return arr[1].trim();
		}
	}
	
	private static int getType(String x){
		if(x.startsWith("datehistogram")){
			return 4;
		}else if(x.startsWith("histogram")){
			return 3;
		}else if(x.startsWith("range")){
			return 2;
		}else if(x.startsWith("terms")){
			return 1;
		}else{
			return 0;
		}
	}
	
	/**
	 * 解析group by 对于的分组字段名称
	 * @param qsp
	 * @return
	 */
	private static Map<Integer, String> getNameMap(String group){
		Map<Integer,String> tmpMap = new HashMap<Integer,String>();
		try{
			String[] arrGroup = group.split(",");
			int num = 0;
			for (String g : arrGroup) {
				num++;
				g = g.trim();
				if(g.indexOf("[")>0 && g.indexOf("]")>0){//group range 
					
					String field = g.substring(0,g.indexOf("[")).trim();
					String value = g.substring(g.indexOf("[")+1,g.indexOf("]"));
					String[] arr = value.split("\\|");
					if(arr.length==1){
						System.out.println("---------arr[0]"+arr[0]);
						String[] tmp = arr[0].split("-");
						if(tmp.length>=2){
							//先处理垂直直方图或者时间垂直直方图
							if("datehistogram".equals(tmp[0].trim().toLowerCase())){//时间垂直直方图
								tmpMap.put(num, "datehistogram");
							}else if("histogram".equals(tmp[0].trim().toLowerCase())){//垂直直方图
								tmpMap.put(num, "histogram");
							}else if("*".equals(tmp[0].trim())){
								tmpMap.put(num, "range"+"\001"+field);
							}else if("*".equals(tmp[1].trim())){
								tmpMap.put(num, "range"+"\001"+field);
							}
						}else{
							//格式错误
							
						}
					}else if(arr.length>1){
						tmpMap.put(num, "range"+"\001"+field);
					}
					
				}else{
					tmpMap.put(num, "terms"+"\001"+g);
				}
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
		return tmpMap;
	}
	
	/**
	 * 获取聚合函数对于的 名字
	 * @param select
	 * @return
	 */
	private static String getFunctionName(String select) {
		String functionName = null;
		try {
			String[] arrSelect = select.split(",");
			String field = null;
			boolean isDestinct = false;
			Operation optr = Operation.COUNT;
			for (String s : arrSelect) {
				if (s.trim().startsWith("count(")) {
					optr = Operation.COUNT;
					if (s.trim().indexOf("distinct") > 0) {
						field = getField("distinct", s.trim());
						isDestinct = true;
					} else {
						field = getField("(", s.trim());
					}
					break;
				} else if (s.trim().startsWith("sum(")) {
					optr = Operation.SUM;
					field = getField("(", s.trim());
					break;
				} else if (s.trim().startsWith("avg(")) {
					optr = Operation.AVG;
					field = getField("(", s.trim());
					break;
				} else if (s.trim().startsWith("max(")) {
					optr = Operation.MAX;
					field = getField("(", s.trim());
					break;
				} else if (s.trim().startsWith("min(")) {
					optr = Operation.MIN;
					field = getField("(", s.trim());
					break;
				} else if (s.trim().startsWith("stats(")) {
					optr = Operation.STATS;
					field = getField("(", s.trim());
					break;
				}
			}

			if (optr == Operation.COUNT) {
				if (isDestinct) {
					functionName = "distinct";// distinct
				}
			} else if (optr == Operation.AVG) {
				functionName ="avg";
			} else if (optr == Operation.SUM) {
				functionName ="sum";
			} else if (optr == Operation.MAX) {
				functionName ="max";
			} else if (optr == Operation.MIN) {
				functionName ="min";
			} else if (optr == Operation.STATS) {
				functionName ="stats";
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return functionName;
	}
	
	
	private static String getField(String indexOf, String s) {
		String tmp = s.substring(s.indexOf(indexOf) + indexOf.length(),
				s.indexOf(")")).trim();
		return tmp;
	}
	
	/**
	 * group by split 
	 * @param str
	 * @return
	 */
	private static List<String> splitGroupBy(String str){
		//String str = "(city,age[*-20|20-25|25-30|30-35|35-40|40-*],state),(city,state),(age,account_number)";
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
	
	
	//select count(who.s) from [events.20140909].events where what.s='hb' and appid.s='982da2ae92188e5f73fbf7f82e41ed65' and (when.d between '2014-09-09' and '2014-09-09') group by when.d[datehistogram-5minute-(HH:mm)]
	public static void resultParser(SearchResponse response,QuerySqlParser qsp){
		
		try{
			System.out.println(response.toString());
			
			//Terms terms = response.getAggregations().get("datehistogram");
			
			InternalGlobal global =  response.getAggregations().get("all_agg");
			
			InternalFilter filter = global.getAggregations().get("filter");
			
			DateHistogram dateHistogram = filter.getAggregations().get("datehistogram");
			
			
			
	       for(Bucket b:dateHistogram.getBuckets()){
	        	System.out.println("filedname:"+b.getKeyAsText()+"     docCount:"+b.getDocCount());
	        }
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	//select appid.s,what.s from [segments.20140909-new] group by appid.s,what.s
	public static void resultParser2(SearchResponse response,QuerySqlParser qsp){
		
		try{
			System.out.println(response.toString());
			
			//Terms terms = response.getAggregations().get("datehistogram");
			
			InternalGlobal global =  response.getAggregations().get("all_agg");
			
			//InternalFilter filter = global.getAggregations().get("filter");
			
			
			InternalTerms trems = global.getAggregations().get("appid.s");
			for(org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket b:trems.getBuckets()){
				System.out.println("filedname:"+b.getKeyAsText()+"     docCount:"+b.getDocCount());
				InternalTerms itrems = b.getAggregations().get("what.s");
				for(org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket b1:itrems.getBuckets()){
					System.out.println("filedname:"+b1.getKeyAsText()+"     docCount:"+b1.getDocCount());
				}
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
