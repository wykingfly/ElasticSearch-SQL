package com.es.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;

import com.es.sql.parse.QuerySqlParser;

/**
 * 聚合处理
 * 
 * @author wangyong
 * 
 */
public class QueryAggregationHandler {

	private enum Operation {
		COUNT, SUM, AVG, MAX, MIN, STATS
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(getField("distinct",
				"count(distinct account_number)"));
		System.out.println(getField("(", "sum(balance)"));
	}

	/**
	 * group by handler
	 * 
	 * @param qsp
	 * @return
	 */
	public static AggregationBuilder getAggregationBuilder(QuerySqlParser qsp,
			FilterBuilder filterBuilder) {

		AggregationBuilder aggregationBuilder = null;

		String group = qsp.getGroupCondition().trim();
		if (StringUtils.isEmpty(group)) {
			return null;
		}
		try {
			Map<Integer,List<AggregationBuilder>> map = new HashMap<Integer,List<AggregationBuilder>>();
			if (group.startsWith("(")) {// 可能多层嵌套group存在
				//(city,age[*-20|20-25|25-30|30-35|35-40|40-*],state),(city,state),(age,account_number)
				
				List<String> list = splitGroupBy(group);
				for(int i=0;i<list.size();i++){
					List<AggregationBuilder> list1 = getAggregationBuilderSimpleGroupBy(list.get(i));
					map.put(i, list1);
				}
				
			} else{// 普通
				//city,age[*-20|20-25|25-30|30-35|35-40|40-*],state
				//city,state
				List<AggregationBuilder> list1 = getAggregationBuilderSimpleGroupBy(group);
				map.put(1, list1);
			}
			
			aggregationBuilder = AggregationBuilders.global("all_agg");
			
			List<AggregationBuilder> mutliValues = new ArrayList<AggregationBuilder>();
			
			for(Map.Entry<Integer, List<AggregationBuilder>> entry: map.entrySet()){
				List<AggregationBuilder> list = entry.getValue();
				AggregationBuilder aggregationBuilder2 = null;
				if (list.size() > 0) {
					aggregationBuilder2 = list.get(list.size() - 1);
					
					aggregationBuilder2 = getAggregationBuilderFuncation(qsp.getSelectCol().trim(),aggregationBuilder2);

					for (int i = list.size() - 2; i >= 0; i--) {

						aggregationBuilder2 = list.get(i).subAggregation(
								aggregationBuilder2);

					}
				}
				if(aggregationBuilder2!=null){
					mutliValues.add(aggregationBuilder2);
				}
				
			}
			if (filterBuilder != null) {
				FilterAggregationBuilder fab = AggregationBuilders.filter("filter").filter(filterBuilder);
				for(AggregationBuilder agg:mutliValues){
					fab.subAggregation(agg);
				}
				aggregationBuilder.subAggregation(fab);
			}else{
				for(AggregationBuilder agg:mutliValues){
					aggregationBuilder.subAggregation(agg);
				}
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return aggregationBuilder;
	}

	/**
	 * group by handler (simple group by )
	 * 
	 * @param qsp
	 * @return
	 */
	private static List<AggregationBuilder> getAggregationBuilderSimpleGroupBy(String group) {
		//int size = 1000;
		List<AggregationBuilder> list = new ArrayList<AggregationBuilder>();
		try {
			String[] arrGroup = group.split(",");
			for (String g : arrGroup) {
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
								String x = tmp[1].trim().toLowerCase();
								Interval interval = null;
								int i = 0;
								if(x.endsWith("second")){
									interval = Interval.SECOND;
									if(!"second".equals(x)){
										String t = x.substring(0,x.indexOf("second"));
										if(isIntegerOrDouble(t)){
											i = Integer.parseInt(t);
										}
										interval = Interval.seconds(i);
									}
								}else if(x.endsWith("minute")){
									interval = Interval.MINUTE;
									if(!"minute".equals(x)){
										String t = x.substring(0,x.indexOf("minute"));
										if(isIntegerOrDouble(t)){
											i = Integer.parseInt(t);
										}
										interval = Interval.minutes(i);
									}
								}else if(x.endsWith("hour")){
									interval = Interval.HOUR;
									if(!"hour".equals(x)){
										String t = x.substring(0,x.indexOf("hour"));
										if(isIntegerOrDouble(t)){
											i = Integer.parseInt(t);
										}
										interval = Interval.hours(i);
									}
								}else if(x.endsWith("day")){
									interval = Interval.DAY;
									if(!"day".equals(x)){
										String t = x.substring(0,x.indexOf("day"));
										if(isIntegerOrDouble(t)){
											i = Integer.parseInt(t);
										}
										interval = Interval.days(i);
									}
									
								}else if(x.endsWith("week")){
									interval = Interval.WEEK;
									if(!"week".equals(x)){
										String t = x.substring(0,x.indexOf("week"));
										if(isIntegerOrDouble(t)){
											i = Integer.parseInt(t);
										}
										interval = Interval.weeks(i);
									}
								}else if(x.endsWith("month")){
									interval = Interval.MONTH;
								}else if(x.endsWith("quarter")){
									interval = Interval.QUARTER;
								}else if(x.endsWith("year")){
									interval = Interval.YEAR;
								}
								if(tmp.length==3){//tmp[2]表示格式
									String formate = tmp[2].trim();
									formate = formate.substring(1,formate.length()-1).trim();
									list.add(AggregationBuilders.dateHistogram("datehistogram").field(field).interval(interval).format(formate));
								}else{
									list.add(AggregationBuilders.dateHistogram("datehistogram").field(field).interval(interval));
								}
							}else if("histogram".equals(tmp[0].trim().toLowerCase())){//垂直直方图
								String x = tmp[1].trim().toLowerCase();
								if(isIntegerOrDouble(x)){
									long interval = Long.parseLong(x);
									list.add(AggregationBuilders.histogram("histogram").field(field).interval(interval));
								}
							
							}else if("*".equals(tmp[0].trim())){
								
								if(isIntegerOrDouble(tmp[1])){//必须是数字或者小数
									
									list.add(AggregationBuilders.range(field).field(field).addUnboundedTo(arr[0].trim(),Double.parseDouble(tmp[1])));
								}
							}else if("*".equals(tmp[1].trim())){
								if(isIntegerOrDouble(tmp[0])){//必须是数字或者小数
									
									
									list.add(AggregationBuilders.range(field).field(field).addUnboundedFrom(arr[0].trim(),Double.parseDouble(tmp[0])));
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
						list.add(rg);
					}
					
				}else{
					list.add(AggregationBuilders.terms(g).field(g));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * 获取分支的最后一个聚合
	 * @param qsp
	 * @return
	 */
	private static AggregationBuilder getAggregationBuilderFuncation(String select,AggregationBuilder aggregationBuilder2) {
		if(aggregationBuilder2 == null){
			return null;
		}
		try {
			String[] arrSelect = select.split(",");
			String field = null;
			boolean isDestinct = false;
			Operation optr = Operation.COUNT;
			for (String s : arrSelect) {
				if (s.trim().startsWith("count(")) {
					optr = Operation.COUNT;
					// String tmp =
					// s.trim().substring(s.trim().indexOf("distinct")+8,s.trim().length()-2);
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
					aggregationBuilder2.subAggregation(AggregationBuilders
							.cardinality("distinct").field(field));// distinct
				}
			} else if (optr == Operation.AVG) {
				aggregationBuilder2.subAggregation(AggregationBuilders.avg(
						"avg").field(field));
			} else if (optr == Operation.SUM) {
				aggregationBuilder2.subAggregation(AggregationBuilders.sum(
						"sum").field(field));
			} else if (optr == Operation.MAX) {
				aggregationBuilder2.subAggregation(AggregationBuilders.max(
						"max").field(field));
			} else if (optr == Operation.MIN) {
				aggregationBuilder2.subAggregation(AggregationBuilders.min(
						"min").field(field));
			} else if (optr == Operation.STATS) {
				aggregationBuilder2.subAggregation(AggregationBuilders.stats(
						"stats").field(field));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return aggregationBuilder2;
	}

	/**
	 * group by handler
	 * 
	 * @param qsp
	 * @return
	 */
	public static AggregationBuilder getAggregationBuilder2(QuerySqlParser qsp,
			FilterBuilder filterBuilder) {
		int size = 100;
		AggregationBuilder aggregationBuilder = null;

		try {
			String select = qsp.getSelectCol();
			String group = qsp.getGroupCondition();
			String[] arrSelect = select.split(",");
			String[] arrGroup = group.split(",");
			String field = null;
			boolean isDestinct = false;
			Operation optr = Operation.COUNT;
			for (String s : arrSelect) {
				if (s.trim().startsWith("count(")) {
					optr = Operation.COUNT;
					// String tmp =
					// s.trim().substring(s.trim().indexOf("distinct")+8,s.trim().length()-2);
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
			// select count(distinct who) ....group by ds,serverid
			// select sum(price) group by ds,serverid
			int num = 0;
			List<AggregationBuilder> list = new ArrayList<AggregationBuilder>();
			aggregationBuilder = AggregationBuilders.global("all_agg");

			for (String g : arrGroup) {
				list.add(AggregationBuilders.terms(g.trim()).field(g.trim())
						.size(size));
			}
			AggregationBuilder aggregationBuilder2 = null;

			if (list.size() > 0) {
				aggregationBuilder2 = list.get(list.size() - 1);
				if (optr == Operation.COUNT) {
					if (isDestinct) {
						aggregationBuilder2.subAggregation(AggregationBuilders
								.cardinality("distinct").field(field));// distinct
					}
				} else if (optr == Operation.AVG) {
					aggregationBuilder2.subAggregation(AggregationBuilders.avg(
							"avg").field(field));
				} else if (optr == Operation.SUM) {
					aggregationBuilder2.subAggregation(AggregationBuilders.sum(
							"sum").field(field));
				} else if (optr == Operation.MAX) {
					aggregationBuilder2.subAggregation(AggregationBuilders.max(
							"max").field(field));
				} else if (optr == Operation.MIN) {
					aggregationBuilder2.subAggregation(AggregationBuilders.min(
							"min").field(field));
				} else if (optr == Operation.STATS) {
					aggregationBuilder2.subAggregation(AggregationBuilders
							.stats("stats").field(field));
				}

				for (int i = list.size() - 2; i >= 0; i--) {

					aggregationBuilder2 = list.get(i).subAggregation(
							aggregationBuilder2);

				}
			}
			if (aggregationBuilder2 != null) {
				if (filterBuilder != null) {
					aggregationBuilder.subAggregation(AggregationBuilders
							.filter("filter").filter(filterBuilder)
							.subAggregation(aggregationBuilder2));
				} else {
					aggregationBuilder.subAggregation(aggregationBuilder2);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return aggregationBuilder;
	}

	private static String getField(String indexOf, String s) {
		String tmp = s.substring(s.indexOf(indexOf) + indexOf.length(),
				s.indexOf(")")).trim();
		return tmp;
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
}
