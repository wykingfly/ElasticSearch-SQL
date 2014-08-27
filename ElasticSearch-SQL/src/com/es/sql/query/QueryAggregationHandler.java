package com.es.sql.query;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import com.es.sql.parse.QuerySqlParser;

public class QueryAggregationHandler {

	private enum Operation{
		COUNT,
		SUM,
		AVG,
		MAX,
		MIN
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println(getField("distinct", "count(distinct account_number)"));
		System.out.println(getField("(","sum(balance)"));
	}
	
	/**
	 * group by handler
	 * @param qsp
	 * @return
	 */
	public static  AggregationBuilder getAggregationBuilder(QuerySqlParser qsp,FilterBuilder filterBuilder){
		int size = 100;
		AggregationBuilder aggregationBuilder = null;
		
		try{
			String select = qsp.getSelectCol();
			String group = qsp.getGroupCondition();
			String[] arrSelect = select.split(",");
			String[] arrGroup = group.split(",");
			String field = null;
			boolean isDestinct = false;
			Operation optr = Operation.COUNT;
			for(String s:arrSelect){
				if(s.trim().startsWith("count(")){
					optr = Operation.COUNT;
					//String tmp = s.trim().substring(s.trim().indexOf("distinct")+8,s.trim().length()-2);
					if(s.trim().indexOf("distinct")>0){
						field = getField("distinct",s.trim());
						isDestinct = true;
					}else{
						field = getField("(", s.trim());
					}
					break;
				}else if(s.trim().startsWith("sum(")){
					optr = Operation.SUM;
					field = getField("(",s.trim());
					break;
				}else if(s.trim().startsWith("avg(")){
					optr = Operation.AVG;
					field = getField("(",s.trim());
					break;
				}else if(s.trim().startsWith("max(")){
					optr = Operation.MAX;
					field = getField("(",s.trim());
					break;
				}else if(s.trim().startsWith("min(")){
					optr = Operation.MIN;
					field = getField("(",s.trim());
					break;
				}
			}
			//select count(distinct who) ....group by ds,serverid
			//select sum(price) group by ds,serverid
			int num = 0;
			List<AggregationBuilder> list = new ArrayList<AggregationBuilder>();
			aggregationBuilder = AggregationBuilders.global("all_agg");
			
			for(String g:arrGroup){
				list.add(AggregationBuilders.terms(g.trim()).field(g.trim()).size(size));
			}
			AggregationBuilder aggregationBuilder2 = null;
			
			if(list.size()>0){
				aggregationBuilder2 = list.get(list.size()-1);
				if(optr==Operation.COUNT){
					if(isDestinct){
						aggregationBuilder2.subAggregation(AggregationBuilders.cardinality("distinct").field(field));//distinct
					}
				}else if(optr == Operation.AVG){
					aggregationBuilder2.subAggregation(AggregationBuilders.avg("avg").field(field));
				}else if(optr == Operation.SUM){
					aggregationBuilder2.subAggregation(AggregationBuilders.sum("sum").field(field));
				}else if(optr == Operation.MAX){
					aggregationBuilder2.subAggregation(AggregationBuilders.max("max").field(field));
				}else if(optr == Operation.MIN){
					aggregationBuilder2.subAggregation(AggregationBuilders.min("min").field(field));
				}
				
				
				for(int i=list.size()-2;i>=0;i--){
					
					aggregationBuilder2 = list.get(i).subAggregation(aggregationBuilder2);
					
				}
			}
			if(aggregationBuilder2!=null){
				if(filterBuilder!=null){
					aggregationBuilder.subAggregation(AggregationBuilders.filter("filter").filter(filterBuilder).subAggregation(aggregationBuilder2));
				}else{
					aggregationBuilder.subAggregation(aggregationBuilder2);
				}
			}
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return aggregationBuilder;
	}
	
	private static String getField(String indexOf,String s){
		String tmp = s.substring(s.indexOf(indexOf)+indexOf.length(),s.indexOf(")")).trim();
		return tmp;
	}

}
