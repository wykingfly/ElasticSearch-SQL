package com.es.sql.query;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.sort.SortBuilder;

import com.es.sql.parse.QuerySqlParser;
import com.es.sql.util.CommonUtils;
import com.es.sql.util.EsUtil;
import com.es.sql.util.CommonConstant.DataType;
import com.es.sql.util.CommonConstant.Operator;

/**
 * Api入口
 * @author wangyong
 *
 */
public class QueryParserHandler {

	
	public static void main(String[] args) {
		//分组
		/*String sql = "query:select ds,serverid,count(distinct userid) as count from index.segment where (appid='10xxxx' and what='item' and (ds between '2014-08-12' and '2014-08-13') and tz='+8') or (axt in(1,2,3) and a='t') or is_admin=1 group by ds,serverid order by count desc limit 0,100";
		
		SearchResponse searchResponse = handler(sql.trim());
		
		System.out.println(searchResponse.toString());*/
		//String sql = "filter:select state,city,count(distinct account_number) as count from bank where gender='M' and age>10 group by state,city";
		//String sql = "filter:select state,city,sum(balance) as total from bank where gender='M' and age>10 group by state,city";
		//String sql = "filter:select state,avg(balance) as total from bank where gender='M' and age>20 group by state";
		//String sql = "filter:select state,max(balance) as total from bank where gender='M' group by state";
		//String sql = "query:select * from bank where gender='M' and age>30";
		//String sql = "query:select * from bank where gender='M' and age in(30,31,32)";
		//String sql = "query:select * from bank where gender='M'";
		//String sql = "query:select * from bank where id=9BnH0MToTvWMHwikTb-uhA";
		//String sql = "query:select * from bank where (gender='M' and age>=40) or (balance>40000)";
		//String sql = "query:select * from bank where (gender='M' and age>=40) or (balance between 40000 and 44000)";
		//String sql = "query:select * from bank where (gender='M' and age>=40) or (balance>40000) limit 10";
		//String sql = "query:select * from bank where gender='M' and age>=30 and (balance between 40000 and 44000)";
		//String sql = "query:select * from bank where gender='M' and age>=30 and (balance between 40000 and 44000) and state in('id','wy')";
		//String sql = "query:select state,max(balance) from bank where gender='M' and age>=30 and state in('id','wy') group by state";
		//String sql = "query:select * from bank where firstname like '%beck%'";
		//String sql = "query:select * from bank where gender='M' and (age>=30 and age<35)";
		
		//String sql = "select sum(who.s) from events where context.channlid.s in(1,2,3,4) and context.serverid.s in('s1','s2') and what.s='item' group by context.serverid.s";
		
		//对多index支持，对sort排序测试，
		//String sql = "select * from bank.account order by age desc,account_number asc";
		//String sql = "select age,sum(balance) from bank.account group by age order by age desc";
		
		//------------------------------------------------------------------------------
		//String sql = "select stats(balance) from bank.account group by age";
		
		//String sql = "select stats(balance) from bank.account group by state,age[*-20|20-25|25-30|30-35|35-40|40-*]";
		
		//String sql = "select stats(balance) from bank.account group by (age[*-20|20-25|25-30|30-35|35-40|40-*]),(state)";
		
		//String sql = "select stats(balance) from bank.account group by (state,age[*-20|20-25|25-30|30-35|35-40|40-*]),(city)";
		
		//String sql = "select account_number,age,balance from bank where age>25";
		
		//String sql = "select account_number,age,balance from bank where age>25 order by balance desc";
		
		//String sql = "select stats(balance) from bank2.account2 group by age[histogram-5]";
		
		//String sql = "select stats(balance) from bank2.account2 group by state,age[histogram-5]";
		
		//String sql = "select stats(balance) from bank2.account2 group by createtime[datehistogram-2day]";
		
		//String sql = "select stats(balance) from bank2.account2 group by state,createtime[datehistogram-2day]";
		
		String sql = "";
		
		//对query/filter分离查询
		
		
		EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
		
		SearchResponse response = handler(sql.trim());
		System.out.println(response.toString());
	}
	
	
	
	public static SearchResponse handler(String sql){
		sql = sql.trim();
		int handler_type = 1;//1:filter 2:query
		if(sql.startsWith("filter:")){
			sql = sql.substring("filter:".length());
			handler_type = 1;
		}else if(sql.startsWith("query:")){
			sql = sql.substring("query:".length());
			handler_type = 2;
		}else{
			handler_type = 1;
		}
		System.out.println(sql.toString());
		
		QuerySqlParser qsp = new QuerySqlParser(sql);
		SearchResponse searchResponse = null;
		if(SyntaxCheck.checkSyntax(qsp)){
			
			Client client = EsUtil.getClient();
			
			int flag = 0;
			if(StringUtils.isNotEmpty(qsp.getWhereCondition())){
				flag = isIdQuery(qsp.getWhereCondition().trim());
			}
			
			if(StringUtils.isNotEmpty(qsp.getGroupCondition())){//聚合查询
				
				searchResponse = aggregationHandler(qsp, client);
				
			}else if(flag==1){//id查询
				
				searchResponse = queryById(qsp, client);
				
			}else if(flag ==2){//单一条件查询,已经冗余在普通查询中
				
				searchResponse = singleConditionQuery(qsp, client);
				
			}else {//普通查询
				
				searchResponse = commonQueryHandler(qsp, client);
				
			}
			
		}else{
			System.out.println("sql 语法错误，请检查后在提交查询！");
		}
		
		return searchResponse;
	}
	
	
	/**
	 * 单一条件查询
	 * @param qsp
	 * @param client
	 * @return
	 */
	private static SearchResponse singleConditionQuery(QuerySqlParser qsp,Client client){
		SearchResponse searchResponse = null;
		try{
			searchResponse = commonQueryHandler(qsp, client);
		}catch (Exception e) {
			e.printStackTrace();
		}
		return searchResponse;
	}
	
	/**
	 * 直接进行id查询
	 * @param qsp
	 * @param client
	 * @return
	 */
	private static SearchResponse queryById(QuerySqlParser qsp,Client client){
		SearchResponse searchResponse = null;
		try{
			String[] values = null;
			
			//id 查询
			String where = qsp.getWhereCondition();
			if(where.indexOf(" in")>0){//in条件查询,一次查询多个id
				Map<DataType, String[]> map = CommonUtils.getValues(where, 1);
				for(Map.Entry<DataType, String[]> entry : map.entrySet()){
					values = entry.getValue();
					System.out.println(values+"------------------------");
				}
				
			}else{
				String[] arr = where.split("=");
				values = new String[1];
				values[0] = arr[1].trim();
			}
			QueryBuilder queryBuilder = null;
			if(values.length>0){
				queryBuilder = QueryBuilders.idsQuery().ids(values);
			}
			
			String from = qsp.getFromTable();
			String[] indexArr = from.split(",");//index1.x,index2.y,index3
			
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
			
			String[] type = null;
			if(typeBuffer.toString().length()>0){
				type = typeBuffer.toString().substring(0,(typeBuffer.toString().length()-"\001".length())).split("\001");
			}
			
			SearchRequestBuilder sqb = client.prepareSearch(CommonUtils.trimStrings(index));
			if(type!=null && type.length>0){
				sqb.setTypes(CommonUtils.trimStrings(type));
			}
			
			if(queryBuilder!=null){
				sqb.setQuery(queryBuilder);
			}
			
			
			String select = qsp.getSelectCol().trim();
			String[] fields = null;
			if(StringUtils.isNotEmpty(select)){
				if(select.length()==1 && select.indexOf("*")>0){//select * from 。。。
					
				}else{
					fields = select.split(",");
					
				}
				
			}
			if(fields!=null && fields.length>0){
				searchResponse = sqb.execute().actionGet();
			}else{
				searchResponse = sqb.addFields(fields).execute().actionGet();
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return searchResponse;
	}
	
	/**
	 * 普通查询
	 * @param qsp
	 * @param client
	 * @return
	 */
	private static SearchResponse commonQueryHandler(QuerySqlParser qsp,Client client){
		SearchResponse searchResponse = null;
		try{
			QueryBuilder queryBuilder = QueryCommonHandler.getQueryBuilder(qsp);
			
			List<SortBuilder> list  = QuerySortHandler.getSortBuilder(qsp);
			int start = 0;
			int size = 100;
			String limit = qsp.getLimitCondition();
			if(StringUtils.isNotEmpty(limit)){
				String[] arr = limit.split(",");
				
				if(arr.length<=2){
					if(arr.length==2){
						start = Integer.parseInt(arr[0].trim());
						size = Integer.parseInt(arr[1].trim());
					}else{
						size = Integer.parseInt(arr[0].trim());
					}
				}
			}
			String from = qsp.getFromTable();
			String[] indexArr = from.split(",");//index1.x,index2.y,index3
			
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
			String[] type = null;
			if(typeBuffer.toString().length()>0){
				type = typeBuffer.toString().substring(0,(typeBuffer.toString().length()-"\001".length())).split("\001");
			}
			
			SearchRequestBuilder sqb = client.prepareSearch(CommonUtils.trimStrings(index));
			if(type!=null && type.length>0){
				sqb.setTypes(CommonUtils.trimStrings(type));
			}
			if(queryBuilder!=null){
				sqb.setQuery(queryBuilder);
			}
			if(list!=null && list.size()>0){
				for(SortBuilder sBuilder:list){
					sqb.addSort(sBuilder);
				}
			}
			
			String select = qsp.getSelectCol().trim();
			String[] fields = null;
			if(StringUtils.isNotEmpty(select)){
				if(select.length()==1 && select.indexOf("*")>0){//select * from 。。。
					
				}else{
					fields = select.split(",");
					
				}
				
			}
			if(fields!=null && fields.length>0){
				sqb.addFields(fields);
			}
			
			sqb.setFrom(start).setSize(size);
			
			searchResponse = sqb.execute().actionGet();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return searchResponse;
	}
	
	/**
	 * 聚合处理
	 * @param qsp
	 * @param client
	 * @return
	 */
	private static SearchResponse aggregationHandler(QuerySqlParser qsp,Client client){
		SearchResponse searchResponse = null;
		try{
			
			/**
			 * 使用Filter Query 进行拼接
			 */
			FilterBuilder filterBuilder = QueryFilterHandler.getFilterBuilder(qsp);
			
			/**
			 * 获取聚合查询条件
			 */
			AggregationBuilder aggregationBuilder = QueryAggregationHandler.getAggregationBuilder(qsp,filterBuilder);
			
			
			System.out.println(aggregationBuilder.toString());
			System.out.println("------------------------------------------------");
			if(filterBuilder!=null){
				System.out.println(filterBuilder.toString());
			}
			
			List<SortBuilder> list  = QuerySortHandler.getSortBuilder(qsp);

			int start = 0;
			int size = 100;
			String limit = qsp.getLimitCondition();
			if(StringUtils.isNotEmpty(limit)){
				String[] arr = limit.split(",");
				if(arr.length<=2){
					if(arr.length==2){
						start = Integer.parseInt(arr[0].trim());
						size = Integer.parseInt(arr[1].trim());
					}else{
						size = Integer.parseInt(arr[0].trim());
					}
				}
			}
			String from = qsp.getFromTable();
			String[] indexArr = from.split(",");//index1.x,index2.y,index3
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
			String[] type = null;
			if(typeBuffer.toString().length()>0){
				type = typeBuffer.toString().substring(0,(typeBuffer.toString().length()-"\001".length())).split("\001");
			}
			
			SearchRequestBuilder sqb = client.prepareSearch(CommonUtils.trimStrings(index));
			if(type!=null && type.length>0){
				sqb.setTypes(CommonUtils.trimStrings(type));
			}
			/*if(filterBuilder!=null){
				sqb.setPostFilter(filterBuilder);
			}*/
			if(aggregationBuilder!=null){
				sqb.addAggregation(aggregationBuilder);
			}
			
			if(list!=null && list.size()>0){
				for(SortBuilder sBuilder:list){
					sqb.addSort(sBuilder);
				}
			}
			
			sqb.setFrom(start).setSize(size);
			
			searchResponse = sqb.execute().actionGet();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return searchResponse;
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
