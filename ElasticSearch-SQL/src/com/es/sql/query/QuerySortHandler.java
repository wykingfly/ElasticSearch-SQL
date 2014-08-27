package com.es.sql.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.es.sql.parse.QuerySqlParser;

public class QuerySortHandler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	/**
	 * 处理排序
	 * @param qsp
	 * @return
	 */
	public static List<SortBuilder> getSortBuilder(QuerySqlParser qsp){
		List<SortBuilder> list = null;
		try{
			
			String orderBy = qsp.getOrderCondition();
			if(StringUtils.isEmpty(orderBy)){
				return null;
			}
			list = handlerSortCondition(orderBy);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return list;
	}
	
	/**
	 * 处理排序条件
	 * @param orderby
	 * @return
	 */
	private static List<SortBuilder> handlerSortCondition(String orderby){
		if(StringUtils.isEmpty(orderby)){
			return null;
		}
		List<SortBuilder> list = new ArrayList<SortBuilder>();
		
		try{
			String[] orderArr = orderby.split(",");
			for(String str:orderArr){// order by a ,b desc,c asc
				String[] arr = str.trim().split(" ");
				SortBuilder sortBuilder = null;
				if(arr.length==1){
					sortBuilder = SortBuilders.fieldSort(arr[0].trim()).order(SortOrder.ASC);
				}else{
					if("asc".equals(arr[1].trim().toLowerCase())){
						sortBuilder = SortBuilders.fieldSort(arr[0].trim()).order(SortOrder.ASC);
					}else{
						sortBuilder = SortBuilders.fieldSort(arr[0].trim()).order(SortOrder.DESC);
					}
				}
				
				list.add(sortBuilder);
			}
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return list;
	}
}
