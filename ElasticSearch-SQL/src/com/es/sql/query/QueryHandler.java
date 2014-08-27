package com.es.sql.query;

public class QueryHandler {

	//---------------------------------------------------------------------
	/**
	 * select count(distinct who) as c from index.segment 
	 * where appid='x' and what='item' and ds between 2014-08-12 and 2014-08-13 
	 * and (kingdom in('1','2') or tz='+08')
	 * group by ds,ip
	 * order by c desc
	 * limit 50
	*/
	//---------------------------------------------------------------------
	/**
	 * select sum(price) as total from index.segment
	 * where appid='x' and what='item' and ds between 2014-08-12 and 2014-08-13 
	 * and (kingdom in('1','2') or tz='+08')
	 * group by ds,ip
	 * order by total desc
	 * limit 100
	 */
	//---------------------------------------------------------------------
	/**
	 * select a,b,c,d from index.type
	 * where e not in('1','2') and p != null and t='x'
	 * order by a desc
	 * limit 0,100
	 */
	//---------------------------------------------------------------------
	/**
	 * select * from index1,index2 where id=1
	 * 
	 * select count(a),sum(b) from index
	 * 
	 */
	
	
	
	
}
