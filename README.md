ElasticSearch-SQL
=================

该项目主要是为了熟悉sql的人员能够很方便的进行elasticsearch数据的查询，降低学习成本。


目前项目可以支持的查询如下：

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
		//String sql = "select * from bank.account order by age desc,account_number asc";
