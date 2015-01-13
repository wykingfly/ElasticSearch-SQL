ElasticSearch-SQL
=================

该项目主要是为了熟悉sql的人员能够很方便的进行elasticsearch数据的查询，降低学习成本。

#目前项目可以支持的查询如下：

```
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
		
		//------------------------------------------------------------------------------
		//String sql = "select stats(balance) from bank.account group by age";
		
		//String sql = "select stats(balance) from bank.account group by state,age[*-20|20-25|25-30|30-35|35-40|40-*]";
		
		//String sql = "select stats(balance) from bank.account group by (age[*-20|20-25|25-30|30-35|35-40|40-*]),(state)";
		
		//String sql = "select stats(balance) from bank.account group by (state,age[*-20|20-25|25-30|30-35|35-40|40-*]),(city)";
		
		//String sql = "select account_number,age,balance from bank where age>25";
		
		//String sql = "select account_number,age,balance from bank where age>25 order by balance desc";
		
		//----------------------------------------------------------------------------------------
		
		//String sql = "select stats(balance) from bank2.account2 group by age[histogram-5]";
		
		//String sql = "select stats(balance) from bank2.account2 group by state,age[histogram-5]";
		
		//String sql = "select stats(balance) from bank2.account2 group by createtime[datehistogram-2day]";
		
		//String sql = "select stats(balance) from bank2.account2 group by state,createtime[datehistogram-2day]";
		
```


#测试数据导入
```
	路径：/com/es/api/test/accounts.json
```
#执行测试：
	*/com/es/api/test/BankMapping.java 创建index ，根据需求修改index和type
	*/com/es/api/test/BankContentIndex.java 根据需求修改index和type，已经accounts.json路径
	*其他测试类可自行修改之后测试

#ElasticSearch - JAVA API 测试用例
```
	/com/es/sql/query/TestQuery.java
```


#已经支持功能如下：
```
	count
	count(distinct ..)
	sum
	max
	avg
	min
	stats
	
	between ... and ...
	like 
	in 
	not in
	=
	!=
	>
	>=
	<
	<=
	
	id查询
	
	自定义查询结果维度 select a,b,c,d from ....
	
	多层group :实例 group by a,b,c,d
	
	多组group :实例 group by (a,b),(c,d),(e,f)
	
	range group:实例 group by age[*-20|20-25|25-30|30-35|35-40|40-*]
	
	histogram :实例 group by age[histogram-5]
	
	datehistogram ：实例 createtime[datehistogram-2day]
```	
#使用的公司

	*热云科技：http://reyun.com/
	
