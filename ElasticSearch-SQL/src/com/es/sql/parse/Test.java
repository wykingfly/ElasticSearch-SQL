package com.es.sql.parse;

public class Test {
	/** *//**
	    * 单句Sql解析器制造工厂
	    * @author 
	    *
	    * @since 2013-6-10
	    * @version 1.00
	    */
	    public static void main(String[] args) {
	        // TODO Auto-generated method stub
	       //String test="select  a from  b " +
	           //    "\n"+"where      a=b";
	       //test=test.replaceAll("\\s{1,}", " ");
	       //System.out.println(test);
	       //程序的入口
	        String testSql="select c1,c2,c3     from    t1,t2 where condi3=3 "+"\n"+"    or condi4=5 order by o1,o2";
	        SqlParserUtil test=new SqlParserUtil();
	        String result=test.getParsedSql(testSql);
	        System.out.println(result);
	       //List<SqlSegment> result=test.getParsedSqlList(testSql);//保存解析结果
	    }
}
