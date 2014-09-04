package com.es.sql.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql语句解析器
 * @author wangyong
 *
 */
public class QuerySqlParser {

	private  String selectCol;
	private  String fromTable;
	private  String whereCondition;
	private  String groupCondition;
	private  String orderCondition;
	private  String limitCondition;
	
	private  String originalSql;
	
	public QuerySqlParser(String sql){
		this.originalSql = sql;
		parserSql(originalSql);
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//String sql = "select ds,serverid,count(distinct userid) as count from index.segment where (appid='10xxxx' and what='item' and (ds between '2014-08-12' and '2014-08-13') and tz='+8') or (axt in(1,2,3)) group by ds,serverid order by count desc limit 0,100";
		//new QuerySqlParser(sql);
		
		String sql = "select * from [segments.20140902].segments where appid.s ='982da2ae92188e5f73fbf7f82e41ed65' and what.s='item' limit 1";
		QuerySqlParser x = new QuerySqlParser(sql);
		System.out.println(x.whereCondition);
		System.out.println(x.limitCondition);
	}

	private void parserSql(String sql) {
		
		selectCol = getMatchedString("(select)(.+)(from)", sql);
		
		fromTable = getMatchedString("(from)(.+)(where)", sql);
		
		String regex = null;
		if (fromTable == null || fromTable.length() <= 0) {
			if (isContains(sql, "group\\s+by")) {
				regex="(from)(.+)(group\\s+by)";
			}else if (isContains(sql, "order\\s+by")) {
				regex="(from)(.+)(order\\s+by)";
			}if (isContains(sql, "limit")) {
				regex="(from)(.+)(limit)";
			}
			fromTable = getMatchedString(regex, sql);
		}
		
		if (isContains(sql, "group\\s+by")) {
			// 条件在where和group by之间
			regex = "(where)(.+)(group\\s+by)";
		} else if (isContains(sql, "order\\s+by")) {
			// 条件在where和order by之间
			regex = "(where)(.+)(order\\s+by)";
		} else if(isContains(sql, "limit")){
			// 条件在where到字符串末尾
			regex = "(where)(.+)(limit)";
		}else{
			// 条件在where到字符串末尾
			regex = "(where)(.+)($)";
		}
		whereCondition = getMatchedString(regex, sql);

		if (isContains(sql, "order\\s+by")) {
			regex = "(group\\s+by)(.+)(order\\s+by)";
		} else if (isContains(sql, "limit")) {
			regex = "(group\\s+by)(.+)(limit)";
		} else {
			regex = "(group\\s+by)(.+)($)";
		}

		groupCondition = getMatchedString(regex, sql);

		if (isContains(sql, "limit")) {
			regex = "(order\\s+by)(.+)(limit)";
		} else {
			regex = "(order\\s+by)(.+)($)";
		}
		orderCondition = getMatchedString(regex, sql);
		
		limitCondition = getMatchedString("(limit)(.+)($)", sql);

	}

	/**
	 * 从文本text中找到regex首次匹配的字符串，不区分大小写
	 * 
	 * @param regex
	 *            ： 正则表达式
	 * @param text
	 *            ：欲查找的字符串
	 * @return regex首次匹配的字符串，如未匹配返回空
	 */
	private static String getMatchedString(String regex, String text) {
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

		Matcher matcher = pattern.matcher(text);

		while (matcher.find()) {
			return matcher.group(2);
		}

		return null;
	}

	/**
	 * 看word是否在lineText中存在，支持正则表达式
	 * 
	 * @param lineText
	 * @param word
	 * @return
	 */
	private static boolean isContains(String lineText, String word) {
		Pattern pattern = Pattern.compile(word, Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(lineText);
		return matcher.find();
	}
	public String getSelectCol() {
		return selectCol;
	}
	public void setSelectCol(String selectCol) {
		this.selectCol = selectCol;
	}
	public String getFromTable() {
		return fromTable;
	}
	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}
	public String getWhereCondition() {
		return whereCondition;
	}
	public void setWhereCondition(String whereCondition) {
		this.whereCondition = whereCondition;
	}
	public String getGroupCondition() {
		return groupCondition;
	}
	public void setGroupCondition(String groupCondition) {
		this.groupCondition = groupCondition;
	}
	public String getOrderCondition() {
		return orderCondition;
	}
	public void setOrderCondition(String orderCondition) {
		this.orderCondition = orderCondition;
	}
	public String getLimitCondition() {
		return limitCondition;
	}
	public void setLimitCondition(String limitCondition) {
		this.limitCondition = limitCondition;
	}
	public String getOriginalSql() {
		return originalSql;
	}
	public void setOriginalSql(String originalSql) {
		this.originalSql = originalSql;
	}

	
}
