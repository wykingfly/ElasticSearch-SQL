package com.es.sql.parse;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseSingleSqlParser {
	/** */
	/**
	 * 　* 原始Sql语句 　
	 */
	protected String originalSql;
	/** */
	/**
	 * 　* Sql语句片段 　
	 */
	protected List<SqlSegment> segments;

	/** */
	/**
	 * 　* 构造函数，传入原始Sql语句，进行劈分。 　* @param originalSql 　
	 */
	public BaseSingleSqlParser(String originalSql) {
		this.originalSql = originalSql;
		segments = new ArrayList<SqlSegment>();
		initializeSegments();
		splitSql2Segment();
	}

	/** */
	/**
	 * 　* 初始化segments，强制子类实现 　* 　
	 */
	protected abstract void initializeSegments();

	/** */
	/**
	 * 　* 将originalSql劈分成一个个片段 　* 　
	 */
	protected void splitSql2Segment() {
		for (SqlSegment sqlSegment : segments) {
			sqlSegment.parse(originalSql);
		}
	}

	/** */
	/**
	 * 　* 得到解析完毕的Sql语句 　* @return 　
	 */
	public String getParsedSql() {

		// 测试输出各个片段的信息
	
		 for(SqlSegment sqlSegment:segments) { 
			 String start=sqlSegment.getStart(); 
			 String end=sqlSegment.getEnd();
			 System.out.println(start); 
			 System.out.println(end); 
		 }
		 

		StringBuffer sb = new StringBuffer();
		for (SqlSegment sqlSegment : segments) {
			sb.append(sqlSegment.getParsedSqlSegment());
		}
		String retval = sb.toString().replaceAll("@+", "\n");
		return retval;
	}

	/** */
	/**
	 * 得到解析的Sql片段
	 * 
	 * @return
	 */
	public List<SqlSegment> RetrunSqlSegments() {
		int SegmentLength = this.segments.size();
		if (SegmentLength != 0) {
			List<SqlSegment> result = this.segments;
			return result;
		} else {
			// throw new Exception();
			return null;
		}
	}
}
