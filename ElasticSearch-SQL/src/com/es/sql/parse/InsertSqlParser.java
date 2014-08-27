package com.es.sql.parse;

public class InsertSqlParser extends BaseSingleSqlParser {
	public InsertSqlParser(String originalSql) {
		super(originalSql);
	}

	@Override
	protected void initializeSegments() {
		segments.add(new SqlSegment("(insert into)(.+)([(])", "[,]"));
		segments.add(new SqlSegment("([(])(.+)( [)] values )", "[,]"));
		segments.add(new SqlSegment("([)] values [(])(.+)( [)])", "[,]"));
	}

	@Override
	public String getParsedSql() {
		String retval = super.getParsedSql();
		retval = retval + ")";
		return retval;
	}
}