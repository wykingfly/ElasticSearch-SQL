package com.es.sql.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.es.sql.parse.QuerySqlParser;

/**
 * group 语法检查
 * @author wangyong
 *
 */
public class SyntaxCheck {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * 语法检查
	 * @param qsp
	 * @return
	 */
	public static boolean checkSyntax(QuerySqlParser qsp){
		boolean b = true;
		if(StringUtils.isEmpty(qsp.getSelectCol())){
			b = false;
		}
		if(StringUtils.isEmpty(qsp.getFromTable())){
			b = false;
		}
		if(StringUtils.isNotEmpty(qsp.getSelectCol()) && StringUtils.isNotEmpty(qsp.getGroupCondition())){//存在group by条件，检查select
			String select = qsp.getSelectCol();
			String group = qsp.getGroupCondition();
			String[] arrSelect = select.split(",");
			String[] arrGroup = group.split(",");
			int num = 0;
			List<String> list = new ArrayList<String>();
			for(String g:arrGroup){
				for(String s:arrSelect){
					if(s.trim().equals(g.trim())){
						num++;
						list.add(s.trim());
						break;
					}else{
						if(s.trim().startsWith("count(") || s.trim().startsWith("sum(") || s.trim().startsWith("avg(") || s.trim().startsWith("max(") || s.trim().startsWith("min(")|| s.trim().startsWith("stats(")){
							
						}else{
							b = false;
							for(String x:list){
								if(x.trim().equals(s.trim())){
									b = true;//纠正
									break;
								}
							}
							//存在select域，在执行group by情况下不属于聚合函数或者group字段的域
						}
					}
				}
			}
			
		}
		return b;
	}
	
}
