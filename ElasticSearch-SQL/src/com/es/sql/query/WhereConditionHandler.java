package com.es.sql.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class WhereConditionHandler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//String sql = "select ds,serverid,count(distinc userid) as count from index.segment where appid='10xxxx' and what='item' and ds between 2014-08-12 and 2014-08-13 and tz='+8' group by ds,serverid order by count desc limit 0,100";
		//String where = "appid = 'x' and what='item' and (ds between 1 and 2) and (a>10 and a<20) and b not in(3,54,10) and c like '%x'";
		//String where = "(a=x and b=y) or (c=z)";
		//String where = "a=x and b = y or c=z";//非严格语句，不处理
		//String where = "a=x or b=y or (c=z and d=t)";
		String where = " (appid='10xxxx' and what='item' and (ds between '2014-08-12' and '2014-08-13') and tz='+8') or (axt in(1,2,3) and t='p') or is_admin=1";
		
		handlerWhereCondition(where);

	}
	
	/**
	 * where condition handler 
	 * @param whereCondition
	 * @return
	 */
	public static Map<String, List<String>> handlerWhereCondition(String whereCondition){
		int split = sqlStartLogic(whereCondition);
		List<String> list = splitWhereConditionLevelOne(whereCondition, split);
		Map<String, List<String>> map = splitLevelOneGetLevelTwo(list, split);
		return map;
	}
	

	/**
	 * 对一级处理数据进行二级拆分处理
	 * @param list
	 * @param split
	 * @return
	 */
	private static Map<String, List<String>> splitLevelOneGetLevelTwo(List<String> list,int split){
		//---------------------------------一级拆分完毕，进行二级拆分处理-------------------------
		
		List<String> levelOneList = new ArrayList<String>();
		List<String> levelTwoList = null;
		Map<String, List<String>> map = new HashMap<String,List<String>>();//key = level_split
		
		int second_split = 1;
		int second_depth = 0;
		for(String str:list){
			if(str.trim().startsWith("(") && str.trim().endsWith(")")){//括弧配对完整,存在二级处理条件的可能性
				if((str.indexOf("in(")>0 || str.indexOf("in (")>0 ) && str.trim().indexOf(" and ")<0 && str.trim().indexOf(" or ")<0){//in 或者 not in ，不满足二级处理条件
					levelOneList.add(str);
				}else if((str.indexOf("not in(")>0 || str.indexOf("not in (")>0 ) && str.trim().indexOf(" and ")<0 && str.trim().indexOf(" or ")<0){//in 或者 not in ，不满足二级处理条件
					levelOneList.add(str);
				}else if(str.trim().indexOf(" between ")>0 && str.trim().indexOf(" and ")>0 && (str.trim().lastIndexOf(" and ")==str.trim().indexOf(" and "))){//between and
					levelOneList.add(str);
				}else if(str.trim().indexOf(" like ")>0 && str.trim().indexOf(" and ")<0 && str.trim().indexOf(" or ")<0){//只有like一条语句
					levelOneList.add(str);
				}else {//可能需要二级处理的(a=x and b=y and c=z) (a>10 and a<=20) (a=t or b=c or t=0) (a in(1,2,3) and b=1 and c like='10%') ((a between 1 and 10) and x)
					if(str.trim().indexOf(" and ")>0 || str.trim().indexOf(" or ")>0){//包含and语句
						int andor = sqlStartLogic(str.trim().substring(1));//丢掉第一个原始括弧进行处理
						second_split = andor;
						
						String[] secondArr = null;
						String tmp = str.trim().substring(1,str.trim().length()-1);//去掉两端的括弧
						if(andor==1){//and
							secondArr = tmp.trim().split(" and ");
						}else{//or
							secondArr = tmp.trim().split(" or ");
						}
						if(secondArr!=null && secondArr.length>1){
							if(andor==1 && secondArr.length==2){//考虑range情况 (a>10 and a<20)
								if(getColumn(secondArr[0]).equals(getColumn(secondArr[1]))){//域相同
									if(tmp.indexOf(">")>0 && tmp.indexOf("<")>0){
										levelOneList.add(str);
									}
								}else{
									levelTwoList = new ArrayList<String>(); 
									for(String s : secondArr){
										levelTwoList.add(s);//二级查询,不涉及多级包含
									}
								}
							}else if(str.trim().indexOf(" between ")>0){
								StringBuffer stringBuffer = new StringBuffer();
								int i = 0;
								levelTwoList = new ArrayList<String>(); 
								for(String s : secondArr){
									if(i==1){
										stringBuffer.append(" and ").append(s);
										levelTwoList.add(stringBuffer.toString());
										stringBuffer = null;
									}
									if(s.indexOf(" between ")>0){
										stringBuffer.append(s);
										i = 1;
									}else if(i==0){
										levelTwoList.add(s);//二级查询,不涉及多级包含
									}
									if(i==1 && stringBuffer == null){
										i=0;
									}
								}
							}else{
								levelTwoList = new ArrayList<String>(); 
								for(String s : secondArr){
									levelTwoList.add(s);//二级查询,不涉及多级包含
								}
							}
						}else if(secondArr.length==1){//(c='x')
							levelOneList.add(tmp);
						}
					}else{
						String tmp = str.trim().substring(1,str.trim().length()-1);//去掉两端的括弧
						levelOneList.add(tmp);
					}
				}
			}else{
				levelOneList.add(str);
			}
			if(levelTwoList!=null && levelTwoList.size()>0){
				map.put("2_"+second_split+"_"+second_depth++, levelTwoList);
				levelTwoList = null;
			}
			
		}
		
		
		map.put("1_"+split, levelOneList);
		
		for(Map.Entry<String, List<String>> entry:map.entrySet()){
			String key = entry.getKey();
			List<String> listx = entry.getValue();
			if(key.startsWith("1_")){
				System.out.println("---------------------------level one -------------------");
				for(String l1:listx){
					System.out.println(l1);
				}
			}else{
				System.out.println("---------------------------level two -------------------");
				for(String l2:listx){
					System.out.println(l2);
				}
			}
		}
		
		
		
		
		return map;
	}
	
	
	/**
	 * where Condition 第一层拆分
	 * @param whereCondition
	 * @param split
	 * @return
	 */
	private static List<String> splitWhereConditionLevelOne(String whereCondition,int split){
		List<String> list = new ArrayList<String>();
		String[] arr = null;
		if(split==1){
			arr = whereCondition.split(" and ");
		}else if(split==2){
			arr = whereCondition.split(" or ");
		}else if(split==0){
			arr = new String[1];
			arr[0] = whereCondition;
		}
		for(String str:arr){
			System.out.println(str);
		}
		
		System.out.println("-----------------------------------");
		
		StringBuffer sBuffer = new StringBuffer();
		
		for(String str:arr){
			boolean b = false;
			if(str.trim().indexOf("in(")>0){
				b = false;
			}else if(str.trim().startsWith("(") ){
				if(str.trim().startsWith("(") && str.trim().endsWith(")")){//括弧配对完整
					b=false;
				}else{
					sBuffer.append(str);
					b = true;
				}
			}else if(sBuffer.length()>0){
				if(split==1){
					sBuffer.append(" and "+str);
				}else if(split==2){
					sBuffer.append(" or "+str);
				}
				if(str.trim().endsWith(")")){
					list.add(sBuffer.toString());
					sBuffer = new StringBuffer();
				}
				b = true;
			}
			
			if(!b){
				list.add(str);
			}
		}
		
		for(String string:list){
			System.out.println(string);
		}
		return list;
	}
	
	/**
	 * >=
	 * <=
	 * >
	 * <
	 * =
	 * !=
	 * @param str
	 * @return
	 */
	private static String getColumn(String str){
		String tmp = "";
		if(str.indexOf(">=")>0){
			tmp = str.substring(0,str.indexOf(">=")).trim();
		}else if(str.indexOf("<=")>0){
			tmp = str.substring(0,str.indexOf("<=")).trim();
		}else if(str.indexOf("!=")>0){
			tmp = str.substring(0,str.indexOf("!=")).trim();
		}else if(str.indexOf("=")>0){
			tmp = str.substring(0,str.indexOf("=")).trim();
		}else if(str.indexOf("<")>0){
			tmp = str.substring(0,str.indexOf("<")).trim();
		}else if(str.indexOf(">")>0){
			tmp = str.substring(0,str.indexOf(">")).trim();
		}
		System.out.println("--------getField-----"+tmp);
		return tmp;
	}
	
	/**
	 * 拆分where Condition ，获取第一层拆分使用and或者or进行
	 * 0:语句为null，1：and 2：or
	 * @param whereCondition
	 * @return
	 */
	private static int sqlStartLogic(String whereCondition){
		
		String where = whereCondition;
		if(StringUtils.isEmpty(where)){
			return 0;
		}
		int split = 1;//and
		if(where.trim().startsWith("(")){
			
			String tmp = where.substring(getPoint(whereCondition)+1);
			if(tmp.trim().startsWith("and")){
				split =1;
			}else if(tmp.trim().startsWith("or")){
				split=2;
			}
		}else if(where.indexOf(" and ")>0 || where.indexOf(" or ")>0){
			int x = where.indexOf("and");
			int y = where.indexOf("or");
			if(x>0 && y>0){
				if(x>y){
					split =2;
				}else{
					split =1;
				}
			}else if(x>0 && y<0){
				split =1;
			}else if(x<0 && y>0){
				split =2;
			}
		}else{//单一条件
			split = 0;
		}
		return split;
	}
	
	private static int getPoint(String whereCondition){
		int x = 0;
		int y = 0;
		int point = 0;
		for(int i=0;i<whereCondition.length();i++){
			if(whereCondition.charAt(i)=='('){
				x++;
				y++;
			}else if(whereCondition.charAt(i)==')'){
				x--;
			}
			
			point = i;
			if(x==0 && y>0){
				break;
			}
		}
		return point;
	}
	
}
