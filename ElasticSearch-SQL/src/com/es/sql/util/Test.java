package com.es.sql.util;
/**
 * Test Class
 * @author wangyong
 *
 */
public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//System.out.println(isIdQuery("id=9BnH0MToTvWMHwikTb-uhA"));
		test();
	}

	private static void test(){
		String[] indexArr = new String[]{"index.account","index1.account1"};
		
		StringBuffer indexBuffer = new StringBuffer();
		StringBuffer typeBuffer = new StringBuffer();
		for(String str:indexArr){
			String[] x = str.split("\\.");
			if(x.length==1){
				indexBuffer.append(x[0]).append("\001");
			}else{
				indexBuffer.append(x[0]).append("\001");
				typeBuffer.append(x[1]).append("\001");
			}
		}
		String[] index = indexBuffer.toString().substring(0,(indexBuffer.toString().length()-"\001".length())).split("\001");
		String[] type = typeBuffer.toString().substring(0,(typeBuffer.toString().length()-"\001".length())).split("\001");
		
		System.out.println(index.toString());
		System.out.println(type.toString());
	}
	
	/**
	 * 判断是否是id查询
	 * @param whereCondition
	 * @return
	 */
	private static int isIdQuery(String whereCondition){
		int flag =0;//0:多条件查询 1：id查询 2：单一条件查询
		try{
			if(whereCondition.indexOf(" and ")>0 || whereCondition.indexOf(" or ")>0){
				flag = 0;
				if(whereCondition.indexOf(" between ")>0 && whereCondition.indexOf(" or ")<0){
					if(whereCondition.indexOf(" and ") == whereCondition.lastIndexOf(" and ")){//只有一个and 没有or 有between
						flag = 2;
					}
				}
			}else if(whereCondition.startsWith("id") && 
					(whereCondition.indexOf("=")>0 && 
							!((whereCondition.indexOf("in(")>0||whereCondition.indexOf("in (")>0) || 
									(whereCondition.indexOf("not in(")>0||whereCondition.indexOf("not in (")>0)))){
				flag = 1;
			}else{
				flag = 2;
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return flag;
	}
	
}
