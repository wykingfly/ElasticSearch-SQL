package com.es.api.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import net.sf.json.JSONObject;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.es.sql.util.EsUtil;

public class BankContentIndex {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path = "E:\\new\\accounts.json";
		readFile(path);
	}

	public static void readFile(String path){
		try{
			File file = new File(path);// Text文件
			BufferedReader br = new BufferedReader(new FileReader(file));// 构造一个BufferedReader类来读取文件
			String s = null;
			while ((s = br.readLine()) != null) {// 使用readLine方法，一次读一行
				System.out.println(s);
				if(s.indexOf("index")<0){
					createXContentBuilder(s);
				}
			}
			br.close();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createXContentBuilder(String content){
		try{
			JSONObject bean = JSONObject.fromString(content);
			XContentBuilder doc = XContentFactory.jsonBuilder().startObject();
			doc.field("account_number",bean.get("account_number"))
			.field("balance",bean.get("balance"))
			.field("firstname", bean.get("firstname"))
			.field("lastname", bean.get("lastname"))
			.field("age", bean.get("age"))
			.field("gender", bean.get("gender"))
			.field("address", bean.get("address"))
			.field("employer", bean.get("employer"))
			.field("email", bean.get("email"))
			.field("city", bean.get("city"))
			.field("state", bean.get("state"))
			.endObject();
			
			indexContent(doc);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void indexContent(XContentBuilder content){
		try{
			
			IndexResponse response = EsUtil.getClient().prepareIndex("bank1", "account1").setSource(content).execute().actionGet();
			System.out.println(response.getId() + "====" + response.getIndex() + "====" + response.getType());
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
