package com.es.api.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

import net.sf.json.JSONObject;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.es.sql.util.EsUtil;
import com.spatial4j.core.shape.impl.InfBufLine;

/**
 * 
 * @author wangyong
 *
 */
public class BankTimeTest {

	public static void main(String[] args) {
		/*PutMappingRequest mRequest = Requests.putMappingRequest("bank2").type("account2").source(getMapping());
		Client client = EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
		client.admin().indices().prepareCreate("bank2").execute().actionGet(); 
		client.admin().indices().putMapping(mRequest).actionGet();
		
		client.close();*/
		
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
			
			StringBuffer stringBuffer = new StringBuffer("2014-08-");
			Random random = new Random();
			int day = random.nextInt(31);
			int hours = random.nextInt(24);
			int min = random.nextInt(60);
			int sec = random.nextInt(60);
			if(day==0)day=1;
			stringBuffer.append(day<10?"0"+day:day).append("T").append(hours<10?"0"+hours:hours)
			.append(":").append(min<10?"0"+min:min).append(":").append(sec<10?"0"+sec:sec).append("Z");
			
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
			.field("createtime",stringBuffer.toString())
			.endObject();
			
			indexContent(doc);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void indexContent(XContentBuilder content){
		try{
			
			IndexResponse response = EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"}).prepareIndex("bank2", "account2").setSource(content).execute().actionGet();
			System.out.println(response.getId() + "====" + response.getIndex() + "====" + response.getType());
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static XContentBuilder getMapping(){
		XContentBuilder mapping = null;
		try{
			mapping = XContentFactory.jsonBuilder().startObject().startObject("account2").startObject("properties");
			mapping.startObject("account_number").field("type", "integer").field("store","yes").endObject()
				.startObject("balance").field("type","integer").field("store","yes").endObject()
				.startObject("firstname").field("type","string").field("store","yes").endObject()
				.startObject("lastname").field("type","string").field("store","yes").endObject()
				.startObject("age").field("type","integer").field("store","yes").endObject()
				.startObject("gender").field("type","string").field("store","yes").endObject()
				.startObject("address").field("type","string").field("store","yes").endObject()
				.startObject("employer").field("type","string").field("store","yes").endObject()
				.startObject("email").field("type","string").field("store","yes").endObject()
				.startObject("city").field("type","string").field("store","yes").endObject()
				.startObject("state").field("type","string").field("store","yes").endObject()
				.startObject("createtime").field("type","date").field("store","yes").endObject();
			mapping.endObject().endObject().endObject();
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return mapping;
	}
}
