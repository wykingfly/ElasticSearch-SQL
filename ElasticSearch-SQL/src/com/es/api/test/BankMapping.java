package com.es.api.test;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.es.sql.util.EsUtil;

/**
 * 项目测试对应的测试index
 * @author wangyong
 *
 */
public class BankMapping {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		PutMappingRequest mRequest = Requests.putMappingRequest("bank1").type("account1").source(getMapping());
		Client client = EsUtil.getClient();
		client.admin().indices().prepareCreate("bank1").execute().actionGet(); 
		client.admin().indices().putMapping(mRequest).actionGet();
		
		client.close(); 
	}

	private static XContentBuilder getMapping(){
		XContentBuilder mapping = null;
		try{
			mapping = XContentFactory.jsonBuilder().startObject().startObject("account1").startObject("properties");
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
				.startObject("state").field("type","string").field("store","yes").endObject();
			mapping.endObject().endObject().endObject();
			
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return mapping;
	}
}
