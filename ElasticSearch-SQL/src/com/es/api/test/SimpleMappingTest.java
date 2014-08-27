package com.es.api.test;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import com.es.sql.util.EsUtil;

public class SimpleMappingTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Client client = EsUtil.getClient();
		String indexName = "index1";
		String typeName = "type1";
		
		createMapping(client, indexName, typeName);
		
		System.out.println(batchAddDocument(client, indexName, typeName));
		
		search(client, indexName, typeName);
		
	}
	
	/**
	 * 字符串搜索
	 * @param client
	 * @param indexName
	 * @param typeName
	 */
	public static void search(Client client,String indexName,String typeName){
		try{
			SearchResponse response = client.prepareSearch(indexName).setTypes(typeName)
					.setQuery(new QueryStringQueryBuilder("png|jpg").field("thumb"))
					.setFrom(0).setSize(50).execute().actionGet();
			System.out.println(response.toString());
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 批量建立index
	 * @param client
	 * @param indexName
	 * @param typeName
	 * @return
	 */
	private static Integer batchAddDocument(Client client,String indexName,String typeName){
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		try{
			for(int i = 0;i<100;i++){
				try{
					bulkRequestBuilder.add(client.prepareIndex(indexName, typeName, i+"")
							.setSource(XContentFactory.jsonBuilder().startObject()
									.field("id",i+"")
									.field("thumb","http://www.chepoo.com/imges/"+i+".jpg")
									.endObject()
									)
					);
					
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			try{
				bulkRequestBuilder.add(client.prepareIndex(indexName, typeName, ""+101)   
			  			.setSource(XContentFactory.jsonBuilder()
								.startObject()
									.field("id", ""+101)
									.field("thumb", "http://www.chepoo.com/imges/"+5+".png")
								.endObject())
					        );
				}catch(Exception e){
					e.printStackTrace();
				}
			bulkRequestBuilder.execute().actionGet();
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return bulkRequestBuilder.numberOfActions();
	}
	
	/**
	 * 构建index mapping
	 * @param client
	 * @param indexName
	 */
	private static void createMapping(Client client,String indexName,String typeName){
		try{
			XContentBuilder mapping = XContentFactory.jsonBuilder()
			.startObject()
				.startObject(typeName)
					.startObject("properties")
						.startObject("id")
							.field("type","long")
							.field("store","yes")
							.field("index","no_analyzed")
							.field("include_in_all","false")
						.endObject()
						.startObject("thumb")
							.field("type","string")
							.field("store","yes")
							.field("include_in_all","false")
						.endObject()
					.endObject()
				.endObject()
			.endObject();
		
		client.admin().indices().putMapping(Requests.putMappingRequest(indexName).type(typeName).source(mapping)).actionGet();
		
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	
}
