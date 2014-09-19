package com.es.sql.query;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.lucene.search.OrFilter;
import org.elasticsearch.index.fielddata.AtomicFieldData.Order;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TemplateQueryBuilder;
import org.elasticsearch.index.search.stats.SearchStats.Stats;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.statistical.StatisticalFacetBuilder;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import com.es.sql.util.EsUtil;

/**
 * 原始elasticsearch java api 查询验证
 * @author wangyong
 *
 */
public class TestQuery {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

	}
	
	@Test
	/**
	 * 获取index.type下面的mapping信息
	 */
	public void testQuery19(){
		try{
			Client client =  EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			GetMappingsRequest request = new GetMappingsRequest();
			request.indices("segments.20140902-new","segments.20140903-new").types("982da2ae92188e5f73fbf7f82e41ed65-item");
			
			GetMappingsResponse response = client.admin().indices().getMappings(request).actionGet();
			
			ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> tmp = response.getMappings();
			
			for(ObjectCursor<String> key :tmp.keys()){
				ImmutableOpenMap<String, MappingMetaData> map= tmp.get(key.value);
				System.out.println(key.value);
				
				for(ObjectCursor<String> ck:map.keys()){
					System.out.println(ck.value);
					MappingMetaData mmData = map.get(ck.value);
					Map<String, Object> cMap = mmData.sourceAsMap();
					for(Map.Entry<String, Object> entry:cMap.entrySet()){
						System.out.println(entry.getKey());
						//System.out.println(entry.getValue().toString());
						
						JSONObject bean = JSONObject.fromString(entry.getValue().toString());
						
						//System.out.println(bean.get("context").toString());
						
						JSONObject context = JSONObject.fromString(bean.get("context").toString());
						JSONObject context_c = JSONObject.fromString(context.get("properties").toString());
						
						Iterator iterator = context_c.keys();
						
						while(iterator.hasNext()){
							String ckey = (String) iterator.next();
							System.out.println(ckey);
						}
					}
				}
			}
			
			//System.out.println(response.toString());
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	/**
	 * 获取index下面的mapping信息
	 */
	public void testQuery18(){
		try{
			Client client =  EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			GetMappingsRequest request = new GetMappingsRequest();
			request.indices("segments.20140902-new");
			
			GetMappingsResponse response = client.admin().indices().getMappings(request).actionGet();
			
			ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> tmp = response.getMappings();
			
			for(ObjectCursor<String> key :tmp.keys()){
				ImmutableOpenMap<String, MappingMetaData> map= tmp.get(key.value);
				System.out.println(key.value);
				
				for(ObjectCursor<String> ck:map.keys()){
					System.out.println(ck.value);
					MappingMetaData mmData = map.get(ck.value);
					Map<String, Object> cMap = mmData.sourceAsMap();
					for(Map.Entry<String, Object> entry:cMap.entrySet()){
						System.out.println(entry.getKey());
						//System.out.println(entry.getValue().toString());
						
						JSONObject bean = JSONObject.fromString(entry.getValue().toString());
						
						//System.out.println(bean.get("context").toString());
						
						JSONObject context = JSONObject.fromString(bean.get("context").toString());
						JSONObject context_c = JSONObject.fromString(context.get("properties").toString());
						
						Iterator iterator = context_c.keys();
						while(iterator.hasNext()){
							String ckey = (String) iterator.next();
							System.out.println(ckey);
						}
					}
				}
			}
			
			//System.out.println(response.toString());
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	/**
	 * 垂直直方图-时间
	 */
	public void testQuery17_1(){
		try{
			Client client =  EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			AggregationBuilder aggregationBuilder2 = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.dateHistogram("datehistogram").field("when.d").interval(Interval.minutes(5)).format("HH:mm")
					);
			FilterBuilder filterBuilder = FilterBuilders.termFilter("appid.s", "982da2ae92188e5f73fbf7f82e41ed65");
			
			SearchResponse response = client.prepareSearch("events.20140909").setTypes("events")
					.setPostFilter(filterBuilder)
					.addAggregation(aggregationBuilder2)
					.setFrom(0).setSize(1)
							.execute().actionGet();
			
			System.out.println(response);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	/**
	 * 垂直直方图-时间
	 */
	public void testQuery17(){
		try{
			Client client =  EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.queryString("982da2ae92188e5f73fbf7f82e41ed65").field("appid.s"))
					.must(QueryBuilders.queryString("item").field("what.s"));
			
			SearchResponse response = client.prepareSearch("segments.20140902").setTypes("segments").setQuery(queryBuilder)
					.setFrom(0).setSize(1)
							.execute().actionGet();
			
			System.out.println(response);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	/**
	 * 垂直直方图-时间
	 */
	public void testQuery16(){
		try{
			Client client =  EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			AggregationBuilder aggregationBuilder = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.dateHistogram("datehistogram").field("createtime").interval(Interval.hours(3))
					);
			
			//Interval.HOUR  hours()
			//Interval.DAY   days()
			//Interval.MINUTE minutes()
			//Interval.MONTH
			//Interval.SECOND  seconds()
			//Interval.YEAR
			//Interval.WEEK   weeks()
			//Interval.QUARTER
			
			AggregationBuilder aggregationBuilder2 = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.dateHistogram("datehistogram").field("createtime").interval(Interval.HOUR)
					);
			
			SearchResponse response = client.prepareSearch("bank2").addAggregation(aggregationBuilder2)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	@Test
	/**
	 * 垂直直方图-普通
	 */
	public void testQuery15(){
		try{
			Client client = EsUtil.initClient(true, "elasticsearch", new String[]{"x00:9300","x01:9300"});
			
			AggregationBuilder aggregationBuilder = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.histogram("xx").field("age").interval(5)
					);
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(aggregationBuilder)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	/**
	 * agg range
	 */
	public void testQuery14(){
		try{
			Client client = EsUtil.getClient();
			
			AggregationBuilder aggregationBuilder = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.range("age-range").field("age")
					.addUnboundedTo("*-20", 20)
					.addRange("20-25", 20, 25)
					.addRange("25-30", 25,30)
					.addRange("30-35", 30, 35)
					.addRange("35-40", 35, 40)
					.addUnboundedFrom("40-*", 40)
					.subAggregation(AggregationBuilders.stats("x").field("balance")
					));
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(aggregationBuilder)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	@Test
	/**
	 * stats
	 */
	public void testQuery13(){
		try{
			Client client = EsUtil.getClient();
			
			AggregationBuilder aggregationBuilder = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.terms("age").field("age")
					.subAggregation(AggregationBuilders.stats("x").field("balance"))
					);
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(aggregationBuilder)
					.addFields(new String[]{"account_number","balance","age","tt"})
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	/**
	 * 设置显示的fields
	 */
	public void testQuery12(){
		try{
			Client client = EsUtil.getClient();
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(AggregationBuilders.stats("x").field("balance"))
					.addFields(new String[]{"account_number","balance","age"})
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testQuery11(){
		try{
			Client client = EsUtil.getClient();
			
			StatisticalFacetBuilder statisticalFacetBuilder = FacetBuilders.statisticalFacet("facet_balance")
					.field("balance");
			
			SearchResponse response = client.prepareSearch("bank").addFacet(statisticalFacetBuilder)
					.addFields(new String[]{"account_number","balance","age"})
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery10(){
		try{
			Client client = EsUtil.getClient();
			
			AggregationBuilder aggregation = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.terms("age").field("age")
					.subAggregation(AggregationBuilders.dateHistogram("x").field("timestamp").interval(new Interval("1").HOUR))
					.subAggregation(AggregationBuilders.sum("b").field("balance")
							)
					);
			
			//StatisticalFacetBuilder statisticalFacetBuilder = FacetBuilders.statisticalFacet("facet_balance").field("balance");
			
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(aggregation)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery9(){
		try{
			Client client = EsUtil.getClient();
			/*FilterBuilder filterBuilder = FilterBuilders.andFilter(
					FilterBuilders.queryFilter(QueryBuilders.queryString("F").field("gender")),
					FilterBuilders.rangeFilter("balance").gt(30000).filterName("balance"),
					FilterBuilders.inFilter("age", new Integer[]{30,31,32}).filterName("age")
					);*/
			Terms.Order order = org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.count(false);
			/*AggregationBuilder aggregation = AggregationBuilders.global("all").subAggregation(AggregationBuilders.filter("xx").filter(filterBuilder)
					.subAggregation(AggregationBuilders.terms("state").field("state").order(org.elasticsearch.search.aggregations.bucket.terms.Terms.Order.count(false))));
			*/
			AggregationBuilder aggregation = AggregationBuilders.global("all").subAggregation(
					AggregationBuilders.terms("age").field("age")
					.subAggregation(AggregationBuilders.sum("b").field("balance"))
					);
			
			SearchResponse response = client.prepareSearch("bank").addAggregation(aggregation)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	@Test
	public void testQuery8(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
			AggregationBuilders.global("").subAggregation(AggregationBuilders.dateHistogram(""));
			SortBuilder sort = SortBuilders.fieldSort("age").order(SortOrder.DESC);
			SortBuilder sort1 = SortBuilders.fieldSort("account_number").order(SortOrder.ASC);
			
			SearchResponse response = client.prepareSearch("bank").setQuery(queryBuilder).addSort(sort).addSort(sort1)
					.setFrom(0).setSize(20)
							.execute().actionGet();
			
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testQuery7(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("firstname", "*beck*");
			AggregationBuilders.global("").subAggregation(AggregationBuilders.dateHistogram(""));
			//最终由type决定了数据获取
			//bank.account=bank.account,bank1=bank.account,bank.account1
			//bank.account,bank1.account1 = bank,bank1
			//可以看出最终是index和type的组合，如果有对应的关系就可以查询对应的数据
			SearchResponse response = client.prepareSearch("bank","bank1").setTypes("account","account1").setQuery(queryBuilder)
					.setFrom(0).setSize(100)
							.execute().actionGet();
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testQuery6(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("firstname", "*beck*");
			AggregationBuilders.global("").subAggregation(AggregationBuilders.dateHistogram(""));
			SearchResponse response = client.prepareSearch("bank").setTypes("account").setQuery(queryBuilder)
					.setFrom(0).setSize(100)
							.execute().actionGet();
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery5(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.fuzzyLikeThisFieldQuery("firstname").likeText("Beck").queryName("like");
			QueryBuilder queryBuilder2 = QueryBuilders.fuzzyLikeThisQuery("firstname").likeText("Beck").queryName("like");
			SearchResponse response = client.prepareSearch("bank").setQuery(queryBuilder2)
					.setFrom(0).setSize(100)
							.execute().actionGet();
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery4(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.inQuery("state", new String[]{"id","wy"});
			SearchResponse response = client.prepareSearch("bank").setQuery(queryBuilder)
					.setFrom(0).setSize(100)
							.execute().actionGet();
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testQuery3(){
		try{
			Client client = EsUtil.getClient();
			FilterBuilder filterBuilder = FilterBuilders.andFilter(
					FilterBuilders.queryFilter(QueryBuilders.queryString("F").field("gender")),
					FilterBuilders.rangeFilter("balance").gt(30000).filterName("balance"),
					FilterBuilders.inFilter("age", new Integer[]{30,31,32}).filterName("age")
					);
			AggregationBuilder aggregation = AggregationBuilders.global("all").subAggregation(AggregationBuilders.filter("xx").filter(filterBuilder)
					.subAggregation(AggregationBuilders.terms("state").field("state")));
			SearchResponse response = client.prepareSearch("bank").setPostFilter(filterBuilder)
					.setFrom(0).setSize(10)
							.execute().actionGet();
			/*SearchResponse response = client.prepareSearch("bank").addAggregation(aggregation)
					.setFrom(0).setSize(10)
							.execute().actionGet();*/
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	@Test
	public void testQuery2(){
		try{
			Client client = EsUtil.getClient();
			QueryBuilder queryBuilder = QueryBuilders.rangeQuery("age").gt(10);
			QueryBuilder queryBuilder2 = QueryBuilders.matchAllQuery();
			SearchResponse response = client.prepareSearch("bank").setQuery(queryBuilder2)
					.addAggregation(AggregationBuilders.global("_counter").subAggregation(AggregationBuilders.terms("state").field("state").size(100)
							.subAggregation(AggregationBuilders.terms("city").field("city").size(100))
							.subAggregation(AggregationBuilders.cardinality("distinct").field("account_number"))))
							.setFrom(0).setSize(100)
							.execute().actionGet();
			System.out.println(response);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	@Test
	public void testQuery1(){
		Client client = EsUtil.getClient();
		
		QueryBuilders.boolQuery().should(
				QueryBuilders.boolQuery().must(QueryBuilders.termQuery("location", "beijing"))
				.must(QueryBuilders.rangeQuery("age").from(18).to(35))
				.mustNot(QueryBuilders.inQuery("userid", "1","2","3"))
				).should(QueryBuilders.termQuery("is_pre", 1))
				.should(QueryBuilders.fuzzyQuery("val", "china"))
				;
		
		
		QueryBuilders.multiMatchQuery("China", "location","state");
		QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "park"))
								.must(QueryBuilders.termQuery("home", "beijing"))
								.mustNot(QueryBuilders.termQuery("isRealMan", false))
								.should(QueryBuilders.termQuery("location", "beijing"));
		
		QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), 
				FilterBuilders.boolFilter()
					.must(FilterBuilders.termFilter("x", "y"))
					
				);
		//TemplateQueryBuilder
		FilterBuilder fBuilder = FilterBuilders.andFilter(
				FilterBuilders.termFilter("appid", "jaidjisleiis"),
				FilterBuilders.termFilter("what", "item"),
				FilterBuilders.rangeFilter("ds").from("2014-08-12").to("2014-08-13"),
				FilterBuilders.orFilter(
						FilterBuilders.termFilter("memo", "1"),
						FilterBuilders.inFilter("val", "1","2")
						),
				FilterBuilders.missingFilter("ip")
				
				);
		
		
		client.prepareSearch("index").setTypes("segment").setPostFilter(fBuilder)
		.addAggregation(AggregationBuilders.global("ds").subAggregation(AggregationBuilders.global("context.serverid").subAggregation(AggregationBuilders.cardinality("who"))))
		.addSort(SortBuilders.fieldSort("context.serverid").sortMode("desc")).setFrom(0).setSize(100).execute().addListener(new ActionListener<SearchResponse>() {
			
			@Override
			public void onResponse(SearchResponse paramResponse) {
				Aggregations aggregations = paramResponse.getAggregations();
				Map<String, Aggregation>  map = aggregations.getAsMap();
				for(Map.Entry<String, Aggregation> entry : map.entrySet()){
					String key = entry.getKey();
					Aggregation aggr = entry.getValue();
					
				}
			}
			
			@Override
			public void onFailure(Throwable paramThrowable) {
				
			}
		});
		
	}

}
