package com.es.sql.util;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.mvel2.optimizers.impl.refl.nodes.ThisValueAccessor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * elasticsearch 工具类
 * @author wangyong
 *
 */
public class EsUtil {

	private static Client client;
	
	private static boolean sniff;
	private static String clusterName;
	private static String[] hosts;
	
	public static long precisionThreshold = 1000;
	public static boolean ignore_unavailable = true;
	public static boolean allow_no_indices = true;
	
	public static long minDocCount = 0;
	
	/**
	 * 获取客户端
	 * @return
	 */
	public synchronized static Client getClient(){
		try{
			if(client == null){
				
				/*Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
						.put("cluster.name","elasticsearch").build();
				client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("x00", 9300))
					.addTransportAddress(new InetSocketTransportAddress("x01", 9300));*/
				client = initClient(isSniff(), getClusterName(), getHosts());
				
			}
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return client;
	}
	
	/**
	 * 初始化信息
	 * @param sniff
	 * @param clusterName
	 * @param hosts
	 * @return
	 */
	public synchronized static Client initClient(boolean sniff,String clusterName,String...hosts ){
		try{
			setSniff(sniff);
			setClusterName(clusterName);
			setHosts(hosts);
			
			if(client == null){
				
				Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", sniff)
						.put("cluster.name",clusterName).build();
				if(hosts!=null && hosts.length>0){
					List<InetSocketTransportAddress> list = new ArrayList<InetSocketTransportAddress>();
					for(String hostport : hosts){
						String[] arr = hostport.split(":");
						if(arr.length==2){
							list.add(new InetSocketTransportAddress(arr[0], Integer.parseInt(arr[1])));
						}
					}
					if(list.size()>0){
						InetSocketTransportAddress[] staarr = new InetSocketTransportAddress[list.size()];
						for(int i=0;i<list.size();i++){
							staarr[i] = list.get(i);
						}
						
						client = new TransportClient(settings).addTransportAddresses(staarr);
					}
					
				}
				
				
			}
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return client;
	}
	
	public static void shutDownClient() {  
        client.close();  
    }

	public static boolean isSniff() {
		return sniff;
	}

	public static void setSniff(boolean sniff) {
		EsUtil.sniff = sniff;
	}

	public static String getClusterName() {
		return clusterName;
	}

	public static void setClusterName(String clusterName) {
		EsUtil.clusterName = clusterName;
	}

	public static String[] getHosts() {
		return hosts;
	}

	public static void setHosts(String[] hosts) {
		EsUtil.hosts = hosts;
	}

	public static long getPrecisionThreshold() {
		return precisionThreshold;
	}

	public static void setPrecisionThreshold(long precisionThreshold) {
		EsUtil.precisionThreshold = precisionThreshold;
	}

	public static boolean isIgnore_unavailable() {
		return ignore_unavailable;
	}

	public static void setIgnore_unavailable(boolean ignore_unavailable) {
		EsUtil.ignore_unavailable = ignore_unavailable;
	}

	public static boolean isAllow_no_indices() {
		return allow_no_indices;
	}

	public static void setAllow_no_indices(boolean allow_no_indices) {
		EsUtil.allow_no_indices = allow_no_indices;
	}

	public static long getMinDocCount() {
		return minDocCount;
	}

	public static void setMinDocCount(long minDocCount) {
		EsUtil.minDocCount = minDocCount;
	} 
	
	
	
}
