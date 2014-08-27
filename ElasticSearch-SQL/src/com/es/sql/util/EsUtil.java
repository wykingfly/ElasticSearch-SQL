package com.es.sql.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class EsUtil {

	private static Client client;
	
	public synchronized static Client getClient(){
		try{
			if(client == null){
				
				Settings settings = ImmutableSettings.settingsBuilder().put("client.transport.sniff", true)
						.put("cluster.name","elasticsearch").build();
				client = new TransportClient(settings)
					.addTransportAddress(new InetSocketTransportAddress("x00", 9300))
					.addTransportAddress(new InetSocketTransportAddress("x01", 9300));
				
			}
		}catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return client;
	}
	
	public static void shutDownClient() {  
        client.close();  
    } 
}
