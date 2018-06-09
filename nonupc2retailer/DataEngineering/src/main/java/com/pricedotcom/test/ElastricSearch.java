package com.pricedotcom.test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElastricSearch {

	public static void main(String args[]) throws UnknownHostException{
		Settings settings = Settings.builder()
		        .put("cluster.name", "099573254109:price-production").build();
		TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("search-price-production-e6jhql3cozjbq2m4rdh6am27jy.us-east-1.es.amazonaws.com"), 80));
		
	}
}
