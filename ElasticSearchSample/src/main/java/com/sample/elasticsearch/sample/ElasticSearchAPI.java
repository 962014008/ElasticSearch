package com.sample.elasticsearch.sample;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchAPI {

	private static final String HOST_URL = "127.0.0.1";

	private static final int PORT = 9300;

	private String indexName;
	
	private String type;
	
	private TransportClient client = null;
	
	
	public ElasticSearchAPI(String indexName, String type) {
		this.indexName=indexName;
		this.type=type;
	}

	@SuppressWarnings("resource")
	public void setup() {
		
		
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
			        .addTransportAddress(new TransportAddress(InetAddress.getByName(HOST_URL), PORT));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public void addMapping(String mappingData) {
		client.admin().indices().preparePutMapping(indexName).setType(type).setSource(mappingData,XContentType.JSON).get();
	}
	
	public void loadData(Map<String,Object> map,String id ) {
		IndexResponse response = client.prepareIndex(indexName,type,id).setSource(map).get();
		System.out.println(response.toString());	
	}
	public void close() {
		client.close();
	}
	
	public void cleanAllExistingIndices() {
		client.admin().indices().prepareDelete("_all").get();
	}
	
	public void createIndex() {
		client.admin().indices().prepareCreate(indexName).get();
	}
	
	

	public void deleteByQuery(String key, String value) {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
			    .filter(QueryBuilders.matchQuery(key, value)) 
			    .source(indexName)                                  
			    .get();                                             
		System.out.println(response.getDeleted());    		
	}
}
