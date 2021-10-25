package com.sample.elasticsearch.sample;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

public class ElasticSearchAPI {

	private static final String HOST_URL = "127.0.0.1";

	private static final int PORT = 9200;

	private String indexName;

	private String type;

	private RestHighLevelClient client = null;

	public ElasticSearchAPI(String indexName, String type) {
		this.indexName = indexName;
		this.type = type;
	}

	@SuppressWarnings("resource")
	public void setup() {

		try {
			client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(HOST_URL,PORT,"http")));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void addMapping(String mappingData) {
		PutMappingRequest putMappingRequest = new PutMappingRequest(indexName).source(mappingData, XContentType.JSON);
		try {
			Boolean isAcknowledged = client.indices()
					.putMapping(putMappingRequest, RequestOptions.DEFAULT)
					.isAcknowledged();
			System.out.println("putMapping是否成功: " + isAcknowledged);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// client.admin().indices().preparePutMapping(indexName).setType(type).setSource(mappingData, XContentType.JSON).get();
	}

	public void loadData(Map<String, Object> map, String id) {
		IndexRequest request = new IndexRequest(indexName).id(id).source(map);
		IndexResponse response = null;
		try {
			response = client.index(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(response.toString());
	}

	public boolean loadBulkdata(List<Products> productRecordRootList) throws IOException {

		//创建批量请求
		BulkRequest bulkRequest = new BulkRequest();
		// BulkRequestBuilder bulkRequest = client.prepareBulk();
		// either use client#prepare, or use Requests# to directly build index/delete
		// requests
		for (Products products : productRecordRootList) {
			for (Product product : products.product) {
				bulkRequest.add(new IndexRequest(indexName).id(product.sku).source(product.toMap()));
				// bulkRequest.add(client.prepareIndex(indexName, type, product.sku).setSource(product.toMap()));
			}
		}
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		// BulkResponse bulkResponse = bulkRequest.get();
		System.out.println("Bulk Loda performed");
		return !bulkResponse.hasFailures();
	}

	public void close() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void cleanAllExistingIndices() {
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("_all");
		try {
			client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// client.admin().indices().prepareDelete("_all").get();
	}

	public void createIndex() {
		try {
			client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// client.admin().indices().prepareCreate(indexName).get();
	}

	public void deleteByQuery(String key, String value) {


		DeleteByQueryRequest delRequest = new DeleteByQueryRequest(indexName);
		delRequest.setQuery(QueryBuilders.termQuery(key, value));
		BulkByScrollResponse response =  null;
		try {
			response = client.deleteByQuery(delRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!response.getBulkFailures().isEmpty()) {
			System.out.println("不成功");
			response.getBulkFailures().forEach(item -> System.out.println(item.getMessage()));
		}
		System.out.println("delete being performed: " + response.getDeleted());

//		BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
//				.filter(QueryBuilders.matchQuery(key, value)).source(indexName).get();
//		System.out.println(response.getDeleted());
	}
}
