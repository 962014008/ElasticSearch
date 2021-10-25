package com.sample.elasticsearch.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Example {

	private static final String INDEX_NAME = "bestbuy";
	private static final String TYPE = "product";

	private static final String MAPPING_DATA= "{\r\n" +
			"    \"properties\": {\r\n" +
			"      \"productTemplate\": {\r\n" +
			"        \"type\": \"keyword\",\r\n" +
			"        \"index\": true\r\n" +
			"      },\r\n" +
			"      \"manufacturer\": {\r\n" +
			"        \"type\": \"keyword\"\r\n" +
			"      },\r\n" +
			"      \"shortDescription\": {\r\n" +
			"        \"type\": \"text\",\r\n" +
			"        \"index\": true\r\n" +
			"      },\r\n" +
			"      \"longDescription\": {\r\n" +
			"        \"type\": \"text\",\r\n" +
			"        \"index\": true\r\n" +
			"      },\r\n" +
			"      \"color\": {\r\n" +
			"        \"type\": \"keyword\"\r\n" +
			"      },\r\n" +
			"      \"customerReviewCount\":{\r\n" +
			"        \"type\":\"long\"\r\n" +
			"      },\r\n" +
			"      \"customerReviewAverage\":{\r\n" +
			"        \"type\":\"byte\"\r\n" +
			"      },\r\n" +
			"      \"customerTopRated\":{\r\n" +
			"        \"type\":\"boolean\"\r\n" +
			"      },\r\n" +
			"      \"salePrice\":{\r\n" +
			"        \"type\":\"half_float\"\r\n" +
			"      },\r\n" +
			"      \"shippingCost\":{\r\n" +
			"        \"type\":\"half_float\"\r\n" +
			"      }\r\n" +
			"    }\r\n" +
			"  }\r\n" +
			"}";
	
	private ElasticSearchAPI elasticSearchAPI = null;
	
	boolean isDataLoaded = false;

	public void start() {

		Object lock = new Object();
		

		elasticSearchAPI = new ElasticSearchAPI(INDEX_NAME, TYPE);
		elasticSearchAPI.setup();
		elasticSearchAPI.cleanAllExistingIndices();
		elasticSearchAPI.createIndex();
		elasticSearchAPI.addMapping(MAPPING_DATA);
		Thread threadDataLoad = new Thread(() -> {
			System.out.println("Data Load Thread is running");
			synchronized (lock) {
				try {
					isDataLoaded = loadData();
				} catch (IOException e) {
					e.printStackTrace();
				}
				lock.notify();

			}
		});

		Thread threadDelete = new Thread(() -> {
			System.out.println("delete thread Initiated");
			synchronized (lock) {
				while (!isDataLoaded) {
					try {
						lock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println("delete being performed");
				elasticSearchAPI.deleteByQuery("name", "手機");
			}
		});
		threadDataLoad.run();
		threadDelete.run();
		
		elasticSearchAPI.close();

	}

	public boolean loadData() throws IOException {
		List<Products> productRecordRootList = prepareSource();

		// one by one
		/*
		 * for (Product product : products.product) {
		 * elasticSearchAPI.loadData(product.toMap(), product.sku); }
		 */

		// bulk request
		return elasticSearchAPI.loadBulkdata(productRecordRootList);
	}

	private List<Products>  prepareSource()  {
		List<Products> productRecordRootList = new ArrayList<Products>();
		for (int i = 1; i <= 10; i++) {
			Products products = new Products();
			List<Product> productsList = new ArrayList<>();
			for (int j = 1; j <= 10; j++) {
				Product product = new Product();
				product.sku =  String.valueOf(i * j);
				product.productId = String.valueOf(i * j);
				product.name = "手機";
				product.type = "Bundle";
				productsList.add(product);
			}
			products.product = productsList;
			productRecordRootList.add(products);
		}
//		try {
//			File folder = new File("D:\\IDEA_WorkPlace\\springbootES\\src\\main\\resources");
//			for (final File fileEntry : folder.listFiles()) {
//			        if (!fileEntry.isDirectory()) {
//			           //System.out.println();
//			           File file = new File(fileEntry.getPath());
//						JAXBContext jaxbContext = JAXBContext.newInstance(Products.class);
//						Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
//						productRecordRootList.add((Products) jaxbUnmarshaller.unmarshal(file));
//			        }
//			 }
//
//		} catch (JAXBException e) {
//			e.printStackTrace();
//		}
		return productRecordRootList;

	}

}
