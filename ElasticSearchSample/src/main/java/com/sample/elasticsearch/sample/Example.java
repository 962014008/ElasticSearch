package com.sample.elasticsearch.sample;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class Example {

	private static final String INDEX_NAME = "bestbuy";
	private static final String TYPE = "product";
	
	private static final String MAPPING_DATA="{\r\n" + 
			"	\"properties\" : {\r\n" + 
			"		\"productClass\": {\r\n" + 
			"			\"type\": \"text\",\r\n" + 
			"			\"index\": false\r\n" + 
			"		},\r\n" + 
			"		\"productSubclass\": {\r\n" + 
			"			\"type\": \"text\",\r\n" + 
			"			\"index\": false\r\n" + 
			"		},\r\n" + 
			"		\"department\": {\r\n" + 
			"			\"type\": \"text\",\r\n" + 
			"			\"index\": false\r\n" + 
			"		}	}\r\n" + 
			"}";

	private ElasticSearchAPI elasticSearchAPI = null;

	public void start() {

		elasticSearchAPI = new ElasticSearchAPI(INDEX_NAME, TYPE);
		elasticSearchAPI.setup();
		elasticSearchAPI.cleanAllExistingIndices();
		elasticSearchAPI.createIndex();
		elasticSearchAPI.addMapping(MAPPING_DATA);
		loadData();
		//elasticSearchAPI.deleteByQuery("type", "Bundle");
		elasticSearchAPI.close();

	}

	public void loadData() {
		Products products = prepareSource();
		
		//one by one
		/*for (Product product : products.product) {
			elasticSearchAPI.loadData(product.toMap(), product.sku);
		}*/
		
		//bulk request
		elasticSearchAPI.loadBulkdata(products);
	}

	private Products prepareSource() {
		Products productRecordRoot = null;
		try {
			File file = new File(this.getClass().getClassLoader()
					.getResource("products_0167_1312126259_to_1312619140.xml").getFile());
			JAXBContext jaxbContext = JAXBContext.newInstance(Products.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			productRecordRoot = (Products) jaxbUnmarshaller.unmarshal(file);
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return productRecordRoot;

	}

}
