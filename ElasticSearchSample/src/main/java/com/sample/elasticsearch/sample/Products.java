	package com.sample.elasticsearch.sample;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Products {
	@XmlElement(name = "product")
	List<Product> product;
}