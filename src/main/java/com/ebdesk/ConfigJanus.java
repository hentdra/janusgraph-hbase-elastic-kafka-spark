package com.ebdesk;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class ConfigJanus {
	
	public static void main(String[] args) {
		JanusGraph graph = JanusGraphFactory.open("conf/hbase2.properties");
		GraphTraversalSource as = graph.traversal();
//		System.out.println(as.V().valueMap());
		as.tx().commit();
		
		System.out.println("SELESAI");
		System.exit(0);
	}
	
//	public static JanusGraph graph() {
//
//		BaseConfiguration conf = new BaseConfiguration();
//
//		conf.setProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
//		conf.setProperty("storage.backend", "hbase");
//		// conf.setProperty("storage.hostname", "192.168.20.122,192.168.20.123");
//		conf.setProperty("storage.hostname", "127.0.0.1");
//		conf.setProperty("storage.hbase.table", "tester");
//		// conf.setProperty("graph.set-vertex-id", true);
//		 conf.setProperty("storage.hbase.ext.zookeeper.znode.parent", "/hbase");
//		 conf.setProperty("storage.hbase.ext.hbase.zookeeper.property.clientPort", 2181);
//		// conf.setProperty("index.tester.backend", "tester");
//		// conf.setProperty("index.tester.solr.mode", "cloud");
//		// conf.setProperty("index.tester.solr.zookeeper-url",
//		// "datanode01.cluster4.ph:2181/solr,namenode01.cluster4.ph:2181/solr,namenode02.cluster4.ph:2181/solr");
//		// conf.setProperty("index.tester.configset", "tester");
//		conf.setProperty("cache.db-cache", true);
//		conf.setProperty("cache.db-cache-clean-wait", 20);
//		conf.setProperty("cache.db-cache-time", 180000);
//		conf.setProperty("cache.db-cache-size", 0.5);
//
//		JanusGraph gr = JanusGraphFactory.open(conf);
//
//		return gr;
//	}

}
