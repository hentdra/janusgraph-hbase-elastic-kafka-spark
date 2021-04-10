package com.ebdesk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PolriConfig implements Serializable {
	private final static String HMASTER = "namenode004.cluster02.bt";
	private final static String ZOOKEEPER = "master001.cluster02.bt,namenode004.cluster02.bt,namenode005.cluster02.bt";
	private final static String BROKER = "l=master04.cluster04.bt:6667,namenode01.cluster04.bt:6667,namenode02.cluster04.bt:6667";
	private final static String BROKER2 = "l=master001.cluster02.bt:6667,namenode004.cluster02.bt:6667,namenode005.cluster02.bt:6667";
	
//	private final static String HMASTER = "namenode01.cluster4.ph";
//	private final static String ZOOKEEPER= "datanode01.cluster4.ph,namenode02.cluster4.ph,namenode01.cluster4.ph";
	
	final static String TWITTER_POST = "enh-twitter-post";
	final static String FB_POST = "sc-fb-post";
	final static String GPLUS_POST = "gplus-post";
	final static String INSTA_POST = "instagram-post";
	final static String LINKEDIN_POST = "linkedin-post";
	final static String YOUTUBE_POST = "youtube-post";
	final static String INSERT_TW_DATA = "cyber-tw-data";
	final static String CYBER_ISSUE_STREAMING = "cyber-issue-streaming";
	
	private static Configuration fbConfig;
	private static Configuration twConfig;
	private static Configuration twInsertDataConfig;
	private static Configuration instaConfig;
	private static Configuration linkedinConfig;
	private static Configuration gplusConfig;
	private static Configuration cyberIssueStreming;
	
	private Map<String, Object> kafkaParam;
	private String groupId;
	
	public PolriConfig(){
		try {
			fbConfig = hbaseConfig("cyber-fb-post");
			twConfig = hbaseConfig("cyber-tw-post");
			instaConfig = hbaseConfig("cyber-insta-post");
			linkedinConfig = hbaseConfig("cyber-linkedin-post");
			gplusConfig = hbaseConfig("cyber-gp-post");
			cyberIssueStreming = hbaseConfig("cyber-issue-streaming");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	};
	
	public PolriConfig setKafkaParam(String groupId){
		this.groupId = groupId;
		kafkaParam = setKafka();
		return this;
	}

	private static class PolriConfigHolder{
		private static final PolriConfig INSTANCE = new PolriConfig();
	}
	
	public static PolriConfig getInstance(){
		return PolriConfigHolder.INSTANCE;
	}
	
	private Map<String, Object> setKafka(){
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", BROKER);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", groupId);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		return kafkaParams;
	}

	public static Configuration hbaseConfig(String tableName) throws IOException{
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", HMASTER);
		config.set("zookeeper.znode.parent", "/hbase-unsecure2");
		config.setInt("timeout", 5000);
		config.set("hbase.zookeeper.quorum", ZOOKEEPER);
		config.set("hbase.client.keyvalue.maxsize", "0");
		config.set("hbase.client.scanner.timeout.period", "100000");
		config.set("hbase.rpc.timeout", "100000");
		config.set("mapred.output.dir", "/tmp");
		config.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
		
		Job newAPIJobConfiguration = Job.getInstance(config);
		newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);
		return newAPIJobConfiguration.getConfiguration();
	}


	public Map<String, Object> getKafkaParam() {
		return kafkaParam;
	}
	
	public Properties setKafkaProducer()
	{
		Properties props = new Properties();
		props.put("bootstrap.servers", "master001.cluster02.bt:6667,namenode004.cluster02.bt:6667,namenode005.cluster02.bt:6667");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("max.partition.fetch.bytes", 10485720);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}
	
}
