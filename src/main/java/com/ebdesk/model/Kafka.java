package com.ebdesk.model;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;

public class Kafka implements Serializable {

	private static final long serialVersionUID = -2009888749575790009L;

	public static final byte[] COL_FAMILY_DATA = Bytes.toBytes("1");

	public static final String COL_QUALIFIER_RAW = "raw";

	private String raw;

	private String topic;

	private JSONObject rawObj;

	private String docId;

	private String hbaseKey;

	public String getRaw() {
		return raw;
	}

	public void setRaw(String raw) {
		this.raw = raw;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public JSONObject getRawObj() {
		return rawObj;
	}

	public void setRawObj(JSONObject rawObj) {
		this.rawObj = rawObj;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getHbaseKey() {
		return hbaseKey;
	}

	public void setHbaseKey(String hbaseKey) {
		this.hbaseKey = hbaseKey;
	}

}
