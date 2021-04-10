package com.ebdesk;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.ebdesk.model.Kafka;

public class tes {
	public static void main(String[] args) throws ParseException {
		String data = "{\"data\": {\"id\":\"203918\",\"label\":\"location\",\"name\":\"sky\"}, \"type\":\"vertex\" }";
		Kafka kafka = new Kafka();
		kafka.setRaw(data);
		kafka.setTopic("test");
		kafka.setRawObj((JSONObject) new JSONParser().parse(data));
		
		JSONObject json = (JSONObject) kafka.getRawObj().get("data");
		
		System.out.println(json);
	}
}
