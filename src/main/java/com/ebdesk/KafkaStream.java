package com.ebdesk;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.ebdesk.model.Kafka;

public class KafkaStream {
	private static final Logger LOGGER = Logger.getLogger(KafkaStream.class);

	public static void main(String[] args) throws IOException, InterruptedException {

//		String kafkaTopic = "sc-ig-follower";
		String kafkaTopic = env.topic;
		String broker = env.broker;
		String group = env.group;
		String offset = env.offset;

		// String kafkaTopic = args[0];
		// String broker = args[1];
		// String group = args[2];
		// String offset = args[3];

		SparkConf sparkConf = new SparkConf();
		// .set("spark.streaming.kafka.consumer.poll.ms", consumerPoll)
		// .set("spark.streaming.backpressure.enabled", backpressureEnabled)
		// .set("spark.streaming.kafka.maxRatePerPartition", ratePerPartition)
		// .set("es.nodes", elasticHost)
		// .set("es.port", elasticPort)
		// .set("spark.mongodb.input.uri", mongoHostInput)
		// .set("spark.mongodb.output.uri", mongoHostOutput);

		SparkSession sparkSession = SparkSession.builder().master("local[4]").appName("KafkaStream")
				.config(sparkConf).getOrCreate();

		JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.seconds(2));

		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.OFF);

		String[] tmpTopics = kafkaTopic.split(",");
		Collection<String> topics = Arrays.asList(tmpTopics);
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", group);
		kafkaParams.put("auto.offset.reset", offset);
		// kafkaParams.put("enable.auto.commit", autoCommit);
		// kafkaParams.put("request.timeout.ms", requestTimeout);
		// kafkaParams.put("max.poll.records", pollRecords);

		// streaming dari awal
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jsc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<Kafka> lines = messages.map(new Function<ConsumerRecord<String, String>, Kafka>() {

			@Override
			public Kafka call(ConsumerRecord<String, String> records) throws Exception {
				// TODO Auto-generated method stub
				Kafka kafka = new Kafka();

				// set raw
				kafka.setRaw(records.value());

				// set kafka topic
				kafka.setTopic(records.topic());

				// set raw object
				kafka.setRawObj((JSONObject) new JSONParser().parse(records.value()));

				return kafka;
			}
		});

		lines.foreachRDD(new VoidFunction<JavaRDD<Kafka>>() {

			@Override
			public void call(JavaRDD<Kafka> rdd) throws Exception {
				// TODO Auto-generated method stub

				rdd.foreachPartition(new VoidFunction<Iterator<Kafka>>() {

					@Override
					public void call(Iterator<Kafka> rdd) throws Exception {
						// TODO Auto-generated method stub

						int i = 0;
						Map<Object, Object> source;
						Map<Object, Object> target;
						Map<Object, Object> edges;

						if (rdd.hasNext()) {

							JanusGraph graph = JanusGraphFactory.open(env.properties);

							TransactionBuilder builder = graph.buildTransaction();
							JanusGraphTransaction tx = builder.enableBatchLoading().consistencyChecks(false).start();
							tx.tx().commit();

							createSchema(graph, env.index_name, true);

							System.out.println(new Date().toString());
							while (rdd.hasNext()) {

								Kafka kafka = rdd.next();
								source = new HashMap<Object, Object>();
								target = new HashMap<Object, Object>();
								edges = new HashMap<Object, Object>();

								if (i % 1000 == 0) System.out.print(i + ".. ");
//								System.out.println(i + ": " + kafka.getRaw());

								// System.out.print("CREATING SOURCE VERTEX ==> ");
								JSONObject json = kafka.getRawObj();
								try {
									source.put("username", json.get("username"));
									source.put("pk", (long) json.get("pk"));
									source.put("platform", "instagram");
									source.put("name", json.get("full_name"));
									source.put("profile_picture", json.get("profile_pic_url"));
									source.put("nodes_type", "ig_follow");
									source.put("created_at", "");
									source.put("description", "");
								} catch (Exception e) {
									System.out.println("ERROR PARSING JSON ==> " + e.getMessage().toString());
								}

								List<Object> source_list = new ArrayList<Object>();
								source.forEach((key, val) -> {
									source_list.add(key);
									source_list.add(val);
								});
								Vertex source_vertex = createVertex(graph, source_list,
										json.get("username").toString());

								// System.out.print("CREATING TARGET VERTEX ==> ");
								JSONObject json2 = (JSONObject) json.get("from");
								try {
									target.put("username", json2.get("username"));
									target.put("pk", (long) json2.get("pk"));
									target.put("platform", "instagram");
									target.put("name", json2.get("full_name"));
									target.put("profile_picture", json2.get("profile_pic_url"));
									target.put("nodes_type", "ig_follow");
									target.put("created_at", "");
									target.put("description", "");
								} catch (Exception e) {
									System.out.println("ERROR PARSING JSON ==> " + e.getMessage().toString());
								}

								List<Object> target_list = new ArrayList<Object>();
								target.forEach((key, val) -> {
									target_list.add(key);
									target_list.add(val);
								});
								Vertex target_vertex = createVertex(graph, target_list,
										json2.get("username").toString());

								// System.out.print("CREATING EDGE ==> ");
								edges.put("source", json.get("pk"));
								edges.put("target", json2.get("pk"));
								edges.put("kind", "ig_follow");
								edges.put("start_time", "");
								edges.put("end_time", "");
								List<Object> edges_list = new ArrayList<Object>();
								edges.forEach((key, val) -> {
									edges_list.add(key);
									edges_list.add(val);
								});
								try {
									// if (source_vertex != null && target_vertex != null) {
									source_vertex.addEdge("follow", target_vertex, edges_list.toArray());
									// System.out.println("EDGE CREATED!");
									// }
								} catch (Exception e) {
									System.out.println("ERROR CREATING EDGE ==> " + e.getMessage().toString());
								}

								i++;
								if (i % 10000 == 0) {
									try {
										System.out.print("COMMITING.... ");
										graph.tx().commit();
										System.out.println("SUCCESS COMMIT!");
										System.out.println(new Date().toString());
									} catch (Exception e) {
										System.out.println("ERROR COMMIT ==> " + e.getMessage().toString());
									}
									i = 0;
								}
							}

							graph.tx().commit();
							graph.tx().close();
							graph.close();
						}

					}
				});

			}
		});

		// lines.print();
		jsc.start();
		jsc.awaitTermination();
	}

	public static Vertex createVertex(final JanusGraph graph, List<Object> objects, Object id) {

		Vertex v = null;
		try {
			if (graph.traversal().V().has("username", id).hasNext()) {
				// System.out.println("VERTEX HAS BEEN CREATED");
				v = graph.traversal().V().has("username", id).next();
			} else {
				// System.out.print("CREATING VERTEX ==> ");
				v = graph.addVertex(objects.toArray());
				// System.out.println("VERTEX CREATED");
			}
		} catch (Exception e) {
			System.out.println("ERROR CREATING VERTEX ==> " + e.getMessage().toString());

		}
		return v;
	}

	public static void createSchema(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {

		JanusGraphManagement mgmt = graph.openManagement();
		PropertyKey username = mgmt.makePropertyKey("username").dataType(String.class).make();

		JanusGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("username", Vertex.class).addKey(username);
		if (uniqueNameCompositeIndex) {
			nameIndexBuilder.unique();
		}
		JanusGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
		mgmt.setConsistency(namei, ConsistencyModifier.DEFAULT);

		PropertyKey pk = mgmt.makePropertyKey("pk").dataType(String.class).make();
		PropertyKey platform = mgmt.makePropertyKey("platform").dataType(String.class).make();
		PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
		PropertyKey profile_picture = mgmt.makePropertyKey("profile_picture").dataType(String.class).make();
		PropertyKey nodes_type = mgmt.makePropertyKey("nodes_type").dataType(String.class).make();
		PropertyKey created_at = mgmt.makePropertyKey("created_at").dataType(String.class).make();
		PropertyKey description = mgmt.makePropertyKey("description").dataType(String.class).make();
		if (null != mixedIndexName)
			mgmt.buildIndex("vertex", Vertex.class).addKey(pk).addKey(username).addKey(platform).addKey(name)
					.addKey(profile_picture).addKey(nodes_type).addKey(created_at).addKey(description)
					.buildMixedIndex(mixedIndexName);

		PropertyKey source = mgmt.makePropertyKey("source").dataType(String.class).make();
		PropertyKey target = mgmt.makePropertyKey("target").dataType(String.class).make();
		PropertyKey kind = mgmt.makePropertyKey("kind").dataType(String.class).make();
		PropertyKey start_time = mgmt.makePropertyKey("start_time").dataType(String.class).make();
		PropertyKey end_time = mgmt.makePropertyKey("end_time").dataType(String.class).make();
		if (null != mixedIndexName)
			mgmt.buildIndex("edge", Edge.class).addKey(source).addKey(target).addKey(kind).addKey(start_time)
					.addKey(end_time).buildMixedIndex(mixedIndexName);

		mgmt.makeEdgeLabel("follow").make();
		mgmt.commit();

	}
}
