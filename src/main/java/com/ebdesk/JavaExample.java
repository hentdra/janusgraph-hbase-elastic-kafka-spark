//package com.ebdesk;
//
////import com.ebdesk.polri.model.PolriModel;
////import com.ebdesk.polri.util.PolriUtil;
//import org.apache.commons.configuration.BaseConfiguration;
//import org.apache.hadoop.classification.InterfaceAudience.Public;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.log4j.Level;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.apache.tinkerpop.gremlin.structure.T;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.janusgraph.core.*;
//import org.janusgraph.graphdb.database.StandardJanusGraph;
//import org.json.JSONObject;
//import org.mortbay.util.ajax.JSON;
//
//import scala.Serializable;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.List;
//
//public class JavaExample implements Serializable {
//
//	private static String path;
//
//	public static void main(String[] args) throws InterruptedException {
//
//		// path = "conf/solr.properties";
//		// path = args[0];
//
//		PolriConfig pConfig = PolriConfig.getInstance();
//
//		Collection<String> topics = Arrays.asList("enh-twitter-post");
//
//		SparkSession spark = SparkSession.builder().appName("janus-streaming").master("local[4]").getOrCreate();
//
//		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//		org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
//		rootLogger.setLevel(Level.OFF);
//
//		JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
//
//		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
//				LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics,
//						pConfig.setKafkaParam("twitter-janus").getKafkaParam()));
//
//		JavaDStream<Tuple2<String, String>> data = stream
//				.map(new Function<ConsumerRecord<String, String>, Tuple2<String, String>>() {
//
//					@Override
//					public Tuple2<String, String> call(ConsumerRecord<String, String> dt) throws Exception {
//						// TODO Auto-generated method stub
//
//						return new Tuple2<String, String>(dt.topic(), dt.value());
//					}
//				}).repartition(4);
//
//		data.foreachRDD(new VoidFunction<JavaRDD<Tuple2<String, String>>>() {
//
//			@Override
//			public void call(JavaRDD<Tuple2<String, String>> rdd) throws Exception {
//				// TODO Auto-generated method stub
//				tester(rdd);
//			}
//		});
//
//		// JanusGraphTransaction tx = tx = graph().newTransaction();
//		// asd.getJSONObject("tw_data").getJSONObject("user").getLong("");
//		//
//		//
//		// long id_user = Long.valueOf(polriModel.getTweetData().getUser().getId());
//		// String sn = polriModel.getTweetData().getUser().getDisplayName();
//		// String img = polriModel.getTweetData().getUser().getImage();
//		//
//		// String idU = String.valueOf(id_user);
//		// long idNew = (idU.length() > 17) ? Long.valueOf(idU.substring(0, 15)) :
//		// id_user;
//		// long id_tw = ((StandardJanusGraph) graph()).getIDManager().toVertexId(idNew);
//		// try {
//		//
//		// System.out.println("Janusgraph is => "+graph().isOpen());
//		// if (polriModel.getStatus().equals("tweet")) {
//		// try {
//		// tx.addVertex(T.id, id_tw, T.label, sn, "id_user", id_user, "screen_name", sn,
//		// "img", img);
//		// tx.commit();
//		// System.out.println("create vertex");
//		// tx.close();
//		// } catch (Exception e) {
//		// System.out.println("ERROR USER ========> " + e.getMessage().toString());
//		// } finally {
//		// tx.close();
//		// }
//		// } else if (polriModel.getStatus().equals("retweet")) {
//		// long id_user_rt =
//		// Long.valueOf(polriModel.getRetweetData().getUser().getId());
//		// String idU2 = String.valueOf(id_user_rt);
//		// long idNew2 = (idU2.length() > 17) ? Long.valueOf(idU2.substring(0, 15)) :
//		// id_user_rt;
//		//
//		// long id_rt = ((StandardJanusGraph)
//		// graph()).getIDManager().toVertexId(idNew2);
//		//
//		// String sn_rt = polriModel.getRetweetData().getUser().getDisplayName();
//		// String img_rt = polriModel.getRetweetData().getUser().getImage();
//		//
//		// Vertex tw = null;
//		// try {
//		// tw = tx.addVertex(T.id, id_tw, T.label, sn, "id_user", id_user,
//		// "screen_name", sn, "img", img);
//		// } catch (Exception e) {
//		// for (JanusGraphIndexQuery.Result<JanusGraphVertex> result :
//		// tx.indexQuery("tester", "id_user_t:'" + id_user + "'").vertices()) {
//		// tw = result.getElement();
//		// }
//		// }
//		//
//		// Vertex rt = null;
//		// try {
//		// rt = tx.addVertex(T.id, id_rt, T.label, sn_rt, "id_user", id_user_rt,
//		// "screen_name", sn_rt, "img", img_rt);
//		// } catch (Exception e) {
//		// for (JanusGraphIndexQuery.Result<JanusGraphVertex> result :
//		// tx.indexQuery("tester", "id_user_t:'" + id_user_rt + "'").vertices()) {
//		// rt = result.getElement();
//		// }
//		// }
//		//
//		// if (tw != null && rt != null) {
//		// try {
//		// tw.addEdge("retweet", rt, "id_post", polriModel.getTweetData().getId(),
//		// "timestamp", polriModel.getTweetData().getCreatedAtDate().getTime());
//		// tx.commit();
//		// System.out.println("create edge");
//		// tx.close();
//		// }catch (Exception e){
//		// System.out.println("ERROR RETWEET ========> " + e.getMessage().toString());
//		// } finally {
//		// tx.close();
//		// }
//		// }
//		//
//		// if(tx.isOpen()){
//		// tx.close();
//		//
//		// }
//		// }
//		// } catch (Exception e) {
//		// System.out.println("ERROR CONNECTION ========> " +
//		// e.getMessage().toString());
//		// } finally {
//		// if(graph().isOpen()){
//		// graph().close();
//		// }
//		// }
//		//
//		// data.print(0);
//		jssc.start();
//		jssc.awaitTermination();
//
//	}
//
//	public static void tester(JavaRDD<Tuple2<String, String>> rdd) {
//
//
//		List<Tuple2<String, String>> asd = rdd.collect();
//
//		for (int i = 0; i < asd.size(); i++) {
//			Tuple2<String, String> das= asd.get(i);
//			JanusGraph graph = JanusGraphFactory.open("conf/hbase2.properties");
//			try {
//				JSONObject adsa = new JSONObject(das._2());
//
//				long id = adsa.getJSONObject("raw").getJSONObject("user").getLong("id");
//				String nama = adsa.getJSONObject("raw").getJSONObject("user").getString("name");
//				String image_url = adsa.getJSONObject("raw").getJSONObject("user").getString("profile_image_url_https");
//
//				Vertex tw = graph.addVertex(T.label, nama, "id_user", id, "nama", nama, "img", image_url);
//				System.out.println("INSERT JANUS");
//			} catch (Exception e) {
//
//			}
//			try {
//				graph.tx().commit();
//			} catch (Exception e) {
//				// TODO: handle exception
//				e.printStackTrace();
////				System.out.println(e.getMessage().toString());
//			}
//		}
//
//	}
//
//}
