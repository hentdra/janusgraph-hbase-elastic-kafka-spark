//package com.ebdesk;
//
//import com.ebdesk.polri.model.PolriModel;
//import com.ebdesk.polri.util.PolriUtil;
//import com.fasterxml.jackson.databind.ObjectMapper;
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
//import org.janusgraph.core.JanusGraph;
//import org.janusgraph.core.JanusGraphFactory;
//import org.janusgraph.example.GraphOfTheGodsFactory;
//import org.janusgraph.graphdb.database.StandardJanusGraph;
//
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//public class ExampleLocal3 {
//
//    private static String path;
//
//    public static void main(String[] args) throws InterruptedException {
//
//        PolriConfig pConfig = PolriConfig.getInstance();
//
//        Collection<String> topics = Arrays.asList(
//                "enh-twitter-post"
//        );
//
//        SparkSession spark = SparkSession.builder()
//                .appName("janus-streaming")
//                .master("local[2]")
//                .getOrCreate();
//
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//
//        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
//        rootLogger.setLevel(Level.OFF);
//
//        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));
//
//        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
//                LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics,
//                        pConfig.setKafkaParam("twitter-janus").getKafkaParam()));
//
//        JavaDStream<PolriModel> data = stream.map(new Function<ConsumerRecord<String, String>, PolriModel>() {
//            @Override
//            public PolriModel call(ConsumerRecord<String, String> v1) throws Exception {
//                PolriModel p = new PolriModel();
//                try {
//                    p = PolriUtil.mappingPost(v1.topic(), v1.value());
//                } catch (Exception e) {
//                    System.out.println("\t" + e.getMessage());
//                }
//                return p;
//            }
//        }).filter(new Function<PolriModel, Boolean>() {
//            @Override
//            public Boolean call(PolriModel v1) throws Exception {
//                boolean result = false;
//                if (v1 != null) {
//                    result = true;
//                }
//                return result;
//            }
//        });
//
//        data.foreachRDD(new VoidFunction<JavaRDD<PolriModel>>() {
//
//            @Override
//            public void call(JavaRDD<PolriModel> rdd) throws Exception {
//            	JanusGraph gr = JanusGraphFactory.open("conf/local-hbase-elasticsearch.properties");
////                JanusGraph gr = conf().open();
//                load(gr, rdd);
//                gr.close();
//            }
//        });
//
//        data.print(0);
//        jssc.start();
//        jssc.awaitTermination();
//
//    }
//
//    public static void load(final JanusGraph graph, JavaRDD<PolriModel> model) {
//
//        String[] colNames = null;
//        boolean firstLine = true;
//        Map<Object, Object> us;
//        Map<Object, Object> rt;
//        Map<Object, Object> rp;
//        Map<Object, Object> qu;
//
//        boolean txSucceeded;
//
//        List<PolriModel> lines = model.collect();
//
//        for (int i = 0; i < lines.size(); i++) {
//            txSucceeded = false;
//            do {
//                try {
//                    PolriModel line = lines.get(i);
//                    us = new HashMap<Object, Object>();
//
//                    long us_id = Long.valueOf(line.getTweetData().getUser().getId());
//                    String us_screen_name = line.getTweetData().getUser().getDisplayName();
//                    String us_img = line.getTweetData().getUser().getImage();
//
//                    us.put(T.label, us_screen_name);
//                    us.put("id_user", us_id);
//                    us.put("screen_name", us_screen_name);
//                    us.put("img", us_img);
//
//                    List<Object> tweet_list = new ArrayList<Object>();
//                    us.forEach((key, val) -> {
//                        tweet_list.add(key);
//                        tweet_list.add(val);
//                    });
//
//                    qu = new HashMap<Object, Object>();
//                    rt = new HashMap<Object, Object>();
//                    rp = new HashMap<Object, Object>();
//
//                    System.out.println(line.getStatus());
//
//                    if (line.getStatus().equals("tweet")) {
//
//                        Vertex tw = createVertex(graph, tweet_list, "id_user", us_id);
//
//                    } else if (line.getStatus().equals("reply")) {
//
//                        us.put(T.label, line.getInReplyToScreenName());
//                        us.put("id_user", Long.valueOf(line.getInReplyToUserId()));
//                        us.put("screen_name", line.getInReplyToScreenName());
//                        us.put("img", line.getTweetData().getUser().getImage());
//
//                        List<Object> tweet_list_reply = new ArrayList<Object>();
//                        us.forEach((key, val) -> {
//                            tweet_list_reply.add(key);
//                            tweet_list_reply.add(val);
//                        });
//
//                        rp.put("id_post", Long.valueOf(line.getTweetData().getUser().getId()));
//                        rp.put("createdAtDate", line.getTweetData().getCreatedAtDate().getTime());
//                        rp.put("postMessage", line.getTweetData().getPostMessage());
//                        List<Object> reply_list_edge = new ArrayList<Object>();
//                        rp.forEach((key, val) -> {
//                            reply_list_edge.add(key);
//                            reply_list_edge.add(val);
//                        });
//
//                        Vertex tw = createVertex(graph, tweet_list, "id_user", us_id);
//                        System.out.println("================= > 1");
//                        Vertex reply = createVertex(graph, tweet_list_reply, "id_user", Long.valueOf(line.getInReplyToUserId()));
//                        System.out.println("================= > 2");
//
//                        try {
//                            reply.addEdge("reply", tw, reply_list_edge.toArray());
//                            System.out.println("CREATE EDGE REPLY");
//                        } catch (Exception e) {
//                            System.out.println("ERROR EDGE REPLY ==> " + e.getMessage().toString());
//                        }
//
//                    } else if (line.getStatus().equals("retweet")) {
//
//                        rt.put(T.label, Long.valueOf(line.getTweetData().getUser().getId()));
//                        rt.put("id_user", Long.valueOf(line.getTweetData().getUser().getId()));
//                        rt.put("screen_name", line.getTweetData().getUser().getDisplayName());
//                        rt.put("img", line.getTweetData().getUser().getImage());
//
//                        List<Object> retweet_list_vertex = new ArrayList<Object>();
//                        rt.forEach((key, val) -> {
//                            retweet_list_vertex.add(key);
//                            retweet_list_vertex.add(val);
//                        });
//
//                        rp.put("id_user", Long.valueOf(line.getTweetData().getUser().getId()));
//                        rp.put("id_status", Long.valueOf(line.getTweetData().getUser().getId()));
//                        List<Object> retweet_list_edge = new ArrayList<Object>();
//                        rp.forEach((key, val) -> {
//                            retweet_list_edge.add(key);
//                            retweet_list_edge.add(val);
//                        });
//
//                        Vertex tw = createVertex(graph, tweet_list, "id_user", us_id);
//                        Vertex retweet = createVertex(graph, retweet_list_vertex, "id_user", Long.valueOf(line.getTweetData().getUser().getId()));
//
//                        try {
//                            tw.addEdge("retweet", retweet, retweet_list_edge.toArray());
//                            System.out.println("CREATE EDGE RETWEET");
//                        } catch (Exception e) {
//                            System.out.println("ERROR EDGE RETWEET ==> " + e.getMessage().toString());
//                        }
//
//                    } else if (line.getStatus().equals("quote")) {
//
//                        qu.put(T.label, line.getRetweetData().getUser().getDisplayName());
//                        qu.put("id_user", Long.valueOf(line.getTweetData().getUser().getId()));
//                        qu.put("screen_name", line.getRetweetData().getUser().getDisplayName());
//                        qu.put("img", line.getRetweetData().getUser().getImage());
//
//                        List<Object> quote_list_vertex = new ArrayList<Object>();
//                        qu.forEach((key, val) -> {
//                            quote_list_vertex.add(key);
//                            quote_list_vertex.add(val);
//                        });
//
//                        rp.put("id_quote", Long.valueOf(line.getRetweetData().getId()));
//                        rp.put("createdAtDate", line.getRetweetData().getCreatedAtDate().getTime());
//                        rp.put("postMessage", line.getRetweetData().getPostMessage());
//                        List<Object> quote_list_edge = new ArrayList<Object>();
//                        rp.forEach((key, val) -> {
//                            quote_list_edge.add(key);
//                            quote_list_edge.add(val);
//                        });
//
//                        Vertex tw = createVertex(graph, tweet_list, "id_user", us_id);
//                        Vertex quote = createVertex(graph, quote_list_vertex, "id_user", Long.valueOf(line.getRetweetData().getUser().getId()));
//
//                        try {
//                            quote.addEdge("quote", tw, quote_list_edge.toArray());
//                            System.out.println("CREATE EDGE QUOTE");
//                        } catch (Exception e) {
//                            System.out.println("ERROR EDGE QUOTE ==> " + e.getMessage().toString());
//                        }
//
//
//                    }
//
//                } catch (Exception e) {
//                    System.out.println("ERROR ==> ");
//                }
//
//            } while (!txSucceeded);
//
//            try {
//                graph.tx().commit();
//                txSucceeded = true;
//            } catch (Exception e) {
//                System.out.println("ERROR ==> " + e.getMessage().toString());
//            }
//
//        }
//    }
//
//    public static Vertex createVertex(final JanusGraph graph, List<Object> objects, String in, long id){
//        Vertex v = null;
//        try {
//            v = graph.addVertex(new Object[] {});
//            System.out.println("CREATE VERTEX");
//        } catch (Exception e) {
//            try {
//                v = graph.traversal().V().has(in, id).next();
//            }catch (Exception e2){
//
//            }
//            System.out.println("ERROR VERTEX ==> " + e.getMessage().toString());
//        }
//        return v;
//    }
//
//}
