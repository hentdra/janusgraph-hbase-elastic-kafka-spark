package com.ebdesk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.lucene.util.fst.Util;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.*;
import org.janusgraph.diskstorage.indexing.RawQuery;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.transaction.StandardTransactionBuilder;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import scala.math.BigInt;
import scala.tools.scalap.scalax.rules.Result;

public class GetJanus {

	public static void main(String[] args) throws Exception {

		JanusGraph gr = JanusGraphFactory.open(env.properties);

//		JanusGraph gr = graph();
//		GraphTraversalSource g = gr.traversal();		// running di awal untuk pembuatan config di hbase
//		g.tx().commit();

		//pembuatan shema vertex
		JanusGraphManagement or = gr.openManagement();

		PropertyKey id_user = or.makePropertyKey("id_user").dataType(String.class).make();
		PropertyKey screen_name = or.makePropertyKey("screen_name").dataType(String.class).make();
		PropertyKey img = or.makePropertyKey("img").dataType(String.class).make();
		PropertyKey name = or.makePropertyKey("name").dataType(String.class).make();
		PropertyKey age = or.makePropertyKey("age").dataType(Integer.class).make();
		or.buildIndex("tes", Vertex.class)
		.addKey(name)
		.addKey(age)
				.addKey(id_user)
				.addKey(screen_name)
				.addKey(img)
				.buildMixedIndex("tes");
		or.commit();

		//pembuatan schema edge
//		JanusGraphManagement or = gr.openManagement();

		PropertyKey id_edge = or.makePropertyKey("id_edge").dataType(String.class).make();
		PropertyKey nama_edge = or.makePropertyKey("nama_edge").dataType(String.class).make();
		PropertyKey alamat_edge = or.makePropertyKey("alamat_edge").dataType(String.class).make();
		PropertyKey time = or.makePropertyKey("time").dataType(Integer.class).make();
		PropertyKey reason = or.makePropertyKey("reason").dataType(String.class).make();
		PropertyKey place = or.makePropertyKey("place").dataType(Geoshape.class).make();
		or.buildIndex("Edg", Edge.class)
		.addKey(time)
		.addKey(reason)
		.addKey(place)
				.addKey(id_edge)
				.addKey(nama_edge)
				.addKey(alamat_edge)
				.buildMixedIndex("tes");
		gr.tx().commit();

		for (int i =100 ; i < 101 ; i++ ) {
			Long a = Long.valueOf(12);
			JanusGraphTransaction tx = gr.newTransaction();

			long vertexId = ((StandardJanusGraph) gr).getIDManager().toVertexId(a);

			tx.addVertex(T.id , vertexId , T.label , "tw", "id_vetexs", "12", "nama_vetexs", "namaku 12", "alamat_vetexs", "alamat 12");
			tx.addVertex(T.label, "location", "name", "sky");
			tx.addVertex(T.label, "location", "name", "sea");
			tx.addVertex(T.label, "god", "name", "jupiter", "age", 5000);
			Vertex rt = tx.addVertex(T.id , a , "id_vetexs", "1", "nama_vetexs", "namaku satu", "alamat_vetexs", "alamat satu");
//			tw.addEdge("make_standart", rt);

			tx.getVertex(vertexId);

			try {
				tx.commit();
			}catch (Exception e){
				e.printStackTrace();
			}

		}


//		JanusGraphTransaction tx = gr.newTransaction();
//		long vertexId = ((StandardJanusGraph) gr).getIDManager().toVertexId(12);
//		Vertex a = tx.getVertex(vertexId);
//		System.out.println(a);


//		ManagementSystem.awaitGraphIndexStatus(gr, "tes").status(SchemaStatus.ENABLED).call();
//
//		GraphTraversalSource g = gr.traversal();
//		System.out.println(g.V().has("nama_vetexs", "namaku 12").count().next());



//		GraphTraversalSource g = gr.traversal();
//		System.out.println(g.V().has("nama_vetexs", Text.textContainsPrefix("namaku")).valueMap().next());
//		Iterable<JanusGraphIndexQuery.Result<JanusGraphVertex>> ta = tx.indexQuery("tes", "id_vetexs_t:'1'").vertices();
//
//		ta.forEach(new Consumer<JanusGraphIndexQuery.Result<JanusGraphVertex>>() {
//			@Override
//			public void accept(JanusGraphIndexQuery.Result<JanusGraphVertex> as) {
//				System.out.println(as.getElement().graph().traversal().V(as.getElement().id()).valueMap().next());
//			}
//		});
//
//		while (ta.iterator().hasNext()){
//			System.out.println("asd");
////			System.out.println(ta.iterator().next().getElement().graph().traversal().V().valueMap().next());
//		}

//		String a = "123456789123";
//		Number b = Integer.valueOf(a);
//		long c = Long.valueOf();


//		System.out.println(b);

//		for (JanusGraphIndexQuery.Result<JanusGraphVertex> result : tx.indexQuery("tes", "id_user_t:'2936724782'").vertices()) {
//			System.out.println(result.getElement());
//		}


		System.out.println("SELESAI");

	}

}