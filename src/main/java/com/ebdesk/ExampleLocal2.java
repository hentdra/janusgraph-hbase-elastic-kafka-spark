package com.ebdesk;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

public class ExampleLocal2 {

    public static void main(String[] args) throws Exception {
        createCollections();
    }

    public static void createCollections(){
        JanusGraph gr = JanusGraphFactory.open("conf/local-hbase-elasticsearch.properties");
        TransactionBuilder builder = gr.buildTransaction();
        JanusGraphTransaction tx = builder.enableBatchLoading().consistencyChecks(false).start();
        tx.tx().commit();

        createSchema(gr, "schema", true);

        System.out.println("SELESAI");
        System.exit(0);
    }

    public static void createSchema(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex){

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey id_user = mgmt.makePropertyKey("id_user").dataType(Long.class).make();
        JanusGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("id_user", Vertex.class).addKey(id_user);
        if (uniqueNameCompositeIndex) {
            nameIndexBuilder.unique();
        }

        JanusGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        PropertyKey screen_name = mgmt.makePropertyKey("screen_name").dataType(String.class).make();
        PropertyKey img = mgmt.makePropertyKey("img").dataType(String.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("tw_vertex", Vertex.class)
                    .addKey(id_user)
                    .addKey(screen_name)
                    .addKey(img)
                    .buildMixedIndex(mixedIndexName);

        PropertyKey id_post = mgmt.makePropertyKey("id_post").dataType(Long.class).make();
        PropertyKey id_quote = mgmt.makePropertyKey("id_quote").dataType(Long.class).make();
        PropertyKey createdAtDate = mgmt.makePropertyKey("createdAtDate").dataType(String.class).make();
        PropertyKey postMessage = mgmt.makePropertyKey("postMessage").dataType(String.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("tw_edge", Edge.class)
                    .addKey(id_post)
                    .addKey(id_quote)
                    .addKey(createdAtDate)
                    .addKey(postMessage).buildMixedIndex(mixedIndexName);
        
        EdgeLabel reply = mgmt.makeEdgeLabel("reply").signature(new PropertyKey[]{createdAtDate}).multiplicity(Multiplicity.MULTI).make();
        mgmt.buildEdgeIndex(reply, "reply", Direction.BOTH, Order.decr, new PropertyKey[]{createdAtDate});
        
        mgmt.makeEdgeLabel("retweet").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("quote").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("tweet").multiplicity(Multiplicity.MULTI).make();
        
        
        mgmt.commit();

    }
}