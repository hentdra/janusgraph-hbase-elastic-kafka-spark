package com.ebdesk;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.example.GraphOfTheGodsFactory;

public class ExampleLocal1 {
	
	public static void main(String[] args) throws Exception {
        createCollections();
    }

    public static void createCollections(){
        JanusGraph gr = JanusGraphFactory.open("conf/hbase2.properties");
        GraphTraversalSource g = gr.traversal();
        g.tx().commit();
        
        createSchema(gr, "schema", true);

        System.out.println("SELESAI");
        System.exit(0);
    }

    public static void createSchema(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex){

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        JanusGraphManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        if (uniqueNameCompositeIndex) {
            nameIndexBuilder.unique();
        }

        JanusGraphIndex namei = nameIndexBuilder.buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        PropertyKey age = mgmt.makePropertyKey("age").dataType(Integer.class).make();
        if (null != mixedIndexName) {
            mgmt.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex(mixedIndexName);
        }

        PropertyKey time = mgmt.makePropertyKey("time").dataType(Integer.class).make();
        PropertyKey reason = mgmt.makePropertyKey("reason").dataType(String.class).make();
        PropertyKey place = mgmt.makePropertyKey("place").dataType(Geoshape.class).make();
        if (null != mixedIndexName) {
            mgmt.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);
        }

        mgmt.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
        mgmt.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel battled = mgmt.makeEdgeLabel("battled").signature(new PropertyKey[]{time}).make();
        mgmt.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.decr, new PropertyKey[]{time});
        mgmt.makeEdgeLabel("lives").signature(new PropertyKey[]{reason}).make();
        mgmt.makeEdgeLabel("pet").make();
        mgmt.makeEdgeLabel("brother").make();
        mgmt.makeVertexLabel("titan").make();
        mgmt.makeVertexLabel("location").make();
        mgmt.makeVertexLabel("god").make();
        mgmt.makeVertexLabel("demigod").make();
        mgmt.makeVertexLabel("human").make();
        mgmt.makeVertexLabel("monster").make();
        mgmt.commit();

        JanusGraphTransaction tx = graph.newTransaction();

        Vertex saturn = tx.addVertex(new Object[]{T.label, "titan", "name", "saturn", "age", Integer.valueOf(10000)});
        Vertex sky = tx.addVertex(new Object[]{T.label, "location", "name", "sky"});
        Vertex sea = tx.addVertex(new Object[]{T.label, "location", "name", "sea"});
        Vertex jupiter = tx.addVertex(new Object[]{T.label, "god", "name", "jupiter", "age", Integer.valueOf(5000)});
        Vertex neptune = tx.addVertex(new Object[]{T.label, "god", "name", "neptune", "age", Integer.valueOf(4500)});
        Vertex hercules = tx.addVertex(new Object[]{T.label, "demigod", "name", "hercules", "age", Integer.valueOf(30)});
        Vertex alcmene = tx.addVertex(new Object[]{T.label, "human", "name", "alcmene", "age", Integer.valueOf(45)});
        Vertex pluto = tx.addVertex(new Object[]{T.label, "god", "name", "pluto", "age", Integer.valueOf(4000)});
        Vertex nemean = tx.addVertex(new Object[]{T.label, "monster", "name", "nemean"});
        Vertex hydra = tx.addVertex(new Object[]{T.label, "monster", "name", "hydra"});
        Vertex cerberus = tx.addVertex(new Object[]{T.label, "monster", "name", "cerberus"});
        Vertex tartarus = tx.addVertex(new Object[]{T.label, "location", "name", "tartarus"});
        jupiter.addEdge("father", saturn, new Object[0]);
        jupiter.addEdge("lives", sky, new Object[]{"reason", "loves fresh breezes"});
        jupiter.addEdge("brother", neptune, new Object[0]);
        jupiter.addEdge("brother", pluto, new Object[0]);
        neptune.addEdge("lives", sea, new Object[0]).property("reason", "loves waves");
        neptune.addEdge("brother", jupiter, new Object[0]);
        neptune.addEdge("brother", pluto, new Object[0]);
        hercules.addEdge("father", jupiter, new Object[0]);
        hercules.addEdge("mother", alcmene, new Object[0]);
        hercules.addEdge("battled", nemean, new Object[]{"time", Integer.valueOf(1), "place", Geoshape.point(38.099998474121094D, 23.700000762939453D)});
        hercules.addEdge("battled", hydra, new Object[]{"time", Integer.valueOf(2), "place", Geoshape.point(37.70000076293945D, 23.899999618530273D)});
        hercules.addEdge("battled", cerberus, new Object[]{"time", Integer.valueOf(12), "place", Geoshape.point(39.0D, 22.0D)});
        pluto.addEdge("brother", jupiter, new Object[0]);
        pluto.addEdge("brother", neptune, new Object[0]);
        pluto.addEdge("lives", tartarus, new Object[]{"reason", "no fear of death"});
        pluto.addEdge("pet", cerberus, new Object[0]);
        cerberus.addEdge("lives", tartarus, new Object[0]);
        tx.commit();
    }

}
