package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.CompositeProbe;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ContainsProbes;
import com.hazelcast.internal.metrics.ProbeName;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ContainsProbesTest {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void test() {
        Node node = new Node();
        Leaf leaf = new Leaf();
        leaf.field = 20;
        node.leaf = leaf;

      //  metricsRegistry.registerRoot(node, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.leaf.field");
        assertEquals(20, gauge.read());
    }

    @Test
    public void testProbeName() {
        Node root = new Node();

        NodeWithName nodeWithName = new NodeWithName();
        root.leaf = nodeWithName;

        Leaf leaf = new Leaf();
        leaf.field = 20;
        nodeWithName.leaf = leaf;

    //    metricsRegistry.registerRoot(root, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.probeName.leaf.field");
        assertEquals(20, gauge.read());
    }

    @Test
    public void testNullNode() {
        Node node = new Node();

    //    metricsRegistry.registerRoot(node, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.field");
        assertEquals(0, gauge.read());
    }

    @Test
    public void nonExistingRoot() {
        LongGauge gauge = metricsRegistry.newLongGauge("notexist.foo.bar");
        assertEquals(0, gauge.read());
    }

    // =================== map ======================

    @Test
    public void map() {
        MapNode node = new MapNode();
        node.map = new HashMap();
        Leaf leaf = new Leaf();
        leaf.field = 20;
        node.map.put("foo", leaf);

   //     metricsRegistry.registerRoot(node, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.leaf.field");
        assertEquals(20, gauge.read());
    }

    @Test
    public void mapWhenNull() {
        MapNode node = new MapNode();
        node.map = null;

    //    metricsRegistry.registerRoot(node, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.field");
        assertEquals(0, gauge.read());
    }

    @Test
    public void mapWhenMapEmptyNull() {
        MapNode node = new MapNode();
        node.map = new HashMap();

    //    metricsRegistry.registerRoot(node, "root");

        LongGauge gauge = metricsRegistry.newLongGauge("root.field");
        assertEquals(0, gauge.read());
    }

    public class Node {

        @ContainsProbes
        Object leaf;
    }

    public class NodeWithName {

        @ContainsProbes
        Leaf leaf;

        @ProbeName
        public String probeName(){
            return "probeName";
        }
    }

    @CompositeProbe(name = "leaf")
    public class Leaf {

        @Probe
        long field;
    }

    public class MapNode {

        @ContainsProbes
        Map map;
    }
}
