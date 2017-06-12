package com.hazelcast.spi.discovery.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SimplePredefinedDiscoveryServiceTest {

    @Test
    public void start() throws Exception {
        final CountDownLatch startCalled = new CountDownLatch(1);
        final PredefinedDiscoveryService service = new PredefinedDiscoveryService(new ExtendableDiscoveryStrategy() {
            @Override
            public void start() {
                startCalled.countDown();
            }
        });
        service.start();
        assertOpenEventually(startCalled);
    }

    @Test
    public void discoverNodes() throws Exception {
        final SimpleDiscoveryNode node = new SimpleDiscoveryNode(new Address());
        final Iterable<DiscoveryNode> nodes = Arrays.<DiscoveryNode>asList(node, node);
        final PredefinedDiscoveryService service = new PredefinedDiscoveryService(new ExtendableDiscoveryStrategy() {
            @Override
            public Iterable<DiscoveryNode> discoverNodes() {
                return nodes;
            }
        });
        assertEquals(nodes, service.discoverNodes());
    }

    @Test
    public void discoverLocalMetadata() throws Exception {
        final Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("a", 1);
        metadata.put("b", 2);
        final PredefinedDiscoveryService service = new PredefinedDiscoveryService(new ExtendableDiscoveryStrategy() {
            @Override
            public Map<String, Object> discoverLocalMetadata() {
                return metadata;
            }
        });
        assertEquals(metadata, service.discoverLocalMetadata());
    }

    @Test
    public void destroy() throws Exception {
        final CountDownLatch destroyCalled = new CountDownLatch(1);
        final PredefinedDiscoveryService service = new PredefinedDiscoveryService(new ExtendableDiscoveryStrategy() {
            @Override
            public void destroy() {
                destroyCalled.countDown();
            }
        });
        service.destroy();
        assertOpenEventually(destroyCalled);
    }

    private static abstract class ExtendableDiscoveryStrategy extends AbstractDiscoveryStrategy {

        public ExtendableDiscoveryStrategy() {
            super(null, Collections.<String, Comparable>emptyMap());
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }
}
