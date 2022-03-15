/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.discovery.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
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
@Category({QuickTest.class, ParallelJVMTest.class})
public class SimplePredefinedDiscoveryServiceTest {

    @Test
    public void start() {
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
    public void discoverNodes() {
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
    public void discoverLocalMetadata() {
        final Map<String, String> metadata = new HashMap<>();
        metadata.put("a", "1");
        metadata.put("b", "2");
        final PredefinedDiscoveryService service = new PredefinedDiscoveryService(new ExtendableDiscoveryStrategy() {
            @Override
            public Map<String, String> discoverLocalMetadata() {
                return metadata;
            }
        });
        assertEquals(metadata, service.discoverLocalMetadata());
    }

    @Test
    public void destroy() {
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

    private abstract static class ExtendableDiscoveryStrategy extends AbstractDiscoveryStrategy {

        ExtendableDiscoveryStrategy() {
            super(null, Collections.<String, Comparable>emptyMap());
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return null;
        }
    }
}
