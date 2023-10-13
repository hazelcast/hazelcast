/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.wan;

import com.hazelcast.config.AbstractWanPublisherConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanEvent;
import com.hazelcast.wan.impl.DelegatingWanScheme;
import com.hazelcast.wan.impl.WanDummyPublisher;
import com.hazelcast.wan.impl.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class WANContextInitializationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void reset() {
        SlowWanPublisher.invocations.getAndSet(0);
        factory.terminateAll();
    }

    @Override
    protected Config getConfig() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("slowWan")
                .addCustomPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("slowWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setWanReplicationRef(wanRef);

        return smallInstanceConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }

    protected WanCustomPublisherConfig getPublisherConfig() {
        return new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(SlowWanPublisher.class.getName());
    }

    @Test
    public void testInitializationOnNewNodeJoin() {
        HazelcastInstance node1 = factory.newHazelcastInstance(getConfig());

        IMap<Object, Object> map = node1.getMap("default");
        map.set(1, 1);
        assertEquals("init method of WAN should be called only once", 1, SlowWanPublisher.invocations.get());

        factory.newHazelcastInstance(getConfig());
        // wait migrations end
        waitAllForSafeState(factory.getAllHazelcastInstances());

        assertEquals("init method of WAN should be called only once", 1, SlowWanPublisher.invocations.get());
        //put new object
        map.set(2, 2);
        assertEquals("init method of WAN should be called twice", 2, SlowWanPublisher.invocations.get());
    }

    @Test
    public void wanReplicationDelegateSupplierLazilyInitialized() {
        initInstances();
        instance1.getMap("default");
        assertEquals(0, SlowWanPublisher.invocations.get());

        for (int i = 0; i < 10; i++) {
            instance1.getMap("default").put(i, i);
        }

        assertEquals(2, SlowWanPublisher.invocations.get());
        //check the queue size
        assertTotalQueueSize(10);
    }


    private void initInstances() {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());

        assertClusterSizeEventually(2, instance1, instance2);
    }

    private WanDummyPublisher getWanReplicationImpl(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers("slowWan");
        return (SlowWanPublisher) delegate.getPublishers().iterator().next();
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(instance1).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(instance2).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));
    }

    static class SlowWanPublisher extends WanDummyPublisher {
        static AtomicInteger invocations = new AtomicInteger();

        @Override
        public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig wanPublisherConfig) {
            sleepSeconds(2);
            invocations.incrementAndGet();
            super.init(wanReplicationConfig, wanPublisherConfig);
        }
    }
}
