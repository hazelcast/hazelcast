/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.networking.nio.MigratablePipeline;
import com.hazelcast.internal.networking.nio.NioChannel;
import com.hazelcast.internal.networking.nio.NioEventLoopGroup;
import com.hazelcast.internal.networking.nio.NioInboundPipeline;
import com.hazelcast.internal.networking.nio.NioOutboundPipeline;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class IOBalancerStressTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testEachConnectionUseDifferentSelectorEventually() {
        Config config = new Config()
                .setProperty(GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1")
                .setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "2");

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(config);

        instance2.shutdown();
        instance2 = Hazelcast.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        assertBalanced(instance1);
        assertBalanced(instance2);
        assertBalanced(instance3);
    }

    private void assertBalanced(HazelcastInstance hz) {
        TcpIpConnectionManager connectionManager = (TcpIpConnectionManager) getConnectionManager(hz);

        Map<NioThread, Set<MigratablePipeline>> handlersPerSelector = getHandlersPerSelector(connectionManager);

        try {
            for (Map.Entry<NioThread, Set<MigratablePipeline>> entry : handlersPerSelector.entrySet()) {
                NioThread selector = entry.getKey();
                Set<MigratablePipeline> handlers = entry.getValue();
                assertBalanced(selector, handlers);
            }
        } catch (AssertionError e) {
            // if something fails, we want to see the debug output
            System.out.println(debug(connectionManager));
            throw e;
        }
    }

    private Map<NioThread, Set<MigratablePipeline>> getHandlersPerSelector(TcpIpConnectionManager connectionManager) {
        Map<NioThread, Set<MigratablePipeline>> handlersPerSelector = new HashMap<NioThread, Set<MigratablePipeline>>();
        for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
            NioChannel channel = (NioChannel) connection.getChannel();
            add(handlersPerSelector, channel.getInboundPipeline());
            add(handlersPerSelector, channel.getOutboundPipeline());
        }
        return handlersPerSelector;
    }

    private void add(Map<NioThread, Set<MigratablePipeline>> handlersPerSelector, MigratablePipeline handler) {
        Set<MigratablePipeline> handlers = handlersPerSelector.get(handler.getOwner());
        if (handlers == null) {
            handlers = new HashSet<MigratablePipeline>();
            handlersPerSelector.put(handler.getOwner(), handlers);
        }
        handlers.add(handler);
    }

    /**
     * A selector is balanced if:
     * <ul>
     * <li>it has 1 active handler (so a high event count)</li>
     * <li>potentially 1 dead handler (duplicate connection), so event count should be low</li>
     * </ul>
     */
    private void assertBalanced(NioThread selector, Set<MigratablePipeline> handlers) {
        assertTrue("no handlers were found for selector:" + selector, handlers.size() > 0);
        assertTrue("too many handlers were found for selector:" + selector, handlers.size() <= 2);

        Iterator<MigratablePipeline> iterator = handlers.iterator();
        MigratablePipeline activeHandler = iterator.next();
        if (handlers.size() == 2) {
            MigratablePipeline deadHandler = iterator.next();
            if (activeHandler.getLoad() < deadHandler.getLoad()) {
                MigratablePipeline tmp = deadHandler;
                deadHandler = activeHandler;
                activeHandler = tmp;
            }

            // the maximum number of events seen on the dead connection is 3. 10 should be save to assume the
            // connection is dead.
            assertTrue("at most 10 event should have been received, number of events received:"
                    + deadHandler.getLoad(), deadHandler.getLoad() < 10);
        }

        assertTrue("activeHandlerEvent count should be at least 1000, but was:" + activeHandler.getLoad(),
                activeHandler.getLoad() > 1000);
    }

    private String debug(TcpIpConnectionManager connectionManager) {
        NioEventLoopGroup threadingModel = (NioEventLoopGroup) connectionManager.getEventLoopGroup();

        StringBuilder sb = new StringBuilder();
        sb.append("in selectors\n");
        for (NioThread in : threadingModel.getInputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
                NioInboundPipeline socketReader = ((NioChannel) connection.getChannel()).getInboundPipeline();
                if (socketReader.getOwner() == in) {
                    sb.append("\t").append(socketReader).append(" eventCount:").append(socketReader.getLoad()).append("\n");
                }
            }
        }
        sb.append("out selectors\n");
        for (NioThread in : threadingModel.getOutputThreads()) {
            sb.append(in).append(": ").append(in.getEventCount()).append("\n");

            for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
                NioOutboundPipeline socketWriter = ((NioChannel) connection.getChannel()).getOutboundPipeline();
                if (socketWriter.getOwner() == in) {
                    sb.append("\t").append(socketWriter).append(" eventCount:").append(socketWriter.getLoad()).append("\n");
                }
            }
        }

        return sb.toString();
    }
}
