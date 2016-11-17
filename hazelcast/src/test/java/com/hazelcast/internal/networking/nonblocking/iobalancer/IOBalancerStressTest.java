/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nonblocking.iobalancer;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.internal.networking.nonblocking.MigratableHandler;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThread;
import com.hazelcast.internal.networking.nonblocking.NonBlockingIOThreadingModel;
import com.hazelcast.internal.networking.nonblocking.NonBlockingSocketReader;
import com.hazelcast.internal.networking.nonblocking.NonBlockingSocketWriter;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
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
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testEachConnectionUseDifferentSelectorEventually() {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_BALANCER_INTERVAL_SECONDS.getName(), "1");
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "2");

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

        Map<NonBlockingIOThread, Set<MigratableHandler>> handlersPerSelector = getHandlersPerSelector(connectionManager);

        try {
            for (Map.Entry<NonBlockingIOThread, Set<MigratableHandler>> entry : handlersPerSelector.entrySet()) {
                NonBlockingIOThread selector = entry.getKey();
                Set<MigratableHandler> handlers = entry.getValue();
                assertBalanced(selector, handlers);
            }
        } catch (AssertionError e) {
            // if something fails, we want to see the debug
            System.out.println(debug(connectionManager));
            throw e;
        }
    }

    public String debug(TcpIpConnectionManager connectionManager) {
        NonBlockingIOThreadingModel threadingModel = (NonBlockingIOThreadingModel) connectionManager.getIoThreadingModel();

        StringBuffer sb = new StringBuffer();
        sb.append("in selectors\n");
        for (NonBlockingIOThread in : threadingModel.getInputThreads()) {
            sb.append(in + " :" + in.getEventCount() + "\n");

            for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
                NonBlockingSocketReader socketReader = (NonBlockingSocketReader) connection.getSocketReader();
                if (socketReader.getOwner() == in) {
                    sb.append("\t" + socketReader + " eventCount:" + socketReader.getEventCount() + "\n");
                }
            }
        }
        sb.append("out selectors\n");
        for (NonBlockingIOThread in : threadingModel.getOutputThreads()) {
            sb.append(in + " :" + in.getEventCount() + "\n");

            for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
                NonBlockingSocketWriter socketWriter = (NonBlockingSocketWriter) connection.getSocketWriter();
                if (socketWriter.getOwner() == in) {
                    sb.append("\t" + socketWriter + " eventCount:" + socketWriter.getEventCount() + "\n");
                }
            }
        }

        return sb.toString();
    }


    private Map<NonBlockingIOThread, Set<MigratableHandler>> getHandlersPerSelector(TcpIpConnectionManager connectionManager) {
        Map<NonBlockingIOThread, Set<MigratableHandler>> handlersPerSelector = new HashMap<NonBlockingIOThread, Set<MigratableHandler>>();
        for (TcpIpConnection connection : connectionManager.getActiveConnections()) {
            add(handlersPerSelector, (MigratableHandler) connection.getSocketReader());
            add(handlersPerSelector, (MigratableHandler) connection.getSocketWriter());
        }
        return handlersPerSelector;
    }

    /**
     * A selector is balanced if:
     * - it has 1 active handler (so a high event count)
     * - potentially 1 dead handler (duplicate connection). So event count should be low.
     *
     * @param selector
     * @param handlers
     */
    public void assertBalanced(NonBlockingIOThread selector, Set<MigratableHandler> handlers) {
        assertTrue("no handlers were found for selector:" + selector, handlers.size() > 0);
        assertTrue("too many handlers were found for selector:" + selector, handlers.size() <= 2);

        Iterator<MigratableHandler> iterator = handlers.iterator();
        MigratableHandler activeHandler = iterator.next();
        if (handlers.size() == 2) {
            MigratableHandler deadHandler = iterator.next();
            if (activeHandler.getEventCount() < deadHandler.getEventCount()) {
                MigratableHandler tmp = deadHandler;
                deadHandler = activeHandler;
                activeHandler = tmp;
            }

            // the maximum number of events seen on the dead connection is 3. 10 should be save to assume the
            // connection is dead.
            assertTrue("at most 10 event should have been received, number of events received:"
                    + deadHandler.getEventCount(), deadHandler.getEventCount() < 10);
        }

        assertTrue("activeHandlerEvent count should be at least 1000, but was:" + activeHandler.getEventCount(),
                activeHandler.getEventCount() > 1000);
    }

    private void add(Map<NonBlockingIOThread, Set<MigratableHandler>> handlersPerSelector, MigratableHandler handler) {
        Set<MigratableHandler> handlers = handlersPerSelector.get(handler.getOwner());
        if (handlers == null) {
            handlers = new HashSet<MigratableHandler>();
            handlersPerSelector.put(handler.getOwner(), handlers);
        }
        handlers.add(handler);
    }
}