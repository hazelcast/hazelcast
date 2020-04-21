/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.TestClockProvider;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControlFactory;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static groovy.util.GroovyTestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationChannelTest extends SqlTestSupport {

    private QueryOperationHandlerImpl operationHandler;

    @After
    public void after() {
        if (operationHandler != null) {
            operationHandler.stop();
        }
    }

    @Test
    public void testOrdering() {
        TestConnection connection = new TestConnection();
        TestNodeServiceProvider serviceProvider = new TestNodeServiceProvider(connection);

        operationHandler = new QueryOperationHandlerImpl(
            "test",
            serviceProvider,
            new DefaultSerializationServiceBuilder().build(),
            new QueryStateRegistry(TestClockProvider.createDefault()),
            1000,
            SimpleFlowControlFactory.INSTANCE,
            1,
            1
        );

        QueryOperationChannel channel = operationHandler.createChannel(UUID.randomUUID(), UUID.randomUUID());

        QueryFlowControlExchangeOperation operation = new QueryFlowControlExchangeOperation(
            QueryId.create(UUID.randomUUID()), 1, 1L
        );

        channel.submit(operation);
        channel.submit(operation);

        assertEquals(0, connection.getUnorderedCount());
        assertEquals(2, connection.getOrderedCount());
    }

    private static class TestNodeServiceProvider implements NodeServiceProvider {

        private final TestConnection connection;

        private TestNodeServiceProvider(TestConnection connection) {
            this.connection = connection;
        }

        @Override
        public Collection<UUID> getDataMemberIds() {
            return null;
        }

        @Override
        public Connection getConnection(UUID memberId) {
            return connection;
        }

        @Override
        public MapContainer getMap(String name) {
            return null;
        }

        @Override
        public ILogger getLogger(Class<?> clazz) {
            return null;
        }

        @Override
        public long currentTimeMillis() {
            return 0;
        }

        @Override
        public UUID getLocalMemberId() {
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    private static class TestConnection implements Connection {

        private int orderedCount;
        private int unorderedCount;

        private int getOrderedCount() {
            return orderedCount;
        }

        private int getUnorderedCount() {
            return unorderedCount;
        }

        @Override
        public ConcurrentMap attributeMap() {
            return null;
        }

        @Override
        public boolean isAlive() {
            return false;
        }

        @Override
        public long lastReadTimeMillis() {
            return 0;
        }

        @Override
        public long lastWriteTimeMillis() {
            return 0;
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return null;
        }

        @Override
        public Address getRemoteAddress() {
            return null;
        }

        @Override
        public void setRemoteAddress(Address remoteAddress) {
            // No-op.
        }

        @Override
        public InetAddress getInetAddress() {
            return null;
        }

        @Override
        public boolean writeOrdered(OutboundFrame frame) {
            orderedCount++;

            return true;
        }

        @Override
        public boolean write(OutboundFrame frame) {
            unorderedCount++;

            return true;
        }

        @Override
        public void close(String reason, Throwable cause) {
            // No-op.
        }

        @Override
        public String getCloseReason() {
            return null;
        }

        @Override
        public Throwable getCloseCause() {
            return null;
        }
    }
}
