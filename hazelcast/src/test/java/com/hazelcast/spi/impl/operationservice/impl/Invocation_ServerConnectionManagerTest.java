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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getSerializationService;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_ServerConnectionManagerTest
        extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private static final String EXPECTED_MSG = "NOOP";

    @Test
    public void testInvocation_whenEndpointManagerIsNoop() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        String key = generateKeyOwnedBy(hz2);
        Data dataKey = getSerializationService(hz1).toData(key);
        Partition partition = hz1.getPartitionService().getPartition(key);

        Operation op = new GetOperation("test", dataKey);
        InvocationBuilder builder
                = getNodeEngineImpl(hz1).getOperationService()
                           .createInvocationBuilder(MapService.SERVICE_NAME, op, partition.getPartitionId());
        builder.setConnectionManager(new NoopEndpointManager());
        expected.expect(UnsupportedOperationException.class);
        expected.expectMessage(EXPECTED_MSG);
        builder.invoke().join();
    }

    class NoopEndpointManager implements ServerConnectionManager {

        @Override
        public Server getServer() {
            return null;
        }

        @Override
        public @Nonnull Collection<ServerConnection> getConnections() {
            return Collections.emptyList();
        }

        @Override
        public int connectionCount(Predicate<ServerConnection> predicate) {
            return 0;
        }

        @Override
        public boolean register(
                Address remoteAddress,
                Address targetAddress,
                Collection<Address> remoteAddressAliases,
                UUID remoteUuid,
                ServerConnection connection,
                int streamId
        ) {
            return false;
        }

        @Override
        public ServerConnection get(@Nonnull Address address, int streamId) {
            return null;
        }

        @Override
        @Nonnull
        public List<ServerConnection> getAllConnections(@Nonnull Address address) {
            return Collections.emptyList();
        }

        @Override
        public ServerConnection getOrConnect(@Nonnull Address address , int streamId) {
            throw new UnsupportedOperationException(EXPECTED_MSG);
        }

        @Override
        public ServerConnection getOrConnect(@Nonnull Address address, boolean silent, int streamId) {
            throw new UnsupportedOperationException(EXPECTED_MSG);
        }

        @Override
        public boolean transmit(Packet packet, Address target, int streamId) {
            return false;
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
        }

        @Override
        public void accept(Packet packet) {
        }

        @Override
        public NetworkStats getNetworkStats() {
            return null;
        }
    }
}
