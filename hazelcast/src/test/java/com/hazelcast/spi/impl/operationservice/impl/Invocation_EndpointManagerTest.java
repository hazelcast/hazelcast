/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collection;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class Invocation_EndpointManagerTest
        extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private HazelcastInstance hz1;
    private HazelcastInstance hz2;

    @After
    public void tearDown() {
        if (hz1 != null) {
            hz1.getLifecycleService().terminate();
        }
        if (hz2 != null) {
            hz2.getLifecycleService().terminate();
        }
    }

    @Test
    public void testInvocation_whenEndpointManagerIsNoop() {
        hz1 = Hazelcast.newHazelcastInstance(advancedNetworkConfig());
        hz2 = Hazelcast.newHazelcastInstance(advancedNetworkConfig());

        String key = generateKeyOwnedBy(hz2);
        Data dataKey = getSerializationService(hz1).toData(key);
        Partition partition = hz1.getPartitionService().getPartition(key);
        Operation op = new GetOperation("test", dataKey);
        InvocationBuilder builder = getNodeEngineImpl(hz1).getOperationService().createInvocationBuilder(MapService.SERVICE_NAME,
                op, partition.getPartitionId());
        builder.setEndpointManager(new NoopEndpointManager());
        expected.expect(UnsupportedOperationException.class);
        builder.invoke().join();
    }

    private Config advancedNetworkConfig() {
        Config config = new Config();
        config.getAdvancedNetworkConfig().setEnabled(true);
        return config;
    }

    private Operation prepareRemoteOperation(HazelcastInstance target) {
        String key = generateKeyOwnedBy(target);
        Data dataKey = getSerializationService(target).toData(key);
        return new GetOperation("test", dataKey);
    }

    class NoopEndpointManager implements EndpointManager {

        @Override
        public Collection getConnections() {
            return null;
        }

        @Override
        public Collection getActiveConnections() {
            return null;
        }

        @Override
        public boolean registerConnection(Address address, Connection connection) {
            return false;
        }

        @Override
        public Connection getConnection(Address address) {
            return null;
        }

        @Override
        public Connection getOrConnect(Address address) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Connection getOrConnect(Address address, boolean silent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean transmit(Packet packet, Connection connection) {
            return false;
        }

        @Override
        public boolean transmit(Packet packet, Address target) {
            return false;
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {

        }

        @Override
        public void accept(Object o) {

        }
    }
}
