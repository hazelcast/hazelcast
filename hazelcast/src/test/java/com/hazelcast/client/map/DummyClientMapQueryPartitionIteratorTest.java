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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.AbstractMapQueryPartitionIteratorTest;
import com.hazelcast.map.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DummyClientMapQueryPartitionIteratorTest extends AbstractMapQueryPartitionIteratorTest {

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        HazelcastInstance server = factory.newHazelcastInstance(getConfig());
        instanceProxy = factory.newHazelcastClient(getClientConfig(server));
    }

    private ClientConfig getClientConfig(HazelcastInstance instance) {
        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();
        ClientNetworkConfig networkConfig = new ClientNetworkConfig()
                .setSmartRouting(false)
                .addAddress(addressString);

        return new ClientConfig().setNetworkConfig(networkConfig);
    }

    @Override
    protected <K, V, R> Iterator<R> getIterator(
            IMap<K, V> map,
            int fetchSize,
            int partitionId,
            Projection<Map.Entry<K, V>, R> projection,
            Predicate<K, V> predicate
    ) {
        return ((ClientMapProxy<K, V>) map).iterator(10, partitionId, projection, predicate);
    }
}
