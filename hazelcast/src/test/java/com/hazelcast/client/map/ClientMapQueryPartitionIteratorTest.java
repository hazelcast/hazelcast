/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.map.AbstractMapQueryPartitionIteratorTest;
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
public class ClientMapQueryPartitionIteratorTest extends AbstractMapQueryPartitionIteratorTest {

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
        instance = factory.newHazelcastInstance(smallInstanceConfig());
        mapProxy = factory.newHazelcastClient().getMap(randomMapName());
    }

    @Override
    protected <K, V, R> Iterator<R> getIterator(int fetchSize, int partitionId, Projection<Map.Entry<K, V>, R> projection,
                                                Predicate<K, V> predicate) {
        return ((ClientMapProxy<K, V>) mapProxy).iterator(10, partitionId, projection, predicate);
    }
}
