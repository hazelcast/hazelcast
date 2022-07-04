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

package com.hazelcast.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.Map.Entry;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapQueryPartitionIteratorTest extends AbstractMapQueryPartitionIteratorTest {

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        instanceProxy = factory.newHazelcastInstance(getConfig());
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Override
    protected <K, V, R> Iterator<R> getIterator(
            IMap<K, V> map,
            int fetchSize,
            int partitionId,
            Projection<Entry<K, V>, R> projection,
            Predicate<K, V> predicate
    ) {
        return ((MapProxyImpl<K, V>) map).iterator(fetchSize, partitionId, projection, predicate);
    }
}
