/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapReduceManagedContextTest
        extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @Test
    public void testManagedContextMapper()
            throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final HazelcastInstance h3 = nodeFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(3, h1.getCluster().getMembers().size());
            }
        });

        IMap<String, Integer> m1 = h1.getMap(MAP_NAME);
        for (int i = 0; i < 100; i++) {
            m1.put("key-" + i, i);
        }

        JobTracker tracker = h1.getJobTracker("default");
        KeyValueSource<String, Integer> source = KeyValueSource.fromMap(m1);
        KeyValueSource<String, Integer> wrapper = new InjectedKeyValueSource<String, Integer>(source);
        Job<String, Integer> job = tracker.newJob(wrapper);

        ICompletableFuture<Map<String, Integer>> future = job //
                .mapper(new InjectedMapper()) //
                .combiner(new InjectedCombinerFactory()) //
                .reducer(new InjectedReducerFactory()) //
                .submit();

        Map<String, Integer> result = future.get();

        // No other tests are necessary since we only want to see that instances are injected
        assertNotNull(result);
    }

    public static class InjectedKeyValueSource<K, V>
            extends KeyValueSource<K, V>
            implements DataSerializable, PartitionIdAware, HazelcastInstanceAware {

        private HazelcastInstance hazelcastInstance;
        private volatile KeyValueSource<K, V> keyValueSource;

        public InjectedKeyValueSource() {
        }

        public InjectedKeyValueSource(KeyValueSource<K, V> keyValueSource) {
            this.keyValueSource = keyValueSource;
        }

        @Override
        public boolean open(NodeEngine nodeEngine) {
            hazelcastInstance.getName();
            return keyValueSource.open(nodeEngine);
        }

        @Override
        public boolean hasNext() {
            return keyValueSource.hasNext();
        }

        @Override
        public K key() {
            return keyValueSource.key();
        }

        @Override
        public Map.Entry<K, V> element() {
            return keyValueSource.element();
        }

        @Override
        public boolean reset() {
            return keyValueSource.reset();
        }

        @Override
        public boolean isAllKeysSupported() {
            return keyValueSource.isAllKeysSupported();
        }

        @Override
        public Collection<K> getAllKeys0() {
            return keyValueSource.getAllKeys0();
        }

        @Override
        public void close()
                throws IOException {

            keyValueSource.close();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {

            out.writeObject(keyValueSource);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {

            keyValueSource = in.readObject();
        }

        @Override
        public void setPartitionId(int partitionId) {
            if (keyValueSource instanceof PartitionIdAware) {
                ((PartitionIdAware) keyValueSource).setPartitionId(partitionId);
            }
        }
    }

    public static class InjectedMapper
            implements Mapper, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public void map(Object key, Object value, Context context) {
            String name = hazelcastInstance.getName();
            context.emit(name, 1);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    public static class InjectedCombinerFactory
            implements CombinerFactory<String, Integer, Integer>, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public Combiner<Integer, Integer> newCombiner(String key) {
            hazelcastInstance.getName();
            return new InjectedCombiner();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        private class InjectedCombiner
                extends Combiner<Integer, Integer> {

            private int count;

            @Override
            public void combine(Integer value) {
                count += value;
            }

            @Override
            public Integer finalizeChunk() {
                int count = this.count;
                this.count = 0;
                return count;
            }
        }
    }

    public static class InjectedReducerFactory
            implements ReducerFactory<String, Integer, Integer>, HazelcastInstanceAware {

        private transient HazelcastInstance hazelcastInstance;

        @Override
        public Reducer<Integer, Integer> newReducer(String key) {
            hazelcastInstance.getName();
            return new InjectedReducer();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        private class InjectedReducer
                extends Reducer<Integer, Integer> {

            private volatile int count;

            @Override
            public void reduce(Integer value) {
                count += value;
            }

            @Override
            public Integer finalizeReduce() {
                return count;
            }
        }
    }

}
