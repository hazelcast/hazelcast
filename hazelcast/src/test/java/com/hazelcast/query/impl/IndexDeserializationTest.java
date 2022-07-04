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

package com.hazelcast.query.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexDeserializationTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 10;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameter(1)
    public CacheDeserializedValues cacheDeserializedValues;

    @Parameters(name = "inMemoryFormat: {0}, cacheDeserializedValues: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.OBJECT, CacheDeserializedValues.NEVER},
                {InMemoryFormat.BINARY, CacheDeserializedValues.NEVER},
                {InMemoryFormat.OBJECT, CacheDeserializedValues.INDEX_ONLY},
                {InMemoryFormat.BINARY, CacheDeserializedValues.INDEX_ONLY},
                {InMemoryFormat.OBJECT, CacheDeserializedValues.ALWAYS},
                {InMemoryFormat.BINARY, CacheDeserializedValues.ALWAYS}
        });
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig("map")
                .setInMemoryFormat(inMemoryFormat)
                .setCacheDeserializedValues(cacheDeserializedValues)
                .setMetadataPolicy(MetadataPolicy.OFF)
                .addIndexConfig(new IndexConfig(IndexType.HASH, "v1"))
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "v2"))
                .addIndexConfig(new IndexConfig(IndexType.BITMAP, "v3"));
        return config;
    }

    @Test
    public void testDeserialization() {
        IMap<Integer, Record> map = createHazelcastInstance().getMap("map");

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.put(i, new Record(i));
        }

        assertEquals(ENTRY_COUNT, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getInsertCount());
        }

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.set(i, new Record(i));
        }
        assertEquals(inMemoryFormat == InMemoryFormat.OBJECT ? ENTRY_COUNT : ENTRY_COUNT * 2, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getUpdateCount());
        }

        Record.deserializationCount.set(0);
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            map.delete(i);
        }
        assertEquals(inMemoryFormat == InMemoryFormat.OBJECT
                || cacheDeserializedValues != CacheDeserializedValues.NEVER ? 0 : ENTRY_COUNT, Record.deserializationCount.get());
        assertEquals(3, map.getLocalMapStats().getIndexStats().size());
        for (LocalIndexStats indexStats : map.getLocalMapStats().getIndexStats().values()) {
            assertEquals(ENTRY_COUNT, indexStats.getRemoveCount());
        }
    }

    static class Record implements DataSerializable {

        static final AtomicInteger deserializationCount = new AtomicInteger();

        int v1;
        int v2;
        int v3;

        @SuppressWarnings("unused")
        Record() {
        }

        Record(int key) {
            this.v1 = key * 1000;
            this.v2 = key * 1001;
            this.v3 = key * 1002;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(v1);
            out.writeInt(v2);
            out.writeInt(v3);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            v1 = in.readInt();
            v2 = in.readInt();
            v3 = in.readInt();

            deserializationCount.incrementAndGet();
        }

    }

}
