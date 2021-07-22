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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.state.QueryStateCallback;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Helper test classes.
 */
public class SqlTestSupport extends HazelcastTestSupport {

    @ClassRule
    public static OverridePropertyRule enableJetRule = OverridePropertyRule.set("hz.jet.enabled", "true");

    /**
     * Check object equality with additional hash code check.
     *
     * @param first First object.
     * @param second Second object.
     * @param expected Expected result.
     */
    public static void checkEquals(Object first, Object second, boolean expected) {
        if (expected) {
            assertEquals(first, second);
            assertEquals(first.hashCode(), second.hashCode());
        } else {
            assertNotEquals(first, second);
        }
    }

    public static InternalSerializationService getSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    public static InternalSerializationService getSerializationService(HazelcastInstance instance) {
        return (InternalSerializationService) nodeEngine(instance).getSerializationService();
    }

    public static MapContainer getMapContainer(IMap<?, ?> map) {
        return ((MapProxyImpl<?, ?>) map).getService().getMapServiceContext().getMapContainer(map.getName());
    }

    public static <T> T serialize(Object original) {
        InternalSerializationService ss = getSerializationService();

        return getSerializationService().toObject(ss.toData(original));
    }

    public static <T> T serializeAndCheck(Object original, int expectedClassId) {
        assertTrue(original instanceof IdentifiedDataSerializable);

        IdentifiedDataSerializable original0 = (IdentifiedDataSerializable) original;

        assertEquals(SqlDataSerializerHook.F_ID, original0.getFactoryId());
        assertEquals(expectedClassId, original0.getClassId());

        return serialize(original);
    }

    public static ListRowBatch createMonotonicBatch(int startValue, int size) {
        List<Row> rows = new ArrayList<>(size);

        for (int i = startValue; i < startValue + size; i++) {
            rows.add(HeapRow.of(i));
        }

        return new ListRowBatch(rows);
    }

    public static void checkMonotonicBatch(RowBatch batch, int startValue, int size) {
        assertEquals(size, batch.getRowCount());

        for (int i = 0; i < size; i++) {
            int value = batch.getRow(i).get(0);

            assertEquals(startValue + i, value);
        }
    }

    public static QueryFragmentContext emptyFragmentContext() {
        return emptyFragmentContext(Collections.emptyList());
    }

    public static QueryFragmentContext emptyFragmentContext(List<Object> args) {
        QueryStateCallback stateCallback = new QueryStateCallback() {
            @Override
            public void onFragmentFinished() {
                // No-op.
            }

            @Override
            public void cancel(Exception e, boolean local) {
                // No-op.
            }

            @Override
            public void checkCancelled() {
                // No-op.
            }
        };

        return new QueryFragmentContext(
            args,
            new LoggingQueryFragmentScheduleCallback(),
            stateCallback,
            new DefaultSerializationServiceBuilder().build()
        );
    }

    /**
     * Creates an opaque plan for tests where concrete values inside the plan are not important.
     *
     * @return Plan.
     */
    public static Plan opaquePlan() {
        return new Plan(
            Collections.emptyMap(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null,
            QueryParameterMetadata.EMPTY,
            null,
            Collections.emptySet(),
            Collections.emptyList()
        );
    }

    public static QueryPath keyPath(String path) {
        return path(path, true);
    }

    public static QueryPath valuePath(String path) {
        return path(path, false);
    }

    public static QueryPath path(String path, boolean key) {
        return new QueryPath(path, key);
    }

    public static NodeEngineImpl nodeEngine(HazelcastInstance instance) {
        return Accessors.getNodeEngineImpl(instance);
    }

    public static Row row(Object... values) {
        assertNotNull(values);
        assertTrue(values.length > 0);

        return new HeapRow(values);
    }

    public static SqlInternalService sqlInternalService(HazelcastInstance instance) {
        return nodeEngine(instance).getSqlService().getInternalService();
    }

    public static List<SqlRow> execute(HazelcastInstance member, String sql, Object... params) {
        SqlStatement query = new SqlStatement(sql);

        if (params != null) {
            query.setParameters(Arrays.asList(params));
        }

        return executeStatement(member, query);
    }

    public static List<SqlRow> executeStatement(HazelcastInstance member, SqlStatement query) {
        List<SqlRow> rows = new ArrayList<>();

        try (SqlResult result = member.getSql().execute(query)) {
            for (SqlRow row : result) {
                rows.add(row);
            }
        }

        return rows;
    }

    public static void clearPlanCache(HazelcastInstance member) {
        ((SqlServiceImpl) member.getSql()).getPlanCache().clear();
    }

    public static PartitionIdSet getLocalPartitions(HazelcastInstance member) {
        PartitionService partitionService = member.getPartitionService();

        PartitionIdSet res = new PartitionIdSet(partitionService.getPartitions().size());

        for (Partition partition : partitionService.getPartitions()) {
            if (partition.getOwner().localMember()) {
                res.add(partition.getPartitionId());
            }
        }

        return res;
    }

    public static <K> List<K> getLocalKeys(
        HazelcastInstance member,
        int count,
        IntFunction<K> keyProducer
    ) {
        return new ArrayList<>(getLocalEntries(member, count, keyProducer, keyProducer).keySet());
    }

    public static <K, V> Map.Entry<K, V> getLocalEntry(
        HazelcastInstance member,
        IntFunction<K> keyProducer,
        IntFunction<V> valueProducer
    ) {
        return getLocalEntries(member, 1, keyProducer, valueProducer).entrySet().iterator().next();
    }

    public static <K, V> Map<K, V> getLocalEntries(
        HazelcastInstance member,
        int count,
        IntFunction<K> keyProducer,
        IntFunction<V> valueProducer
    ) {
        if (count == 0) {
            return Collections.emptyMap();
        }

        PartitionService partitionService = member.getPartitionService();

        Map<K, V> res = new LinkedHashMap<>();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            K key = keyProducer.apply(i);

            if (key == null) {
                continue;
            }

            Partition partition = partitionService.getPartition(key);

            if (!partition.getOwner().localMember()) {
                continue;
            }

            V value = valueProducer.apply(i);

            if (value == null) {
                continue;
            }

            res.put(key, value);

            if (res.size() == count) {
                break;
            }
        }

        if (res.size() < count) {
            throw new RuntimeException("Failed to get the necessary number of keys: " + res.size());
        }

        return res;
    }

}
