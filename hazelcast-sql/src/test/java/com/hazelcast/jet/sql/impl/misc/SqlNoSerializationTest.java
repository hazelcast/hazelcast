/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test that ensures that there are no unexpected serializations when executing a query on an IMap with the
 * default {@link InMemoryFormat#BINARY} format.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlNoSerializationTest extends OptimizerTestSupport {

    private static final String MAP_NAME = "map";
    private static final int KEY_COUNT = 100;

    @Parameterized.Parameter
    public boolean useIndex;

    private static volatile boolean failOnSerialization;

    @Parameterized.Parameters(name = "useIndex:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(false, true);
    }

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Before
    public void before() {
        Map<Key, Value> localMap = new HashMap<>();

        for (int i = 0; i < KEY_COUNT; i++) {
            localMap.put(new Key(i), new Value(i));
        }

        createMapping(MAP_NAME, Key.class, Value.class);

        IMap<Key, Value> map = instance().getMap(MAP_NAME);

        map.putAll(localMap);

        // An index may change the behavior due to MapContainer.isUseCachedDeserializedValuesEnabled.
        if (useIndex) {
            map.addIndex(new IndexConfig(IndexType.HASH, "val"));
        }

        failOnSerialization = true;
    }

    @After
    public void after() {
        failOnSerialization = false;
    }

    @Test
    public void test() {
        check("SELECT __key, this FROM " + MAP_NAME + " WHERE val = 1", useIndex);
    }

    private void check(String sql, boolean expectedIndexUsage) {
        checkIndexUsage(new SqlStatement(sql), expectedIndexUsage);

        try (SqlResult res = instance().getSql().execute(sql)) {
            int count = 0;

            for (SqlRow row : res) {
                Object key = row.getObject(0);
                Object value = row.getObject(1);

                assertTrue(key instanceof Key);
                assertTrue(value instanceof Value);

                count++;
            }

            assertEquals(1, count);
        }
    }

    private void checkIndexUsage(SqlStatement statement, boolean expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.OBJECT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.OBJECT, false, QueryPath.VALUE_PATH),
                new MapTableField("val", QueryDataType.INT, false, new QueryPath("val", false))
        );
        HazelcastTable table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(MAP_NAME)), mapTableFields),
                KEY_COUNT
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(statement.getSql(), parameterTypes, table);
        assertPlan(
                optimizationResult.getLogical(),
                plan(planRow(0, FullScanLogicalRel.class))
        );
        if (expectedIndexUsage) {
            assertPlan(
                    optimizationResult.getPhysical(),
                    plan(planRow(0, IndexScanMapPhysicalRel.class))
            );
        } else {
            assertPlan(
                    optimizationResult.getPhysical(),
                    plan(planRow(0, FullScanPhysicalRel.class))
            );
        }
    }

    public static class Key implements Externalizable {

        public int key;

        public Key() {
            // No-op
        }

        private Key(int key) {
            this.key = key;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(key);

            if (failOnSerialization) {
                throw new IOException("Key serialization must not happen.");
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            key = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key that = (Key) o;

            return key == that.key;
        }

        @Override
        public int hashCode() {
            return key;
        }
    }

    public static class Value implements Externalizable {

        public int val;

        public Value() {
            // No-op
        }

        private Value(int val) {
            this.val = val;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);

            if (failOnSerialization) {
                throw new IOException("Value serialization must not happen.");
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException {
            val = in.readInt();
        }
    }
}
