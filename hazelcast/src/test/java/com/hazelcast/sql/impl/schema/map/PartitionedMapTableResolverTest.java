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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.Partition;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionedMapTableResolverTest extends MapSchemaTestSupport {

    private static final String MAP_SERIALIZABLE_OBJECT = "S_O";
    private static final String MAP_SERIALIZABLE_BINARY = "S_B";
    private static final String MAP_PORTABLE_OBJECT = "P_O";
    private static final String MAP_PORTABLE_BINARY = "P_B";
    private static final String MAP_WILDCARD = "Template*";
    private static final String MAP_FROM_WILDCARD = "TemplateMap";

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance;

    @Before
    public void before() {
        factory = createHazelcastInstanceFactory(2);

        Config config = new Config()
            .addMapConfig(new MapConfig(MAP_SERIALIZABLE_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
            .addMapConfig(new MapConfig(MAP_SERIALIZABLE_BINARY).setInMemoryFormat(InMemoryFormat.BINARY))
            .addMapConfig(new MapConfig(MAP_PORTABLE_OBJECT).setInMemoryFormat(InMemoryFormat.OBJECT))
            .addMapConfig(new MapConfig(MAP_PORTABLE_BINARY).setInMemoryFormat(InMemoryFormat.BINARY))
            .addMapConfig(new MapConfig(MAP_WILDCARD))
            .setSerializationConfig(new SerializationConfig().addPortableFactory(1, classId -> {
                if (classId == 1) {
                    return new PortableKey();
                } else if (classId == 2) {
                    return new PortableValue();
                }

                throw new IllegalArgumentException("Unknown classId: " + classId);
            }));

        instance = factory.newHazelcastInstance(config);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testStatistics() {
        // Start the second instance and prepare maps.
        factory.newHazelcastInstance();

        String mapName = "statsMap";

        instance.getConfig().addMapConfig(new MapConfig(mapName).setBackupCount(1));

        IMap<Integer, Integer> map = instance.getMap(mapName);

        // Put local and remote key to the map to verify that backups are excluded from estimation.
        int localKey = 0;

        while (true) {
            Partition partition = instance.getPartitionService().getPartition(localKey);

            if (partition.getOwner().getUuid().equals(instance.getCluster().getLocalMember().getUuid())) {
                break;
            }

            localKey++;
        }

        // Get the partitioning key that is remote to the instance.
        int remoteKey = 0;

        while (true) {
            Partition partition = instance.getPartitionService().getPartition(remoteKey);

            if (!partition.getOwner().getUuid().equals(instance.getCluster().getLocalMember().getUuid())) {
                break;
            }

            remoteKey++;
        }

        map.put(localKey, 1);
        map.put(remoteKey, 1);

        // Check statistics.
        Table tableStats = getExistingTable(resolver().getTables(), mapName);
        assertEquals(2, tableStats.getStatistics().getRowCount());
    }

    @Test
    public void testSearchPaths() {
        List<List<String>> searchPaths = resolver().getDefaultSearchPaths();

        assertEquals(1, searchPaths.size());
        assertEquals(Arrays.asList(QueryUtils.CATALOG, QueryUtils.SCHEMA_NAME_PARTITIONED), searchPaths.get(0));
    }

    /**
     * Ensure that not started predefined maps are visible.
     */
    @Test
    public void testNotStartedPredefinedMaps() {
        Collection<Table> tables = resolver().getTables();

        assertEquals(4, tables.size());

        checkEmpty(getExistingTable(tables, MAP_SERIALIZABLE_OBJECT));
        checkEmpty(getExistingTable(tables, MAP_SERIALIZABLE_BINARY));
        checkEmpty(getExistingTable(tables, MAP_PORTABLE_OBJECT));
        checkEmpty(getExistingTable(tables, MAP_PORTABLE_BINARY));
    }

    /**
     * Ensure that an exception is thrown for an empty started map.
     */
    @Test
    public void testEmptyMap() {
        String mapName = MAP_SERIALIZABLE_OBJECT;

        IMap<Integer, Integer> map = instance.getMap(mapName);

        map.put(1, 1);
        map.remove(1);
        assertEquals(0, map.size());

        Collection<Table> tables = resolver().getTables();

        assertEquals(4, tables.size());

        checkEmpty(getExistingTable(tables, mapName));
    }

    /**
     * Test maps with different storage formats and serializers.
     */
    @Test
    public void testNormalMaps() {
        // Add values to maps.
        IMap<SerializableKey, SerializableValue> so = instance.getMap(MAP_SERIALIZABLE_OBJECT);
        IMap<SerializableKey, SerializableValue> sb = instance.getMap(MAP_SERIALIZABLE_BINARY);
        IMap<PortableKey, PortableValue> po = instance.getMap(MAP_PORTABLE_OBJECT);
        IMap<PortableKey, PortableValue> pb = instance.getMap(MAP_PORTABLE_BINARY);
        IMap<SerializableKey, SerializableValue> dynamic = instance.getMap(MAP_FROM_WILDCARD);

        so.put(new SerializableKey(1, 1), new SerializableValue(1, 1));
        sb.put(new SerializableKey(1, 1), new SerializableValue(1, 1));
        po.put(new PortableKey(1, 1), new PortableValue(1, 1));
        pb.put(new PortableKey(1, 1), new PortableValue(1, 1));
        dynamic.put(new SerializableKey(1, 1), new SerializableValue(1, 1));

        // Analyze metadata.
        Collection<Table> tables = resolver().getTables();
        assertEquals(5, tables.size());

        // Check serializable maps. They all should have the same schema.
        MapTableField[] expectedFields = new MapTableField[] {
            hiddenField(KEY, QueryDataType.OBJECT, true),
            field("field1", QueryDataType.INT, true),
            field("field2", QueryDataType.INT, true),
            field("field3", QueryDataType.INT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        };

        checkFields(getExistingTable(tables, MAP_SERIALIZABLE_OBJECT), expectedFields);
        checkFields(getExistingTable(tables, MAP_SERIALIZABLE_BINARY), expectedFields);
        checkFields(getExistingTable(tables, MAP_FROM_WILDCARD), expectedFields);

        // Check portable in the OBJECT mode.
        checkFields(
            getExistingTable(tables, MAP_PORTABLE_OBJECT),
            hiddenField(KEY, QueryDataType.OBJECT, true),
            field("portableField1", QueryDataType.INT, true),
            field("portableField2", QueryDataType.INT, true),
            field("portableField3", QueryDataType.INT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        );

        // Check portable in the BINARY mode.
        checkFields(
            getExistingTable(tables, MAP_PORTABLE_BINARY),
            hiddenField(KEY, QueryDataType.OBJECT, true),
            field("portableField1", QueryDataType.INT, true),
            field("portableField2", QueryDataType.INT, true),
            field("portableField3", QueryDataType.INT, false),
            hiddenField(VALUE, QueryDataType.OBJECT, false)
        );

        // Destroy a dynamic map and ensure that it is no longer shown.
        dynamic.destroy();

        tables = resolver().getTables();
        assertEquals(4, tables.size());
        assertNull(getTable(tables, MAP_FROM_WILDCARD));
    }

    private static void checkFields(Table table, MapTableField... expectedFields) {
        if (expectedFields == null) {
            expectedFields = new MapTableField[0];
        }

        assertEquals(expectedFields.length, table.getFieldCount());

        for (int i = 0; i < expectedFields.length; i++) {
            MapTableField field = table.getField(i);
            MapTableField expectedField = expectedFields[i];

            assertEquals(expectedField, field);
        }
    }

    @Test
    public void testDeserializationError() {
        IMap<Integer, BadValue> map = instance.getMap(MAP_SERIALIZABLE_BINARY);
        map.put(1, new BadValue());

        Collection<Table> tables = resolver().getTables();
        Table table = getExistingTable(tables, MAP_SERIALIZABLE_BINARY);

        try {
            table.getFieldCount();

            fail("Exception is not thrown: " + table.getName());
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertTrue(e.getMessage().contains("Failed to resolve value metadata"));
        }
    }

    private PartitionedMapTableResolver resolver() {
        return new PartitionedMapTableResolver(nodeEngine(instance));
    }

    private static void checkEmpty(Table table) {
        try {
            table.getFieldCount();

            fail("Exception is not thrown: " + table.getName());
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.GENERIC, e.getCode());
            assertTrue(e.getMessage().contains("Cannot resolve IMap schema because it doesn't have entries on the local member"));
        }
    }

    private static Table getExistingTable(Collection<Table> tables, String name) {
        Table table = getTable(tables, name);

        if (table != null) {
            return table;
        }

        throw new RuntimeException("Table not found: " + name);
    }

    private static Table getTable(Collection<Table> tables, String name) {
        for (Table table : tables) {
            if (table.getName().equals(name)) {
                PartitionedMapTable table0 = (PartitionedMapTable) table;

                assertEquals(QueryUtils.SCHEMA_NAME_PARTITIONED, table.getSchemaName());

                if (table0.getException() == null) {
                    assertNotNull(table0.getKeyDescriptor());
                    assertNotNull(table0.getValueDescriptor());
                }

                return table;
            }
        }

        return null;
    }

    @SuppressWarnings("unused")
    private static class SerializableKey implements Serializable {

        public int field1;
        public int field2;

        private SerializableKey() {
            // No-op.
        }

        private SerializableKey(int field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }
    }

    @SuppressWarnings("unused")
    private static class SerializableValue implements Serializable {

        public long field2;
        public int field3;

        private SerializableValue() {
            // No-op.
        }

        private SerializableValue(long field2, int field3) {
            this.field2 = field2;
            this.field3 = field3;
        }
    }

    private static class PortableKey implements Portable {

        public int field1;
        public int field2;

        private PortableKey() {
            // No-op.
        }

        private PortableKey(int field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("portableField1", field1);
            writer.writeInt("portableField2", field2);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            field1 = reader.readInt("portableField1");
            field2 = reader.readInt("portableField2");
        }
    }

    private static class PortableValue implements Portable {

        public long field2;
        public int field3;

        private PortableValue() {
            // No-op.
        }

        private PortableValue(long field2, int field3) {
            this.field2 = field2;
            this.field3 = field3;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 2;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeLong("portableField2", field2);
            writer.writeInt("portableField3", field3);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            field2 = reader.readLong("portableField2");
            field3 = reader.readInt("portableField3");
        }
    }

    private static class BadValue implements DataSerializable {
        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            throw new IOException("Bad value!");
        }
    }
}
