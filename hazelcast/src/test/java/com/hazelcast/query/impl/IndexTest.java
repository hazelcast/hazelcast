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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.monitor.impl.MemberPartitionStateImpl;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.instance.impl.TestUtil.toData;
import static com.hazelcast.internal.util.IterableUtil.size;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class IndexTest {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    static final short FACTORY_ID = 1;

    final InternalSerializationService ss =
            new DefaultSerializationServiceBuilder().addPortableFactory(FACTORY_ID, new TestPortableFactory()).build();
    MapContainer mapContainer = mock(MapContainer.class);

    {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        MapServiceContext mapServiceContext = mock(MapServiceContext.class);

        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getClusterVersion()).thenReturn(Versions.CURRENT_CLUSTER_VERSION);
        when(mapContainer.getMapConfig()).thenReturn(new MapConfig());
        when(mapContainer.getMapServiceContext()).thenReturn(mapServiceContext);
    }

    final DataRecordFactory recordFactory = new DataRecordFactory(mapContainer, ss);

    @Test
    public void testBasics() {
        testIt(true);
        testIt(false);
    }

    private CachedQueryEntry<?, ?> newRecord(Object key, final Comparable<?> attributeValue) {
        return new CachedQueryEntry<>(ss, toData(key), attributeValue, newExtractor());
    }

    @Test
    public void testRemoveEnumIndex() {
        IndexConfig config = IndexUtils.createTestIndexConfig(IndexType.HASH, "favoriteCity");

        Indexes is = Indexes.newBuilder(ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).build();
        is.addOrGetIndex(config);
        Data key = ss.toData(1);
        Data value = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.ISTANBUL));
        is.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        assertNotNull(is.getIndex(config.getName()));
        Record record = recordFactory.newRecord(key, value);
        is.removeEntry(key, Records.getValueOrCachedValue(record, ss), Index.OperationSource.USER);
        assertEquals(0, is.getIndex(config.getName()).getRecords(SerializableWithEnum.City.ISTANBUL).size());
    }

    @Test
    public void testUpdateEnumIndex() {
        IndexConfig config = IndexUtils.createTestIndexConfig(IndexType.HASH, "favoriteCity");

        Indexes is = Indexes.newBuilder(ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).build();
        is.addOrGetIndex(config);
        Data key = ss.toData(1);
        Data value = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.ISTANBUL));
        is.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);

        Data newValue = ss.toData(new SerializableWithEnum(SerializableWithEnum.City.KRAKOW));
        is.putEntry(new QueryEntry(ss, key, newValue, newExtractor()), value, Index.OperationSource.USER);

        assertEquals(0, is.getIndex(config.getName()).getRecords(SerializableWithEnum.City.ISTANBUL).size());
        assertEquals(1, is.getIndex(config.getName()).getRecords(SerializableWithEnum.City.KRAKOW).size());
    }

    protected Extractors newExtractor() {
        return Extractors.newBuilder(ss).build();
    }

    @Test
    public void testIndex() throws QueryException {
        Indexes indexes = Indexes.newBuilder(ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).build();
        Index dIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "d"));
        Index boolIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "bool"));
        Index strIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "str"));
        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(i % 2 == 0, -10.34d, "joe" + i));
            indexes.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        }
        assertEquals(1000, dIndex.getRecords(-10.34d).size());
        assertEquals(1, strIndex.getRecords("joe23").size());
        assertEquals(500, boolIndex.getRecords(true).size());

        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, 11.34d, "joe"));
            indexes.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        }

        assertEquals(0, dIndex.getRecords(-10.34d).size());
        assertEquals(0, strIndex.getRecords("joe23").size());
        assertEquals(1000, strIndex.getRecords("joe").size());
        assertEquals(1000, boolIndex.getRecords(false).size());
        assertEquals(0, boolIndex.getRecords(true).size());

        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, -1 * (i + 1), "joe" + i));
            indexes.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        }
        assertEquals(0, dIndex.getRecords(1d, true, 1001d, true).size());
        assertEquals(1000, dIndex.getRecords(-1001d, true, -1d, true).size());
        assertEquals(0, dIndex.getRecords(-1d, true, -1001d, true).size());
        clearIndexes(dIndex, boolIndex, strIndex);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(new MainPortable(false, 1 * (i + 1), "joe" + i));
            indexes.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        }
        assertEquals(1000, dIndex.getRecords(1d, true, 1001d, true).size());
        assertEquals(0, dIndex.getRecords(-1d, true, -1001d, true).size());
        assertEquals(400, dIndex.getRecords(Comparison.GREATER, 600d).size());
        assertEquals(401, dIndex.getRecords(Comparison.GREATER_OR_EQUAL, 600d).size());
        assertEquals(9, dIndex.getRecords(Comparison.LESS, 10d).size());
        assertEquals(10, dIndex.getRecords(Comparison.LESS_OR_EQUAL, 10d).size());
        assertEquals(1, size(indexes.query(new AndPredicate(new EqualPredicate("d", 1d), new EqualPredicate("bool", "false")), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(1, size(indexes.query(new AndPredicate(new EqualPredicate("d", 1), new EqualPredicate("bool", Boolean.FALSE)), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(1, size(indexes.query(new AndPredicate(new EqualPredicate("d", "1"), new EqualPredicate("bool", false)), SKIP_PARTITIONS_COUNT_CHECK)));
    }

    private void clearIndexes(Index... indexes) {
        for (Index index : indexes) {
            index.clear();
        }
    }

    @Test
    public void testIndexWithNull() throws QueryException {
        Indexes is = Indexes.newBuilder(ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).build();
        Index strIndex = is.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.SORTED, "str"));

        Data value = ss.toData(new MainPortable(false, 1, null));
        Data key1 = ss.toData(0);
        is.putEntry(new QueryEntry(ss, key1, value, newExtractor()), null, Index.OperationSource.USER);

        value = ss.toData(new MainPortable(false, 2, null));
        Data key2 = ss.toData(1);
        is.putEntry(new QueryEntry(ss, key2, value, newExtractor()), null, Index.OperationSource.USER);


        for (int i = 2; i < 1000; i++) {
            Data key = ss.toData(i);
            value = ss.toData(new MainPortable(false, 1 * (i + 1), "joe" + i));
            is.putEntry(new QueryEntry(ss, key, value, newExtractor()), null, Index.OperationSource.USER);
        }

        assertEquals(2, strIndex.getRecords((Comparable) null).size());
    }

    private class TestPortableFactory implements PortableFactory {

        public Portable create(int classId) {
            switch (classId) {
                case MainPortable.CLASS_ID:
                    return new MainPortable();
                default:
                    return null;
            }
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    private static final class SerializableWithEnum implements DataSerializable {

        enum City {
            ISTANBUL,
            RIZE,
            KRAKOW,
            TRABZON
        }

        private City favoriteCity;

        private SerializableWithEnum() {
        }

        private SerializableWithEnum(City favoriteCity) {
            this.favoriteCity = favoriteCity;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(favoriteCity);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            favoriteCity = in.readObject();
        }
    }

    private static final class MainPortable implements Portable {

        public static final int CLASS_ID = 1;

        byte b;
        boolean bool;
        char c;
        short s;
        int i;
        long l;
        float f;
        double d;
        String str;

        private MainPortable() {
        }

        private MainPortable(boolean bool, double d, String str) {
            this.bool = bool;
            this.d = d;
            this.str = str;
        }

        private MainPortable(byte b, boolean bool, char c, short s, int i, long l, float f, double d, String str) {
            this.b = b;
            this.bool = bool;
            this.c = c;
            this.s = s;
            this.i = i;
            this.l = l;
            this.f = f;
            this.d = d;
            this.str = str;
        }

        public int getClassId() {
            return CLASS_ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByte("b", b);
            writer.writeBoolean("bool", bool);
            writer.writeChar("c", c);
            writer.writeShort("s", s);
            writer.writeInt("i", i);
            writer.writeLong("l", l);
            writer.writeFloat("f", f);
            writer.writeDouble("d", d);
            writer.writeString("str", str);
        }

        public void readPortable(PortableReader reader) throws IOException {
            b = reader.readByte("b");
            bool = reader.readBoolean("bool");
            c = reader.readChar("c");
            s = reader.readShort("s");
            i = reader.readInt("i");
            l = reader.readLong("l");
            f = reader.readFloat("f");
            d = reader.readDouble("d");
            str = reader.readString("str");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MainPortable that = (MainPortable) o;
            if (b != that.b) {
                return false;
            }
            if (bool != that.bool) {
                return false;
            }
            if (c != that.c) {
                return false;
            }
            if (Double.compare(that.d, d) != 0) {
                return false;
            }
            if (Float.compare(that.f, f) != 0) {
                return false;
            }
            if (i != that.i) {
                return false;
            }
            if (l != that.l) {
                return false;
            }
            if (s != that.s) {
                return false;
            }
            return str != null ? str.equals(that.str) : that.str == null;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = (int) b;
            result = 31 * result + (bool ? 1 : 0);
            result = 31 * result + (int) c;
            result = 31 * result + (int) s;
            result = 31 * result + i;
            result = 31 * result + (int) (l ^ (l >>> 32));
            result = 31 * result + (f != +0.0f ? Float.floatToIntBits(f) : 0);
            temp = d != +0.0d ? Double.doubleToLongBits(d) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + (str != null ? str.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "MainPortable{"
                    + "b=" + b
                    + ", bool=" + bool
                    + ", c=" + c
                    + ", s=" + s
                    + ", i=" + i
                    + ", l=" + l
                    + ", f=" + f
                    + ", d=" + d
                    + ", str='" + str + '\''
                    + '}';
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }
    }

    private void testIt(boolean ordered) {
        IndexType type = ordered ? IndexType.SORTED : IndexType.HASH;

        IndexConfig config = IndexUtils.createTestIndexConfig(type, QueryConstants.THIS_ATTRIBUTE_NAME.value());

        IndexImpl index = new IndexImpl(
                config,
                ss,
                newExtractor(),
                copyBehavior,
                PerIndexStats.EMPTY,
                MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT
        );

        assertEquals(0, index.getRecords(0L).size());
        assertEquals(0, index.getRecords(0L, true, 1000L, true).size());
        CachedQueryEntry<?, ?> record5 = newRecord(5L, 55L);
        index.putEntry(record5, null, record5, Index.OperationSource.USER);
        assertEquals(Collections.<QueryableEntry>singleton(record5), index.getRecords(55L));

        CachedQueryEntry<?, ?> record6 = newRecord(6L, 66L);
        index.putEntry(record6, null, record6, Index.OperationSource.USER);

        assertEquals(Collections.<QueryableEntry>singleton(record6), index.getRecords(66L));

        CachedQueryEntry<?, ?> newRecord5 = newRecord(5L, 555L);
        index.putEntry(newRecord5, record5, newRecord5, Index.OperationSource.USER);
        record5 = newRecord5;

        assertEquals(0, index.getRecords(55L).size());
        assertEquals(Collections.<QueryableEntry>singleton(record5), index.getRecords(555L));

        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getRecords(55L, true, 555L, true).size());
        assertEquals(2, index.getRecords(66L, true, 555L, true).size());
        assertEquals(1, index.getRecords(555L, true, 555L, true).size());
        CachedQueryEntry<?, ?> record50 = newRecord(50L, 555L);
        index.putEntry(record50, null, record50, Index.OperationSource.USER);
        assertEquals(new HashSet<QueryableEntry>(asList(record5, record50)), index.getRecords(555L));

        Map<Data, QueryableEntry> records = getRecordMap(index, 555L);
        assertNotNull(records);
        assertEquals(2, records.size());
        assertEquals(record5, records.get(record5.getKeyData()));
        assertEquals(record50, records.get(record50.getKeyData()));
        assertEquals(2, index.getRecords(555L).size());
        assertEquals(3, index.getRecords(55L, true, 555L, true).size());
        assertEquals(3, index.getRecords(66L, true, 555L, true).size());
        assertEquals(2, index.getRecords(555L, true, 555L, true).size());
        assertEquals(0, index.getRecords(Comparison.LESS, 66L).size());
        assertEquals(1, index.getRecords(Comparison.LESS_OR_EQUAL, 66L).size());
        assertEquals(1, index.getRecords(Comparison.LESS_OR_EQUAL, 67L).size());
        assertEquals(2, index.getRecords(Comparison.GREATER, 66L).size());
        assertEquals(3, index.getRecords(Comparison.GREATER_OR_EQUAL, 66L).size());
        assertEquals(3, index.getRecords(Comparison.GREATER_OR_EQUAL, 61L).size());
        assertEquals(3, index.getRecords(new Comparable[]{66L, 555L, 34234L}).size());
        assertEquals(2, index.getRecords(new Comparable[]{555L, 34234L}).size());

        index.removeEntry(record5, Index.OperationSource.USER);

        assertEquals(Collections.<QueryableEntry>singleton(record50), index.getRecords(555L));

        records = getRecordMap(index, 555L);
        assertNotNull(records);
        assertNull(records.get(5L));
        assertEquals(record50, records.get(toData(50L)));
        assertEquals(1, index.getRecords(555L).size());
        assertEquals(2, index.getRecords(55L, true, 555L, true).size());
        assertEquals(2, index.getRecords(66L, true, 555L, true).size());
        assertEquals(1, index.getRecords(555L, true, 555L, true).size());
        assertEquals(0, index.getRecords(Comparison.LESS, 66L).size());
        assertEquals(1, index.getRecords(Comparison.LESS_OR_EQUAL, 66L).size());
        assertEquals(1, index.getRecords(Comparison.LESS_OR_EQUAL, 67L).size());
        assertEquals(1, index.getRecords(Comparison.GREATER, 66L).size());
        assertEquals(2, index.getRecords(Comparison.GREATER_OR_EQUAL, 66L).size());
        assertEquals(2, index.getRecords(Comparison.GREATER_OR_EQUAL, 61L).size());

        index.removeEntry(record50, Index.OperationSource.USER);

        assertEquals(0, index.getRecords(555L).size());

        records = getRecordMap(index, 555L);
        assertNull(records);
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(1, index.getRecords(55L, true, 555L, true).size());
        assertEquals(1, index.getRecords(66L, true, 555L, true).size());
        assertEquals(0, index.getRecords(555L, true, 555L, true).size());

        index.removeEntry(record6, Index.OperationSource.USER);

        assertEquals(0, index.getRecords(66L).size());

        assertNull(getRecordMap(index, 66L));
        assertEquals(0, index.getRecords(555L).size());
        assertEquals(0, index.getRecords(55L, true, 555L, true).size());
        assertEquals(0, index.getRecords(66L, true, 555L, true).size());
        assertEquals(0, index.getRecords(555L, true, 555L, true).size());
    }

    private Map<Data, QueryableEntry> getRecordMap(IndexImpl index, Comparable indexValue) {
        Set<QueryableEntry> records = index.getRecords(indexValue);
        if (records.isEmpty()) {
            return null;
        }
        Map<Data, QueryableEntry> recordMap = new HashMap<Data, QueryableEntry>(records.size());
        for (QueryableEntry entry : records) {
            recordMap.put(entry.getKeyData(), entry);
        }
        return recordMap;
    }
}
