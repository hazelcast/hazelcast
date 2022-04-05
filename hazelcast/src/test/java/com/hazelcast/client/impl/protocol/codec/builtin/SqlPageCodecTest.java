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

package com.hazelcast.client.impl.protocol.codec.builtin;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.client.SqlPage;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.NULL;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Precise checks of the column encoding in the {@link SqlPageCodec}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPageCodecTest {

    private InternalSerializationService serializationService;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void testVarchar() {
        checkGenerated(VARCHAR, VarcharValueFactory::new);
    }

    @Test
    public void testBoolean() {
        checkGenerated(BOOLEAN, BooleanValueFactory::new);
    }

    @Test
    public void testTinyint() {
        checkGenerated(TINYINT, TinyintValueFactory::new);
    }

    @Test
    public void testSmallint() {
        checkGenerated(SMALLINT, SmallintValueFactory::new);
    }

    @Test
    public void testInteger() {
        checkGenerated(INTEGER, IntegerValueFactory::new);
    }

    @Test
    public void testBigint() {
        checkGenerated(BIGINT, BigintValueFactory::new);
    }

    @Test
    public void testDecimal() {
        checkGenerated(DECIMAL, DecimalValueFactory::new);
    }

    @Test
    public void testReal() {
        checkGenerated(REAL, RealValueFactory::new);
    }

    @Test
    public void testDouble() {
        checkGenerated(DOUBLE, DoubleValueFactory::new);
    }

    @Test
    public void testDate() {
        checkGenerated(DATE, DateValueFactory::new);
    }

    @Test
    public void testTime() {
        checkGenerated(TIME, TimeValueFactory::new);
    }

    @Test
    public void testTimestamp() {
        checkGenerated(TIMESTAMP, TimestampValueFactory::new);
    }

    @Test
    public void testTimestampWithTimeZone() {
        checkGenerated(TIMESTAMP_WITH_TIME_ZONE, TimestampWithTimeZoneValueFactory::new);
    }

    @Test
    public void testNull() {
        checkGenerated(NULL, NullValueFactory::new);
    }

    @Test
    public void testObject() {
        checkGenerated(OBJECT, ObjectValueFactory::new);
    }

    private void checkGenerated(SqlColumnType type, Supplier<ValueFactory> factoryProducer) {
        List<List<Object>> columns = generate(factoryProducer);

        for (List<Object> column : columns) {
            check(type, column);
        }
    }

    private void check(SqlColumnType type, List<Object> values) {
        check(type, values, true);
        check(type, values, false);
    }

    private void check(SqlColumnType type, List<Object> values, boolean last) {
        SqlRowMetadata rowMetadata = new SqlRowMetadata(Collections.singletonList(new SqlColumnMetadata("a", type, true)));

        List<SqlRow> rows = new ArrayList<>();

        for (Object value : values) {
            if (SqlPage.convertToData(type) && value != null) {
                value = serializationService.toData(value);
            }

            rows.add(new SqlRowImpl(rowMetadata, new JetSqlRow(TEST_SS, new Object[]{value})));
        }

        SqlPage originalPage = SqlPage.fromRows(
            Collections.singletonList(type),
            rows,
            last,
            serializationService
        );

        ClientMessage message = ClientMessage.createForEncode();
        SqlPageCodec.encode(message, originalPage);
        SqlPage restoredPage = SqlPageCodec.decode(message.frameIterator());

        assertEquals(1, restoredPage.getColumnCount());
        assertEquals(values.size(), restoredPage.getRowCount());
        assertEquals(last, restoredPage.isLast());

        assertEquals(1, restoredPage.getColumnTypes().size());
        assertEquals(type, restoredPage.getColumnTypes().get(0));

        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            Object restoredValue = restoredPage.getColumnValueForClient(0, i);

            if (restoredValue instanceof Data) {
                assertTrue(SqlPage.convertToData(type));

                restoredValue = serializationService.toObject(restoredValue);
            }

            assertEquals(value, restoredValue);
        }
    }

    private static List<List<Object>> generate(Supplier<ValueFactory> factoryProducer) {
        List<List<Object>> res = new ArrayList<>();

        for (int i = 0; i <= 8; i++) {
            generate(res, factoryProducer, i, Collections.emptyList());
        }

        List<Object> prefix = res.get(res.size() - 1);

        for (int i = 0; i <= 8; i++) {
            generate(res, factoryProducer, i, prefix);
        }

        return res;
    }

    private static void generate(List<List<Object>> res, Supplier<ValueFactory> factoryProducer, int count, List<Object> prefix) {
        if (count == 0) {
            if (prefix.isEmpty()) {
                res.add(Collections.emptyList());
            }

            return;
        }

        int maxMask = 1 << count;

        for (int mask = 0; mask < maxMask; mask++) {
            ValueFactory factory = factoryProducer.get();

            List<Object> list = new ArrayList<>(count);
            list.addAll(prefix);

            for (int j = 0; j < count; j++) {
                boolean isNull = (mask & (1 << j)) == 0;

                list.add(isNull ? null : factory.next());
            }

            res.add(list);
        }
    }

    interface ValueFactory {
        Object next();
    }

    private static final class VarcharValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return Long.toString(current++);
        }
    }

    private static final class BooleanValueFactory implements ValueFactory {

        private boolean current;

        @Override
        public Object next() {
            boolean res = current;

            current = !current;

            return res;
        }
    }

    private static final class TinyintValueFactory implements ValueFactory {

        private byte current = 0;

        @Override
        public Object next() {
            return current++;
        }
    }

    private static final class SmallintValueFactory implements ValueFactory {

        private short current = 0;

        @Override
        public Object next() {
            return current++;
        }
    }

    private static final class IntegerValueFactory implements ValueFactory {

        private int current = 0;

        @Override
        public Object next() {
            return current++;
        }
    }

    private static final class BigintValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return current++;
        }
    }

    private static final class DecimalValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return BigDecimal.valueOf(current++);
        }
    }

    private static final class RealValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return (float) current++;
        }
    }

    private static final class DoubleValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return (double) current++;
        }
    }

    private static final class DateValueFactory implements ValueFactory {

        private final LocalDate now = LocalDate.now();
        private long current;

        @Override
        public Object next() {
            return now.plusDays(current++);
        }
    }

    private static final class TimeValueFactory implements ValueFactory {

        private final LocalTime now = LocalTime.now();
        private long current;

        @Override
        public Object next() {
            return now.plusSeconds(current++);
        }
    }

    private static final class TimestampValueFactory implements ValueFactory {

        private final LocalDateTime now = LocalDateTime.now();
        private long current;

        @Override
        public Object next() {
            return now.plusSeconds(current++);
        }
    }

    private static final class TimestampWithTimeZoneValueFactory implements ValueFactory {

        private final OffsetDateTime now = OffsetDateTime.now();
        private long current;

        @Override
        public Object next() {
            return now.plusSeconds(current++);
        }
    }

    private static final class NullValueFactory implements ValueFactory {
        @Override
        public Object next() {
            return null;
        }
    }

    private static final class ObjectValueFactory implements ValueFactory {

        private long current;

        @Override
        public Object next() {
            return new ObjectHolder(current++);
        }
    }

    private static final class ObjectHolder implements Serializable {

        private final long value;

        private ObjectHolder(long value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ObjectHolder that = (ObjectHolder) o;

            return value == that.value;
        }

        @Override
        public int hashCode() {
            return (int) (value ^ (value >>> 32));
        }
    }
}
