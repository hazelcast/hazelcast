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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Helper test classes.
 */
public class SqlTestSupport extends HazelcastTestSupport {
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

    public static <T> T serialize(Object original) {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        return ss.toObject(ss.toData(original));
    }

    public static <T> T serializeAndCheck(IdentifiedDataSerializable original, int expectedClassId) {
        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(expectedClassId, original.getClassId());

        return serialize(original);
    }

    public static ListRowBatch createMonotonicBatch(int startValue, int size) {
        List<Row> rows = new ArrayList<>(size);

        for (int i = startValue; i < startValue + size; i++) {
            rows.add(HeapRow.of(i));
        }

        return new ListRowBatch(rows);
    }

    public static void checkMonotonicBatch(RowBatch batch, int start, int size) {
        assertEquals(size, batch.getRowCount());

        for (int i = 0; i < size; i++) {
            int value = batch.getRow(i).get(0);

            assertEquals(start + i, value);
        }
    }
}
