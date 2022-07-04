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

package com.hazelcast.sql;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlColumnTypeTest {
    @Test
    public void testColumnTypes() {
        checkType(SqlColumnType.BOOLEAN, Boolean.class);
        checkType(SqlColumnType.TINYINT, Byte.class);
        checkType(SqlColumnType.SMALLINT, Short.class);
        checkType(SqlColumnType.INTEGER, Integer.class);
        checkType(SqlColumnType.BIGINT, Long.class);
        checkType(SqlColumnType.REAL, Float.class);
        checkType(SqlColumnType.DOUBLE, Double.class);
        checkType(SqlColumnType.DECIMAL, BigDecimal.class);

        checkType(SqlColumnType.DATE, LocalDate.class);
        checkType(SqlColumnType.TIME, LocalTime.class);
        checkType(SqlColumnType.TIMESTAMP, LocalDateTime.class);
        checkType(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.class);

        checkType(SqlColumnType.OBJECT, Object.class);
    }

    private static void checkType(SqlColumnType type, Class<?> valueClass) {
        assertEquals(valueClass, type.getValueClass());
    }
}
