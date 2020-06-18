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

package com.hazelcast.sql.impl.type;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.values;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryDataTypeFamilyTest {
    @Test
    public void testTemporal() {
        for (QueryDataTypeFamily typeFamily : values()) {
            switch (typeFamily) {
                case TIME:
                case DATE:
                case TIMESTAMP:
                case TIMESTAMP_WITH_TIME_ZONE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_SECOND:
                    assertTrue(typeFamily.isTemporal());

                    break;

                default:
                    assertFalse(typeFamily.isTemporal());
            }
        }
    }

    @Test
    public void testEstimatedSize() {
        assertTrue(BOOLEAN.getEstimatedSize() <= TINYINT.getEstimatedSize());
        assertTrue(TINYINT.getEstimatedSize() < SMALLINT.getEstimatedSize());
        assertTrue(SMALLINT.getEstimatedSize() < INT.getEstimatedSize());
        assertTrue(INT.getEstimatedSize() < BIGINT.getEstimatedSize());

        assertTrue(REAL.getEstimatedSize() < DOUBLE.getEstimatedSize());

        assertTrue(NULL.getEstimatedSize() > 0);
    }

}
