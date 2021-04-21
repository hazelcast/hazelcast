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

package com.hazelcast.jet.sql;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.config.IndexType.SORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlDeleteQueriesTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
    }

    @Test
    public void deleteBySingleKey() {
        put(1);
        checkUpdateCount("delete from test_map where __key = 1", 1);
        put(1);
        checkUpdateCount("delete from test_map where 1 = __key", 1);
        put(1);
        checkUpdateCount("delete from test_map where 2 = __key", 0);
    }

    @Test
    public void deleteWithDisjunctionPredicate() {
        put(1);
        put(2);
        checkUpdateCount("delete from test_map where __key = 1 or __key = 2", 2);
        put(1);
        put(2);
        put(3);
        put(4);
        put(5);
        checkUpdateCount("delete from test_map where __key = 1 or __key = 2 or __key = 3 or __key = 5", 4);
    }

    @Test
    public void deleteThatDoesNotCheckKeyForEquality_fails() {
        put(10);

        checkError("delete from test_map where __key > 1", "GREATER_THAN predicate is not supported for DELETE queries");
    }

    @Test
    public void deleteWithConjunctionPredicate_fails() {
        put(10);

        checkError("delete from test_map where __key = 1 and __key = 2", "AND predicate is not supported for DELETE queries");
    }

    private void checkError(String sql, String expectedErrorMessage) {
        try {
            instance().getSql().execute(sql);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertNotNull(e.getMessage());
            assertTrue(
                    "\nExpected: " + expectedErrorMessage + "\nActual: " + e.getMessage(),
                    e.getMessage().contains(expectedErrorMessage)
            );

            assertEquals(e.getCode() + ": " + e.getMessage(), SqlErrorCode.GENERIC, e.getCode());
        }
    }

    private void checkUpdateCount(String sql, int expected) {
        assertThat(instance().getSql().execute(sql).updateCount()).isEqualTo(expected);
    }

    private void put(Object key) {
        IMap<Object, Object> map = instance().getMap("test_map");

        IndexConfig indexConfig = new IndexConfig().setName("Index_" + randomName())
                .setType(SORTED);
        for (String fieldName : new String[]{"__key", "this"}) {
            indexConfig.addAttribute(fieldName);
        }
        map.addIndex(indexConfig);
        map.put(key, key);
    }
}
