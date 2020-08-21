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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class SqlExpressionIntegrationTestSupport extends SqlTestSupport {

    protected static final Object SKIP_VALUE_CHECK = new Object();
    protected HazelcastInstance member;

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private IMap map;

    @Before
    public void before() {
        member = factory.newHazelcastInstance();

        map = member.getMap("map");
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    protected void put(Object value) {
        put(0, value);
    }

    protected void put(int key, Object value) {
        map.clear();
        map.put(key, value);

        clearPlanCache(member);
    }

    protected void putAll(Object... values) {
        if (values == null || values.length == 0) {
            return;
        }

        Map<Integer, Object> entries = new HashMap<>();

        int key = 0;

        for (Object value : values) {
            entries.put(key++, value);
        }

        putAll(entries);
    }

    protected void putAll(Map<Integer, Object> entries) {
        map.clear();
        map.putAll(entries);

        clearPlanCache(member);
    }

    protected Object checkValueInternal(
        String sql,
        SqlColumnType expectedType,
        Object expectedValue,
        Object... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(expectedType, row.getMetadata().getColumn(0).getType());

        Object value = row.getObject(0);

        if (expectedValue != SKIP_VALUE_CHECK) {
            assertEquals(expectedValue, value);
        }

        return value;
    }

    protected void checkFailureInternal(
        String sql,
        int expectedErrorCode,
        String expectedErrorMessage,
        Object... params
    ) {
        try {
            execute(member, sql, params);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertTrue(expectedErrorMessage.length() != 0);
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage(),  e.getMessage().contains(expectedErrorMessage));

            assertEquals(e.getCode() + ": " + e.getMessage(), expectedErrorCode, e.getCode());
        }
    }
}
