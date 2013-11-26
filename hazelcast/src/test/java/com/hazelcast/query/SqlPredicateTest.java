/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlPredicateTest
{
    @Test
    public void testEqualsWhenSqlMatches() throws Exception {
        SqlPredicate sql1 = new SqlPredicate("foo='bar'");
        SqlPredicate sql2 = new SqlPredicate("foo='bar'");
        assertEquals(sql1, sql2);
    }

    @Test
    public void testEqualsWhenSqlDifferent() throws Exception {
        SqlPredicate sql1 = new SqlPredicate("foo='bar'");
        SqlPredicate sql2 = new SqlPredicate("foo='baz'");
        assertNotEquals(sql1, sql2);
    }

    @Test
    public void testEqualsNull() throws Exception {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertNotEquals(sql, null);
    }

    @Test
    public void testEqualsSameObject() throws Exception {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertEquals(sql, sql);
    }

    @Test
    public void testHashCode() throws Exception {
        SqlPredicate sql = new SqlPredicate("foo='bar'");
        assertEquals("foo='bar'".hashCode(), sql.hashCode());
    }
}
