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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SqlPredicateSkipIndexTest
        extends HazelcastTestSupport {

    @Test
    public void testInPredicate() {
        SqlPredicate sqlPredicate = (SqlPredicate) Predicates.sql("%age in (1)");
        Predicate p = sqlPredicate.getPredicate();

        SkipIndexPredicate skipIndexPredicate = (SkipIndexPredicate) p;

        InPredicate inPredicate = (InPredicate) skipIndexPredicate.getTarget();
        assertEquals("age", inPredicate.attributeName, "age");
        assertArrayEquals(new String[]{"1"}, inPredicate.values);
    }

    @Test
    public void testEqualPredicate() {
        SqlPredicate sqlPredicate = (SqlPredicate) Predicates.sql("%age=40");
        Predicate p = sqlPredicate.getPredicate();

        SkipIndexPredicate skipIndexPredicate = (SkipIndexPredicate) p;

        EqualPredicate equalPredicate = (EqualPredicate) skipIndexPredicate.getTarget();
        assertEquals("age", equalPredicate.attributeName, "age");
        assertEquals("40", equalPredicate.value);
    }

    @Test
    public void testNotEqualNotPredicate() {
        notEqualPredicate("<>");
        notEqualPredicate("!=");
    }

    public void notEqualPredicate(String operator) {
        SqlPredicate sqlPredicate = (SqlPredicate) Predicates.sql("%age" + operator + "40");
        Predicate p = sqlPredicate.getPredicate();

        SkipIndexPredicate skipIndexPredicate = (SkipIndexPredicate) p;

        NotEqualPredicate equalPredicate = (NotEqualPredicate) skipIndexPredicate.getTarget();
        assertEquals("age", equalPredicate.attributeName, "age");
        assertEquals("40", equalPredicate.value);
    }

    @Test
    public void testGreaterLess() {
        greaterLess(">");
        greaterLess(">=");
        greaterLess("<");
        greaterLess("<=");
    }

    public void greaterLess(String operator) {
        SqlPredicate sqlPredicate = (SqlPredicate) Predicates.sql("%age" + operator + "40");
        Predicate p = sqlPredicate.getPredicate();

        SkipIndexPredicate skipIndexPredicate = (SkipIndexPredicate) p;

        GreaterLessPredicate equalPredicate = (GreaterLessPredicate) skipIndexPredicate.getTarget();
        assertEquals("age", equalPredicate.attributeName, "age");
        assertEquals("40", equalPredicate.value);
    }
}
