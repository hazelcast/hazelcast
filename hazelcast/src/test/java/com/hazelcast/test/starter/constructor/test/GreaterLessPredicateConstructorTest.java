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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.GreaterLessPredicateConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GreaterLessPredicateConstructorTest {

    // Tests that the copied object is not null due to lack of equals implementation in Predicate implementations
    // see also https://github.com/hazelcast/hazelcast/issues/13245
    @Test
    public void testConstructor() {
        GreaterLessPredicateConstructor constructor = new GreaterLessPredicateConstructor(GreaterLessPredicate.class);

        GreaterLessPredicate original = new GreaterLessPredicate("a", 3, true, true);
        GreaterLessPredicate copied = (GreaterLessPredicate) constructor.createNew(original);
        assertNotNull(copied);

        original = new GreaterLessPredicate("b", 5, false, true);
        copied = (GreaterLessPredicate) constructor.createNew(original);
        assertNotNull(copied);

        original = new GreaterLessPredicate("c", 6, true, false);
        copied = (GreaterLessPredicate) constructor.createNew(original);
        assertNotNull(copied);

        original = new GreaterLessPredicate("d", 7, false, false);
        copied = (GreaterLessPredicate) constructor.createNew(original);
        assertNotNull(copied);
    }
}
