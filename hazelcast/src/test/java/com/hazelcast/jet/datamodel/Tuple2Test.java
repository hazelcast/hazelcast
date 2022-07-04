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

package com.hazelcast.jet.datamodel;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Tuple2Test {
    private Tuple2<String, String> t;

    @Test
    public void when_useFactory_then_everythingThere() {
        // When
        t = tuple2("a", "b");

        // Then
        assertEquals("a", t.f0());
        assertEquals("b", t.f1());
    }

    @Test
    public void when_getKeyAndValue_then_sameAsF0AndF1() {
        // Given
        t = tuple2("a", "b");

        // When - Then
        assertEquals("a", t.getKey());
        assertEquals("b", t.getValue());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void when_setValue_then_exception() {
        // When - Then
        tuple2("a", "b").setValue("c");
    }

    @Test
    public void when_equalTuples_thenEqualsTrueAndHashCodesEqual() {
        // Given
        t = tuple2("a", "b");
        Tuple2<String, String> t_b = tuple2("a", "b");

        // When - Then
        assertTrue(t.equals(t_b));
        assertTrue(t.hashCode() == t_b.hashCode());
    }

    @Test
    public void when_unequalTuples_thenEqualsFalse() {
        // Given
        t = tuple2("a", "b");
        Tuple2<String, String> t_b = tuple2("a", "xb");

        // When - Then
        assertFalse(t.equals(t_b));
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(tuple2("a", "b").toString());
        assertNotNull(tuple2(null, null).toString());
    }

}
