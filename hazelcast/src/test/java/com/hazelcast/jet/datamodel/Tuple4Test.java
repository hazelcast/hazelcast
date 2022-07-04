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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.jet.datamodel.Tuple4.tuple4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Tuple4Test {
    private Tuple4<String, String, String, String> t;

    @Before
    public void before() {
        t = tuple4("a", "b", "c", "d");
    }

    @Test
    public void when_useFactory_then_everythingThere() {
        assertEquals("a", t.f0());
        assertEquals("b", t.f1());
        assertEquals("c", t.f2());
        assertEquals("d", t.f3());
    }

    @Test
    public void when_equalTuples_thenEqualsTrueAndHashCodesEqual() {
        // Given
        Tuple4<String, String, String, String> t_b = tuple4("a", "b", "c", "d");

        // When - Then
        assertTrue(t.equals(t_b));
        assertTrue(t.hashCode() == t_b.hashCode());
    }

    @Test
    public void when_unequalTuples_thenEqualsFalse() {
        // Given
        Tuple4<String, String, String, String> t_b = tuple4("a", "b", "c", "xd");

        // When - Then
        assertFalse(t.equals(t_b));
    }

    @Test
    public void when_toString_then_noFailures() {
        assertNotNull(t.toString());
        assertNotNull(tuple4(null, null, null, null).toString());
    }
}
