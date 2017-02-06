/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;

import static com.hazelcast.jet.impl.util.DoneItem.DONE_ITEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollectionWithDoneDetectorTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private CollectionWithDoneDetector coll = new CollectionWithDoneDetector();

    @Before
    public void setUp() {
        coll.wrapped = new ArrayList<>();
    }

    @Test
    public void when_addNormalItem_then_acceptIt() {
        assertFalse(coll.done);

        assertTrue(coll.add("a"));
        assertFalse(coll.done);
        assertEquals(coll.size(), 1);

        assertTrue(coll.add("b"));
        assertFalse(coll.done);
        assertEquals(coll.size(), 2);
    }

    @Test
    public void when_addDoneItem_then_refuseItAndBecomeDone() {
        assertFalse(coll.done);

        assertTrue(coll.add("a"));
        assertFalse(coll.done);
        assertEquals(coll.size(), 1);

        assertFalse(coll.add(DONE_ITEM));
        assertTrue(coll.done);
        assertEquals(coll.size(), 1);
    }

    @Test
    public void when_addAfterDone_then_fail() {
        assertFalse(coll.done);

        assertTrue(coll.add("a"));
        assertFalse(coll.done);
        assertEquals(coll.size(), 1);

        assertFalse(coll.add(DONE_ITEM));
        assertTrue(coll.done);
        assertEquals(coll.size(), 1);

        exception.expect(AssertionError.class);
        coll.add("c");
    }

    @Test
    public void when_addMultipleDoneItems_then_fail() {
        assertFalse(coll.done);

        assertTrue(coll.add("a"));
        assertFalse(coll.done);
        assertEquals(coll.size(), 1);

        assertFalse(coll.add(DONE_ITEM));
        assertTrue(coll.done);
        assertEquals(coll.size(), 1);

        exception.expect(AssertionError.class);
        coll.add(DONE_ITEM);
    }

}