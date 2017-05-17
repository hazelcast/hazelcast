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

package com.hazelcast.jet.impl.util;

import org.junit.Test;

import java.util.stream.IntStream;

import static com.hazelcast.jet.Traverser.over;
import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.Traversers.traverseStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FlatMappingTraverserTest {

    @Test
    public void smokeTest() {
        FlatMappingTraverser<Integer, Integer> trav = new FlatMappingTraverser<>(
                over(0, 1, 2, 3),
                numItems -> traverseStream(IntStream.range(0, numItems).boxed()));

        assertEquals(0, (int) trav.next());
        assertEquals(0, (int) trav.next());
        assertEquals(1, (int) trav.next());
        assertEquals(0, (int) trav.next());
        assertEquals(1, (int) trav.next());
        assertEquals(2, (int) trav.next());
        assertNull(trav.next());
        assertNull(trav.next());
    }

    @Test
    public void when_flatMapToNullTraverser_then_skipOverToNext() {
        // This test would fail, if the internal FlatMappingTraverser.NULL_TRAVERSER instance
        // would be the same (as per == operator) as the instance returned by Traversers.empty()
        FlatMappingTraverser<Integer, String> trav =
                new FlatMappingTraverser<>(over(1, 2, 3), item -> item != 3 ? empty() : over("a"));

        assertEquals("a", trav.next());
        assertNull(trav.next());
        assertNull(trav.next());
    }
}
