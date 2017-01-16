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

package com.hazelcast.jet;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Vector;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Stream.of;
import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(QuickTest.class)
public class TraversersTest {

    @Test
    public void iterate() {
        validateTraversal(Traversers.iterate(Stream.of(1, 2).iterator()));
    }

    @Test
    public void spliterate() {
        validateTraversal(Traversers.spliterate(Stream.of(1, 2).spliterator()));
    }

    @Test
    public void enumerate() {
        validateTraversal(Traversers.enumerate(new Vector<>(asList(1, 2)).elements()));
    }

    @Test
    public void traverseStream() {
        validateTraversal(Traversers.traverseStream(of(1, 2)));
    }

    @Test
    public void traverseIterable() {
        validateTraversal(Traversers.traverseIterable(asList(1, 2)));
    }

    @Test
    public void traverseArray() {
        validateTraversal(Traversers.traverseArray(new Object[] {1, 2}));
    }

    @Test
    public void when_lazyLeftAlone_then_supplierNotCalled() {
        Traversers.lazy(() -> {
            fail();
            return null;
        });
    }

    @Test
    public void lazyTraversalTransparent() {
        validateTraversal(Traversers.lazy(() -> Traversers.traverseStream(of(1, 2))));
    }

    private static void validateTraversal(Traverser<?> trav) {
        // When
        final Object t1 = trav.next();
        final Object t2 = trav.next();
        final Object t3 = trav.next();

        // Then
        assertEquals(1, t1);
        assertEquals(2, t2);
        assertNull(t3);
    }
}
