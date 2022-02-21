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

package com.hazelcast.jet.core;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ResettableSingletonTraverserTest {
    private final ResettableSingletonTraverser<Integer> trav = new ResettableSingletonTraverser<>();

    @Test
    public void when_acceptNotCalled_thenNextReturnsNull() {
        // When
        Integer item = trav.next();

        // Then
        assertNull(item);
    }

    @Test
    public void when_acceptItem_thenNextReturnsIt() {
        // When
        trav.accept(1);

        // Then
        assertEquals(Integer.valueOf(1), trav.next());
    }

    @Test
    public void when_itemConsumed_thenNextReturnsNull() {
        // Given
        trav.accept(1);
        trav.next();

        // When
        Integer secondItem = trav.next();

        // Then
        assertNull(secondItem);
    }

    @Test(expected = AssertionError.class)
    public void when_itemNotConsumed_thenAcceptFails() {
        trav.accept(1);
        trav.accept(2);
    }
}
