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

package com.hazelcast.internal.eviction;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeEvictionCheckerTest {

    @Test(expected = IllegalArgumentException.class)
    public void compositionOperatorCannotBeNull() {
        CompositeEvictionChecker.newCompositeEvictionChecker(
                null,
                mock(EvictionChecker.class),
                mock(EvictionChecker.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictionCheckersCannotBeNull() {
        CompositeEvictionChecker.newCompositeEvictionChecker(
                CompositeEvictionChecker.CompositionOperator.AND,
                null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void evictionCheckersCannotBeEmpty() {
        CompositeEvictionChecker.newCompositeEvictionChecker(
                CompositeEvictionChecker.CompositionOperator.AND);
    }

    @Test
    public void resultShouldReturnTrue_whenAllIsTrue_withAndCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsTrue = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsTrue = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsTrue.isEvictionRequired()).thenReturn(true);
        when(evictionChecker2ReturnsTrue.isEvictionRequired()).thenReturn(true);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.AND, evictionChecker1ReturnsTrue,
                        evictionChecker2ReturnsTrue);

        assertTrue(compositeEvictionChecker.isEvictionRequired());
    }

    @Test
    public void resultShouldReturnFalse_whenAllIsFalse_withAndCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsFalse = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsFalse = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsFalse.isEvictionRequired()).thenReturn(false);
        when(evictionChecker2ReturnsFalse.isEvictionRequired()).thenReturn(false);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.AND, evictionChecker1ReturnsFalse,
                        evictionChecker2ReturnsFalse);

        assertFalse(compositeEvictionChecker.isEvictionRequired());
    }

    @Test
    public void resultShouldReturnFalse_whenOneIsFalse_withAndCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsTrue = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsFalse = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsTrue.isEvictionRequired()).thenReturn(true);
        when(evictionChecker2ReturnsFalse.isEvictionRequired()).thenReturn(false);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.AND, evictionChecker1ReturnsTrue,
                        evictionChecker2ReturnsFalse);

        assertFalse(compositeEvictionChecker.isEvictionRequired());
    }

    @Test
    public void resultShouldReturnTrue_whenAllIsTrue_withOrCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsTrue = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsTrue = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsTrue.isEvictionRequired()).thenReturn(true);
        when(evictionChecker2ReturnsTrue.isEvictionRequired()).thenReturn(true);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.OR, evictionChecker1ReturnsTrue,
                        evictionChecker2ReturnsTrue);

        assertTrue(compositeEvictionChecker.isEvictionRequired());
    }

    @Test
    public void resultShouldReturnFalse_whenAllIsFalse_withOrCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsFalse = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsFalse = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsFalse.isEvictionRequired()).thenReturn(false);
        when(evictionChecker2ReturnsFalse.isEvictionRequired()).thenReturn(false);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.OR, evictionChecker1ReturnsFalse,
                        evictionChecker2ReturnsFalse);

        assertFalse(compositeEvictionChecker.isEvictionRequired());
    }

    @Test
    public void resultShouldReturnTrue_whenOneIsTrue_withOrCompositionOperator() {
        EvictionChecker evictionChecker1ReturnsTrue = mock(EvictionChecker.class);
        EvictionChecker evictionChecker2ReturnsFalse = mock(EvictionChecker.class);

        when(evictionChecker1ReturnsTrue.isEvictionRequired()).thenReturn(true);
        when(evictionChecker2ReturnsFalse.isEvictionRequired()).thenReturn(false);

        CompositeEvictionChecker compositeEvictionChecker =
                CompositeEvictionChecker.newCompositeEvictionChecker(
                        CompositeEvictionChecker.CompositionOperator.OR, evictionChecker1ReturnsTrue,
                        evictionChecker2ReturnsFalse);

        assertTrue(compositeEvictionChecker.isEvictionRequired());
    }
}
