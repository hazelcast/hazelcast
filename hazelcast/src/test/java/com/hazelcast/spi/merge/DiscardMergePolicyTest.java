/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.merge;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DiscardMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected SplitBrainMergePolicy mergePolicy;

    @Before
    public void setup() {
        mergePolicy = new DiscardMergePolicy();
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingValueAbsent() {
        MergingValue existing = null;
        MergingValue merging = mergingValueWithGivenValue(MERGING);

        assertNull(mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_existingValuePresent() {
        MergingValue existing = mergingValueWithGivenValue(EXISTING);
        MergingValue merging = mergingValueWithGivenValue(MERGING);

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingNull() {
        MergingValue existing = mergingValueWithGivenValue(EXISTING);
        MergingValue merging = null;

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_bothValuesNull() {
        MergingValue existing = mergingValueWithGivenValue(null);
        MergingValue merging = mergingValueWithGivenValue(null);

        assertNull(mergePolicy.merge(merging, existing));
    }

    private MergingValue mergingValueWithGivenValue(String value) {
        MergingValue mergingValue = mock(MergingValue.class);
        try {
            when(mergingValue.getValue()).thenReturn(value);
            return mergingValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
