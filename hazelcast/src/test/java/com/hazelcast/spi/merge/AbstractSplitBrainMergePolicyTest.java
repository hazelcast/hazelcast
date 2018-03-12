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

import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.merge.FullMergingEntryImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSplitBrainMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected SplitBrainMergePolicy mergePolicy;

    @Before
    public void setup() {
        mergePolicy = createMergePolicy();
    }

    protected abstract SplitBrainMergePolicy createMergePolicy();

    @Test
    public void merge_mergingWins() {
        MergingValue existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MergingValue merging = mergingValueWithGivenPropertyAndValue(333, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_existingWins() {
        MergingValue existing = mergingValueWithGivenPropertyAndValue(333, EXISTING);
        MergingValue merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_draw_mergingWins() {
        MergingValue existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MergingValue merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingWins_sinceExistingIsNotExist() {
        MergingValue existing = null;
        MergingValue merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingWins_sinceMergingIsNotExist() {
        MergingValue existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MergingValue merging = null;

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    private MergingValue mergingValueWithGivenPropertyAndValue(long testedProperty, String value) {
        FullMergingEntryImpl mergingEntry = mock(FullMergingEntryImpl.class);
        try {
            if (mergePolicy instanceof HigherHitsMergePolicy) {
                when(mergingEntry.getHits()).thenReturn(testedProperty);
            } else if (mergePolicy instanceof LatestAccessMergePolicy) {
                when(mergingEntry.getLastAccessTime()).thenReturn(testedProperty);
            } else if (mergePolicy instanceof LatestUpdateMergePolicy) {
                when(mergingEntry.getLastUpdateTime()).thenReturn(testedProperty);
            } else {
                fail("Unsupported MergePolicy type");
            }
            when(mergingEntry.getValue()).thenReturn(value);
            return mergingEntry;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
