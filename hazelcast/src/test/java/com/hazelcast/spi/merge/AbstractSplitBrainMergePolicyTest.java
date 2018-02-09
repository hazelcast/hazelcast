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
import com.hazelcast.spi.serialization.SerializationService;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSplitBrainMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected SerializationService serializationService;
    protected SplitBrainMergePolicy policy;

    @Before
    public void setup() {
        policy = createMergePolicy();
    }

    protected abstract SplitBrainMergePolicy createMergePolicy();

    @Test
    public void merge_mergingWins() {
        MergeDataHolder existing = entryWithGivenPropertyAndValue(1, EXISTING);
        MergeDataHolder merging = entryWithGivenPropertyAndValue(333, MERGING);

        assertEquals(MERGING, policy.merge(merging, existing));
    }

    @Test
    public void merge_existingWins() {
        MergeDataHolder existing = entryWithGivenPropertyAndValue(333, EXISTING);
        MergeDataHolder merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(EXISTING, policy.merge(merging, existing));
    }

    @Test
    public void merge_draw_mergingWins() {
        MergeDataHolder existing = entryWithGivenPropertyAndValue(1, EXISTING);
        MergeDataHolder merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, policy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingWins_sinceExistingIsNotExist() {
        MergeDataHolder existing = null;
        MergeDataHolder merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, policy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingWins_sinceMergingIsNotExist() {
        MergeDataHolder existing = entryWithGivenPropertyAndValue(1, EXISTING);
        MergeDataHolder merging = null;

        assertEquals(EXISTING, policy.merge(merging, existing));
    }

    private MergeDataHolder entryWithGivenPropertyAndValue(long testedProperty, String value) {
        SimpleMergeDataHolder entryView = mock(SimpleMergeDataHolder.class);
        try {
            if (policy instanceof HigherHitsMergePolicy) {
                when(entryView.getHits()).thenReturn(testedProperty);
            } else if (policy instanceof LatestAccessMergePolicy) {
                when(entryView.getLastAccessTime()).thenReturn(testedProperty);
            } else if (policy instanceof LatestUpdateMergePolicy) {
                when(entryView.getLastUpdateTime()).thenReturn(testedProperty);
            }
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
