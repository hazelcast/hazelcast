/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.merge;

import com.hazelcast.core.EntryView;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractMapMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected MapMergePolicy policy;

    @Test
    public void merge_mergingWins() {
        EntryView existing = entryWithGivenPropertyAndValue(1, EXISTING);
        EntryView merging = entryWithGivenPropertyAndValue(333, MERGING);

        assertEquals(MERGING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_existingWins() {
        EntryView existing = entryWithGivenPropertyAndValue(333, EXISTING);
        EntryView merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(EXISTING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_draw_mergingWins() {
        EntryView existing = entryWithGivenPropertyAndValue(1, EXISTING);
        EntryView merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, policy.merge("map", merging, existing));
    }

    private EntryView entryWithGivenPropertyAndValue(long testedProperty, String value) {
        EntryView entryView = mock(EntryView.class);
        try {
            if (policy instanceof HigherHitsMapMergePolicy) {
                when(entryView.getHits()).thenReturn(testedProperty);
            } else if (policy instanceof LatestUpdateMapMergePolicy) {
                when(entryView.getLastUpdateTime()).thenReturn(testedProperty);
            }
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
