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

package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractCacheMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected CacheMergePolicy policy;

    @Before
    public void setup() {
        policy = createCacheMergePolicy();
    }

    protected abstract CacheMergePolicy createCacheMergePolicy();

    @Test
    public void merge_mergingWins() {
        CacheEntryView existing = entryWithGivenPropertyAndValue(1, EXISTING);
        CacheEntryView merging = entryWithGivenPropertyAndValue(333, MERGING);

        assertEquals(MERGING, policy.merge("cache", merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingWins_sinceExistingIsNotExist() {
        CacheEntryView existing = null;
        CacheEntryView merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, policy.merge("cache", merging, existing));
    }

    @Test
    public void merge_existingWins() {
        CacheEntryView existing = entryWithGivenPropertyAndValue(333, EXISTING);
        CacheEntryView merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(EXISTING, policy.merge("cache", merging, existing));
    }

    @Test
    public void merge_draw_mergingWins() {
        CacheEntryView existing = entryWithGivenPropertyAndValue(1, EXISTING);
        CacheEntryView merging = entryWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, policy.merge("cache", merging, existing));
    }

    private CacheEntryView entryWithGivenPropertyAndValue(long testedProperty, String value) {
        CacheEntryView entryView = mock(CacheEntryView.class);
        try {
            if (policy instanceof HigherHitsCacheMergePolicy) {
                when(entryView.getAccessHit()).thenReturn(testedProperty);
            } else if (policy instanceof LatestAccessCacheMergePolicy) {
                when(entryView.getLastAccessTime()).thenReturn(testedProperty);
            }
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
