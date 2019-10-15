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

package com.hazelcast.spi.merge;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.internal.serialization.SerializationService;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSplitBrainMergePolicyTest {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();
    private static final Data EXISTING = SERIALIZATION_SERVICE.toData("EXISTING");
    private static final Data MERGING = SERIALIZATION_SERVICE.toData("MERGING");

    protected SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy;

    @Before
    public void setup() {
        mergePolicy = createMergePolicy();
    }

    protected abstract SplitBrainMergePolicy<Data, MapMergeTypes> createMergePolicy();

    @Test
    public void merge_mergingWins() {
        MapMergeTypes existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MapMergeTypes merging = mergingValueWithGivenPropertyAndValue(333, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_existingWins() {
        MapMergeTypes existing = mergingValueWithGivenPropertyAndValue(333, EXISTING);
        MapMergeTypes merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    @Test
    public void merge_draw_mergingWins() {
        MapMergeTypes existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MapMergeTypes merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_mergingWins_sinceExistingIsNotExist() {
        MapMergeTypes existing = null;
        MapMergeTypes merging = mergingValueWithGivenPropertyAndValue(1, MERGING);

        assertEquals(MERGING, mergePolicy.merge(merging, existing));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void merge_existingWins_sinceMergingIsNotExist() {
        MapMergeTypes existing = mergingValueWithGivenPropertyAndValue(1, EXISTING);
        MapMergeTypes merging = null;

        assertEquals(EXISTING, mergePolicy.merge(merging, existing));
    }

    private MapMergeTypes mergingValueWithGivenPropertyAndValue(long testedProperty, Data value) {
        MapMergeTypes mergingEntry = mock(MapMergeTypes.class);
        try {
            if (mergePolicy instanceof ExpirationTimeMergePolicy) {
                when(mergingEntry.getExpirationTime()).thenReturn(testedProperty);
            } else if (mergePolicy instanceof HigherHitsMergePolicy) {
                when(mergingEntry.getHits()).thenReturn(testedProperty);
            } else if (mergePolicy instanceof LatestAccessMergePolicy) {
                when(mergingEntry.getLastAccessTime()).thenReturn(testedProperty);
            } else if (mergePolicy instanceof LatestUpdateMergePolicy) {
                when(mergingEntry.getLastUpdateTime()).thenReturn(testedProperty);
            } else {
                fail("Unsupported MergePolicy type");
            }
            when(mergingEntry.getValue()).thenReturn(value);
            when(mergingEntry.getDeserializedValue()).thenReturn(SERIALIZATION_SERVICE.toObject(value));
            return mergingEntry;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
