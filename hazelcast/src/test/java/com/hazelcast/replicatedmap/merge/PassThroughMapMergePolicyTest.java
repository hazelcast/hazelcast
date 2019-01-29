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

package com.hazelcast.replicatedmap.merge;

import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryView;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PassThroughMapMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected ReplicatedMapMergePolicy policy;

    @Before
    public void given() {
        policy = PassThroughMergePolicy.INSTANCE;
    }

    @Test
    public void merge_mergingNotNull() {
        ReplicatedMapEntryView existing = entryWithGivenValue(EXISTING);
        ReplicatedMapEntryView merging = entryWithGivenValue(MERGING);

        assertEquals(MERGING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_mergingNull() {
        ReplicatedMapEntryView existing = entryWithGivenValue(EXISTING);

        assertEquals(EXISTING, policy.merge("map", null, existing));
    }

    private ReplicatedMapEntryView entryWithGivenValue(String value) {
        ReplicatedMapEntryView entryView = mock(ReplicatedMapEntryView.class);
        try {
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
