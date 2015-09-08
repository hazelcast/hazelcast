package com.hazelcast.map.merge;

import com.hazelcast.core.EntryView;
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

    protected MapMergePolicy policy;

    @Before
    public void given() {
        policy = new PassThroughMergePolicy();
    }

    @Test
    public void merge_mergingNotNull() {
        EntryView existing = entryWithGivenValue(EXISTING);
        EntryView merging = entryWithGivenValue(MERGING);

        assertEquals(MERGING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_mergingNull() {
        EntryView existing = entryWithGivenValue(EXISTING);
        EntryView merging = null;

        assertEquals(EXISTING, policy.merge("map", merging, existing));
    }

    private EntryView entryWithGivenValue(String value) {
        EntryView entryView = mock(EntryView.class);
        try {
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
