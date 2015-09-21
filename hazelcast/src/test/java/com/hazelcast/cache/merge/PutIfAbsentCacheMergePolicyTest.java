package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.PutIfAbsentCacheMergePolicy;
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
public class PutIfAbsentCacheMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected CacheMergePolicy policy;

    @Before
    public void setup() {
        policy = new PutIfAbsentCacheMergePolicy();
    }

    @Test
    public void merge_existingValueAbsent() {
        CacheEntryView existing = null;
        CacheEntryView merging = entryWithGivenValue(MERGING);

        assertEquals(MERGING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_existingValuePresent() {
        CacheEntryView existing = entryWithGivenValue(EXISTING);
        CacheEntryView merging = entryWithGivenValue(MERGING);

        assertEquals(EXISTING, policy.merge("map", merging, existing));
    }

    @Test
    public void merge_bothValuesNull() {
        CacheEntryView existing = entryWithGivenValue(null);
        CacheEntryView merging = entryWithGivenValue(null);

        assertNull(policy.merge("map", merging, existing));
    }

    private CacheEntryView entryWithGivenValue(String value) {
        CacheEntryView entryView = mock(CacheEntryView.class);
        try {
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
