package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
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
public class PassThroughCacheMergePolicyTest {

    private static final String EXISTING = "EXISTING";
    private static final String MERGING = "MERGING";

    protected CacheMergePolicy policy;

    @Before
    public void setup() {
        policy = new PassThroughCacheMergePolicy();
    }

    @Test
    public void merge_mergingNotNull() {
        CacheEntryView existing = entryWithGivenValue(EXISTING);
        CacheEntryView merging = entryWithGivenValue(MERGING);

        assertEquals(MERGING, policy.merge("cache", merging, existing));
    }

    @Test
    public void merge_mergingNull() {
        CacheEntryView existing = entryWithGivenValue(EXISTING);
        CacheEntryView merging = null;

        assertEquals(EXISTING, policy.merge("cache", merging, existing));
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
