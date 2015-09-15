package com.hazelcast.cache.merge;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.HigherHitsCacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.LatestAccessCacheMergePolicy;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractCacheMergePolicyTest {

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
