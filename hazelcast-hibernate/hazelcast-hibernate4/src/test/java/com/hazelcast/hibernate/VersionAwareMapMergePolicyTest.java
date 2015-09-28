package com.hazelcast.hibernate;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hibernate.cache.spi.entry.CacheEntry;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionAwareMapMergePolicyTest {


    private static final MockVersion versionOld = new MockVersion(0);
    private static final MockVersion versionNew = new MockVersion(1);

    protected MapMergePolicy policy;

    @Before
    public void given() {
        policy = new VersionAwareMapMergePolicy();
    }

    @Test
    public void merge_mergingUptodate() {
        CacheEntry existing = cacheEntryWithVersion(versionOld);
        CacheEntry merging = cacheEntryWithVersion(versionNew);

        EntryView entryExisting = entryWithGivenValue(existing);
        EntryView entryMerging = entryWithGivenValue(merging);

        assertEquals(merging, policy.merge("map", entryMerging, entryExisting));
    }

    @Test
    public void merge_mergingStale() {
        CacheEntry existing = cacheEntryWithVersion(versionNew);
        CacheEntry merging = cacheEntryWithVersion(versionOld);

        EntryView entryExisting = entryWithGivenValue(existing);
        EntryView entryMerging = entryWithGivenValue(merging);

        assertEquals(existing, policy.merge("map", entryMerging, entryExisting));
    }

    @Test
    public void merge_mergingNull() {
        CacheEntry existing = null;
        CacheEntry merging = cacheEntryWithVersion(versionNew);

        EntryView entryExisting = entryWithGivenValue(existing);
        EntryView entryMerging = entryWithGivenValue(merging);

        assertEquals(merging, policy.merge("map", entryMerging, entryExisting));
    }


    private CacheEntry cacheEntryWithVersion(MockVersion mockVersion) {
        CacheEntry cacheEntry = mock(CacheEntry.class);
        when(cacheEntry.getVersion()).thenReturn(mockVersion);
        return cacheEntry;
    }

    private EntryView entryWithGivenValue(Object value) {
        EntryView entryView = mock(EntryView.class);
        try {
            when(entryView.getValue()).thenReturn(value);
            return entryView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    protected static class MockVersion implements Comparable<MockVersion> {

        private int version;

        public MockVersion(int version) {
            this.version = version;
        }

        @Override
        public int compareTo(MockVersion o) {
            return version - o.version;
        }
    }
}
