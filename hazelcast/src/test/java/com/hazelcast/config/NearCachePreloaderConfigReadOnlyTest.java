package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCachePreloaderConfigReadOnlyTest {

    private NearCachePreloaderConfig getReadOnlyConfig() {
        return new NearCachePreloaderConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEnabledOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setFilenameOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setDirectory("myFileName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStoreInitialDelaySecondsOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setStoreInitialDelaySeconds(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStoreIntervalSecondsOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setStoreIntervalSeconds(5);
    }
}
