package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCachePreloaderConfigTest {

    private NearCachePreloaderConfig config = new NearCachePreloaderConfig();

    @Test
    public void setStoreInitialDelaySeconds() {
        config.setStoreInitialDelaySeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withZero() {
        config.setStoreInitialDelaySeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreInitialDelaySeconds_withNegative() {
        config.setStoreInitialDelaySeconds(-1);
    }

    @Test
    public void setStoreIntervalSeconds() {
        config.setStoreIntervalSeconds(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withZero() {
        config.setStoreIntervalSeconds(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStoreIntervalSeconds_withNegative() {
        config.setStoreIntervalSeconds(-1);
    }
}
