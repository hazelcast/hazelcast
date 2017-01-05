package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorConfigReadOnlyTest {

    private ScheduledExecutorConfig getReadOnlyConfig() {
        return new ScheduledExecutorConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyScheduledExecutorConfigShouldFail() {
        getReadOnlyConfig().setName("myName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPoolSizeOnReadOnlyScheduledExecutorConfigShouldFail() {
        getReadOnlyConfig().setPoolSize(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setDurabilityOnReadOnlyScheduledExecutorConfigShouldFail() {
        getReadOnlyConfig().setDurability(42);
    }
}
