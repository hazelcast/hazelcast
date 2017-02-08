package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CardinalityEstimatorConfigReadOnlyTest {

    private CardinalityEstimatorConfig getReadOnlyConfig() {
        return new CardinalityEstimatorConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyCardinalityEstimatorConfigShouldFail() {
        getReadOnlyConfig().setName("myName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBackupCountOnReadOnlyCardinalityEstimatorConfigShouldFail() {
        getReadOnlyConfig().setBackupCount(23);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAsyncBackupCountOnReadOnlyCardinalityEstimatorConfigShouldFail() {
        getReadOnlyConfig().setAsyncBackupCount(42);
    }
}
