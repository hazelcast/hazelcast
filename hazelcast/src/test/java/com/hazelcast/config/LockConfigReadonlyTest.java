package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LockConfigReadonlyTest {

    private LockConfig getReadOnlyConfig() {
        return new LockConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyLockConfigShouldFail() {
        getReadOnlyConfig().setName("anyName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setQuorumNameOnReadOnlyLockConfigShouldFail() {
        getReadOnlyConfig().setQuorumName("anyName");
    }
}
