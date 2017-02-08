package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationRefReadOnlyTest {

    private WanReplicationRef getReadOnlyRef() {
        return new WanReplicationRef().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOnReadOnlyWanReplicationRefShouldFail() {
        getReadOnlyRef().setName("myRef");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMergePolicyOnReadOnlyWanReplicationRefShouldFail()  {
        getReadOnlyRef().setMergePolicy("myMergePolicy");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setFiltersOnReadOnlyWanReplicationRefShouldFail()  {
        getReadOnlyRef().setFilters(Collections.<String>emptyList());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addFilterOnReadOnlyWanReplicationRefShouldFail()  {
        getReadOnlyRef().addFilter("myFilter");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setRepublishingEnabledOnReadOnlyWanReplicationRefShouldFail()  {
        getReadOnlyRef().setRepublishingEnabled(true);
    }
}
