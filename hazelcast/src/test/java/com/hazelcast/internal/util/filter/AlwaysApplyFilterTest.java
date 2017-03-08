package com.hazelcast.internal.util.filter;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AlwaysApplyFilterTest {

    @Test
    public void whenPassedNull_thenMatches() {
        AlwaysApplyFilter<?> f = AlwaysApplyFilter.newInstance();
        assertTrue(f.accept(null));
    }

    @Test
    public void whenPassedObject_thenMatches() {
        AlwaysApplyFilter<Object> f = AlwaysApplyFilter.newInstance();
        assertTrue(f.accept(new Object()));
    }
}
