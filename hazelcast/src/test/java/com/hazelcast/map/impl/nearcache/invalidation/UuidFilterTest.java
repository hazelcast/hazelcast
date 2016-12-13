package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class UuidFilterTest {

    private UuidFilter uuidFilter = new UuidFilter("sourceUuid");

    @Test
    public void testEval() {
        assertTrue(uuidFilter.eval("sourceUuid"));
    }

    @Test
    public void testEval_withNonMatchingParameter() {
        assertFalse(uuidFilter.eval("otherUuid"));
    }

    @RequireAssertEnabled
    @Test(expected = AssertionError.class)
    public void testEval_withInvalidParameter() {
        uuidFilter.eval(5);
    }

    @Test
    public void testToString() {
        assertNotNull(uuidFilter.toString());
    }
}
