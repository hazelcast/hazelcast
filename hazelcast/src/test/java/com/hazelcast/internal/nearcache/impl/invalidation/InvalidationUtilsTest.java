package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InvalidationUtilsTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(InvalidationUtils.class);
    }
}
