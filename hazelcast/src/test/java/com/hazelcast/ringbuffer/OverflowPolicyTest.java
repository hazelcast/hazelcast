package com.hazelcast.ringbuffer;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.ringbuffer.OverflowPolicy.FAIL;
import static com.hazelcast.ringbuffer.OverflowPolicy.OVERWRITE;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OverflowPolicyTest {

    @Test
    public void test(){
        assertSame(FAIL, OverflowPolicy.getById(FAIL.getId()));
        assertSame(OVERWRITE, OverflowPolicy.getById(OVERWRITE.getId()));
        assertNull(OverflowPolicy.getById(-1));
    }
}
