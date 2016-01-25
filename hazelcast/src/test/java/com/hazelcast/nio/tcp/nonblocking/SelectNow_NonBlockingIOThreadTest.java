package com.hazelcast.nio.tcp.nonblocking;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SelectNow_NonBlockingIOThreadTest extends NonBlockingIOThreadAbstractTest {

    @Override
    protected boolean selectNow() {
        return true;
    }
}
