package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.EventloopTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class IOUringEventloopTest extends EventloopTest {

    @Override
    public Eventloop createEventloop() {
        return new IOUringEventloop();
    }
}
