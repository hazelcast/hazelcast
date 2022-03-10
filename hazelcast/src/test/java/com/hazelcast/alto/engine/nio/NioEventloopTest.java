package com.hazelcast.alto.engine.nio;

import com.hazelcast.internal.tpc.nio.NioEventloop;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.alto.engine.EventloopTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class NioEventloopTest extends EventloopTest {

    @Override
    public Eventloop createEventloop() {
        return new NioEventloop();
    }
}
