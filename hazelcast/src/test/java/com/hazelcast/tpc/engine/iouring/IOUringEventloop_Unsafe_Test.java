package com.hazelcast.tpc.engine.iouring;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.tpc.engine.Eventloop;
import com.hazelcast.tpc.engine.Eventloop_Unsafe_Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class IOUringEventloop_Unsafe_Test extends Eventloop_Unsafe_Test {

    @Override
    public Eventloop create() {
        return new IOUringEventloop();
    }
}
